use fs2::FileExt as OtherFileExt;
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{self, Error, ErrorKind, Write},
    os::unix::prelude::FileExt,
};

extern crate byteorder;

use byteorder::{ByteOrder, LittleEndian};

use crate::{
    b_tree::{b_node::BTREE_PAGE_SIZE, BTree},
    free_list::FreeList,
};

const DB_SIG: &str = "BuildYourOwnDB00";

pub struct KV {
    path: String,
    fp: File,
    tree: BTree,
    free: FreeList,
}

impl KV {
    /** Opens the database. Callers responsiblity to close even if open results in an error */
    pub fn open(path: String) -> io::Result<KV> {
        // Open or create the file
        let file_open = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path);

        if file_open.is_err() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("failed to open file: {:?}", file_open.unwrap_err()),
            ));
        }
        let fp = file_open.unwrap();

        let free = FreeList::new(&fp)?;

        let mut kv = KV {
            path,
            fp,
            free,
            tree: BTree::new(),
        };

        kv.master_load()?;

        // done
        Ok(kv)
    }

    // TODO: Look into closing this properly
    pub fn close(self) {
        // We should just make a closed function for free
        let mut mmap = self.free.page_manager.mmap;
        mmap.chunks.clear();
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Result<Vec<u8>, ()> {
        self.tree.get_value(&self.free, key)
    }

    pub fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> io::Result<()> {
        self.tree.insert(&mut self.free, key, value);
        self.flush_pages()
    }

    pub fn del(&mut self, key: &Vec<u8>) -> Result<bool, ()> {
        let deleted = self.tree.delete(&mut self.free, key);
        let flush_result = self.flush_pages();

        if flush_result.is_err() {
            Err(())
        } else {
            Ok(deleted)
        }
    }

    // Put in free list
    fn master_load(&mut self) -> io::Result<()> {
        if self.free.page_manager.mmap.file == 0 {
            // empty file, the master page will be create on the first write
            self.free.page_manager.page.flushed = 1; // reserved for the master page
            return Ok(());
        }

        let data = self.free.page_manager.mmap.chunks[0].as_ref();
        let root = LittleEndian::read_u64(&data[16..]);
        let used = LittleEndian::read_u64(&data[24..]);
        let free_list = LittleEndian::read_u64(&data[32..]);

        if &data[..16] != DB_SIG.as_bytes() {
            return Err(io::Error::new(io::ErrorKind::Other, "bad signature"));
        }
        let mut bad =
            !(1 <= used && used <= self.free.page_manager.mmap.file / BTREE_PAGE_SIZE as u64);
        bad = bad || root >= used;
        bad = bad || free_list >= used;
        bad = bad || free_list < 1 || free_list == root;
        if bad {
            return Err(io::Error::new(io::ErrorKind::Other, "bad master page"));
        }
        self.tree.root = root;
        self.free.page_manager.page.flushed = used;

        self.free.head = free_list;
        Ok(())
    }

    fn master_save(&mut self) -> io::Result<()> {
        let mut data = [0; 40];
        // Convert signature to bytes
        assert!(DB_SIG.len() == 16, "const DG_SIG must be 16 bytes");
        data[..16].copy_from_slice(DB_SIG.as_bytes());
        LittleEndian::write_u64(&mut data[16..], self.tree.root);
        LittleEndian::write_u64(&mut data[24..], self.free.page_manager.page.flushed);
        LittleEndian::write_u64(&mut data[32..], self.free.head);

        // Atomic write to the master page
        self.fp.lock_exclusive()?;
        let result = self.fp.write_at(&data, 0);
        if result.is_err() {
            self.fp.unlock()?;
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to write master page: {:?}", result.unwrap_err()),
            ));
        }
        self.fp.unlock()?;

        Ok(())
    }

    fn write_pages(&mut self) -> io::Result<()> {
        // update the free list
        let mut freed_ptrs = VecDeque::new();
        for (ptr, data) in self.free.page_manager.page.updates.iter() {
            if data.is_none() {
                freed_ptrs.push_back(*ptr);
            }
        }

        let nfree = self.free.page_manager.page.nfree;
        self.free.update(nfree, freed_ptrs);

        let n_pages =
            self.free.page_manager.page.flushed + self.free.page_manager.page.nappend as u64;

        self.extend_file(n_pages)?;
        self.extend_mmap(n_pages)?;

        // copy temp data to mmap
        for (ptr, temp_page) in self.free.page_manager.page.updates.iter() {
            if temp_page.is_some() {
                self.free
                    .page_manager
                    .mmap
                    .page_set(*ptr, temp_page.as_ref().unwrap());
            }
        }

        Ok(())
    }

    fn sync_pages(&mut self) -> io::Result<()> {
        // Flush data to the disk. Must be done before updating the master page.
        self.fp.flush()?;

        self.free.page_manager.page.flushed += self.free.page_manager.page.nappend as u64;
        self.free.page_manager.page.nfree = 0;
        self.free.page_manager.page.nappend = 0;
        self.free.page_manager.page.updates.clear();

        // update and flush the master page
        self.master_save()?;
        self.fp.flush()?;

        Ok(())
    }

    fn extend_file(&mut self, npages: u64) -> io::Result<()> {
        let mut file_pages = self.free.page_manager.mmap.file / BTREE_PAGE_SIZE as u64;
        if file_pages >= npages {
            return Ok(());
        }

        while file_pages < npages {
            // the file size is increased exponentially,
            // so that we don't have to extend the file for every update.
            let mut inc = file_pages / 8;
            if inc < 1 {
                inc = 1;
            }
            file_pages += inc;
        }

        let file_size = file_pages * BTREE_PAGE_SIZE as u64;
        let result = self.fp.set_len(file_size);
        if result.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to extend file: {:?}", result.unwrap_err()),
            ));
        }

        self.free.page_manager.mmap.file = file_size;
        Ok(())
    }

    fn flush_pages(&mut self) -> io::Result<()> {
        self.write_pages()?;
        self.sync_pages()
    }

    fn extend_mmap(&mut self, npages: u64) -> io::Result<()> {
        self.free
            .page_manager
            .mmap
            .extend_mmap(&self.fp, npages as usize)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs};

    use crate::{
        b_tree::{
            b_node::{BNode, NodeType},
            BTreePageManager,
        },
        free_list::fl_node::MAX_FREE_LIST_IN_PAGE,
    };

    use super::*;
    extern crate rand;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn new_kv(path: &str, delete_old: bool) -> KV {
        fs::create_dir_all("test_run_dir").unwrap();
        let file_name = format!("test_run_dir/{}", path);
        if delete_old {
            fs::remove_file(&file_name).unwrap();
        }
        KV::open(file_name).unwrap()
    }

    fn debug_free_list(kv: &KV) {
        let mut head = kv.free.head;
        if head == 0 {
            println!("free list is empty");
            return;
        }

        while head != 0 {
            let free_node = kv.free.page_manager.page_get_flnode(head);
            println!("Page {}: {:?}", head, free_node);
            head = free_node.next();
        }
    }

    fn get_free_list_total(kv: &KV) -> u64 {
        let head = kv.free.head;
        if head == 0 {
            0
        } else {
            kv.free.page_manager.page_get_flnode(head).total()
        }
    }

    #[test]
    fn test_kv_single_set() {
        let mut kv = new_kv("test_kv_single_set.db", true);

        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();
        kv.set(&key, &value).unwrap();

        let result = kv.get(&key).unwrap();
        assert_eq!(value, result);

        kv.close();
    }

    #[test]
    fn test_kv_small_set_get() {
        let mut kv = new_kv("test_kv_small_set_get.db", true);

        for i in 0..100 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv.set(&key, &value).unwrap();

            let result = kv.get(&key).unwrap();
            assert_eq!(value, result);
        }
        debug_free_list(&kv);

        kv.close();
    }

    // Without page reuse the database size is 7.7MB (10000 as loop)
    // With page resuse the database size is 590 KB (10000 as loop)
    #[test]
    fn test_kv() {
        let mut kv = new_kv("test_kv.db", true);

        let mut deleted_keys: HashSet<i32> = HashSet::new();
        let mut rng = StdRng::seed_from_u64(675127398);

        for i in 0..2000000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv.set(&key, &value).unwrap();
            if i != 0 && i % 5 == 0 {
                let to_delete = rng.gen_range(0..i);
                if !deleted_keys.contains(&to_delete) {
                    let key = format!("key{}", to_delete).as_bytes().to_vec();
                    kv.del(&key).unwrap();
                    if rng.gen_bool(0.5) {
                        let value = format!("value{}", to_delete).as_bytes().to_vec();
                        kv.set(&key, &value).unwrap();
                    } else {
                        deleted_keys.insert(to_delete);
                    }
                }
            }
        }

        kv.close();

        let mut kv = new_kv("test_kv.db", false);
        for i in 0..2000000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            let result = kv.get(&key);
            if deleted_keys.contains(&i) {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), value);
            }
            println!("{}: FL.total() = {}", i, get_free_list_total(&kv));
            kv.del(&key).unwrap();
        }

        kv.close();
        let mut kv = new_kv("test_kv.db", false);
        for i in 0..2000000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let result = kv.get(&key);
            assert!(result.is_err());
        }
        kv.close();
    }

    #[test]
    fn test_fl_full_node() {
        let mut kv = new_kv("test_fl_full_node.db", true);

        let mut pages: HashSet<u64> = HashSet::new();
        let mut free_pages: u64 = 0;
        for i in 0..15 * MAX_FREE_LIST_IN_PAGE {
            let new_page = kv.free.page_new(BNode::new(NodeType::Leaf, 0));
            free_pages = free_pages.saturating_sub(1);
            assert!(!pages.contains(&new_page));
            pages.insert(new_page);
            if i != 0 && i % 10 == 0 && pages.contains(&(new_page - 5)) {
                kv.free.page_del(new_page - 5);
                free_pages += 1;
                pages.remove(&(new_page - 5));
            }
            kv.flush_pages().unwrap();

            // Assert free list matchs
        }

        for page in pages {
            kv.free.page_del(page);
            kv.flush_pages().unwrap();
            let head = kv.free.head;

            assert!(head != 0 || free_pages == 0);
            if free_pages > 0 {
                let free_node = kv.free.page_manager.page_get_flnode(head);
                assert_eq!(free_pages, free_node.total());
            }
        }
    }
}
