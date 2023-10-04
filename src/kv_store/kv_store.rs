use fs2::FileExt as OtherFileExt;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{self, Error, ErrorKind, Write},
    os::unix::prelude::FileExt,
    rc::Rc,
};

extern crate byteorder;

use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::{
    b_tree::{
        b_node::{BNode, Node, BTREE_PAGE_SIZE},
        b_tree::BTree,
    },
    free_list::{self, free_list::FreeList},
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
        self.tree.Insert(&mut self.free, key, value);
        self.flush_pages()
    }

    pub fn del(&mut self, key: &Vec<u8>) -> Result<bool, ()> {
        let deleted = self.tree.Delete(&mut self.free, key);
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
    use std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        fs,
        iter::StepBy,
        rc::Rc,
    };

    use super::*;
    extern crate rand;

    use rand::Rng;

    fn new_kv(path: &str, delete_old: bool) -> KV {
        fs::create_dir_all("test_run_dir").unwrap();
        let file_name = format!("test_run_dir/{}", path);
        if delete_old {
            fs::remove_file(&file_name);
        }
        KV::open(file_name).unwrap()
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

        kv.close();
    }

    // Without page reuse the database size is 7.7MB
    // With page resuse the database size is 590 KB
    #[test]
    fn test_kv() {
        let mut kv = new_kv("test_kv.db", true);
        let mut data = [0; BTREE_PAGE_SIZE];

        for i in 0..50000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv.set(&key, &value).unwrap();
        }

        kv.close();

        let mut kv = new_kv("test_kv.db", false);
        for i in 0..50000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            let result = kv.get(&key).unwrap();
            assert_eq!(value, result);
            if i == 36754 {
                let free_node = kv.free.page_manager.page_get_flnode(kv.free.head);
            }
            // Currently fails on i = 36754
            kv.del(&key).unwrap();
        }

        kv.close();
        let mut kv = new_kv("test_kv.db", false);
        for i in 0..50000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let result = kv.get(&key);
            assert!(result.is_err());
        }
        let free_node = kv.free.page_manager.page_get_flnode(kv.free.head);
        kv.close();
    }
}
