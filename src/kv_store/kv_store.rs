use fs2::FileExt as OtherFileExt;
use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{self, Error, ErrorKind, Write},
    os::unix::prelude::FileExt,
    path::Path,
    rc::Rc,
};

extern crate byteorder;

use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::b_tree::{
    b_node::{BNode, BTREE_PAGE_SIZE},
    b_tree::BTree,
};

use super::{mmap::MMap, page::Page};

const DB_SIG: &str = "BuildYourOwnDB00";

pub struct KV {
    path: String,
    fp: File,
    tree: BTree,
    mmap: Rc<RefCell<MMap>>,
    page: Rc<RefCell<Page>>,
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

        // create reference counting mmap and page
        let mmap = Rc::new(RefCell::new(MMap::new(&fp)?));
        let page = Rc::new(RefCell::new(Page::new()));

        // btree callbacks
        let mmap_get_ref = mmap.clone();
        let tree_get = Box::new(move |ptr: u64| mmap_get_ref.borrow_mut().page_get(ptr));

        let page_new_ref = page.clone();
        let tree_new = Box::new(move |node: BNode| page_new_ref.borrow_mut().page_new(node));

        let page_del_ref = page.clone();
        let tree_del = Box::new(move |ptr: u64| page_del_ref.borrow_mut().page_del(ptr));

        // read the master page
        let mut kv = KV {
            path,
            fp,
            tree: BTree::new_with_callbacks(tree_get, tree_new, tree_del),
            mmap,
            page,
        };
        kv.master_load()?;

        // done
        Ok(kv)
    }

    pub fn close(self) {
        let mut mmap = self.mmap.borrow_mut();
        mmap.chunks.clear();
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Result<Vec<u8>, ()> {
        self.tree.get_value(key)
    }

    pub fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> io::Result<()> {
        self.tree.Insert(key, value);
        self.flush_pages()
    }

    pub fn del(&mut self, key: &Vec<u8>) -> Result<bool, ()> {
        let deleted = self.tree.Delete(key);
        let flush_result = self.flush_pages();

        if flush_result.is_err() {
            Err(())
        } else {
            Ok(deleted)
        }
    }

    fn master_load(&mut self) -> io::Result<()> {
        let mmap = self.mmap.borrow();
        let mut page = self.page.borrow_mut();
        if mmap.file == 0 {
            // empty file, the master page will be create on the first write
            page.flushed = 1; // reserved for the master page
            return Ok(());
        }

        let data = mmap.chunks[0].as_ref();
        let root = LittleEndian::read_u64(&data[16..]);
        let used = LittleEndian::read_u64(&data[24..]);

        if &data[..16] != DB_SIG.as_bytes() {
            return Err(io::Error::new(io::ErrorKind::Other, "bad signature"));
        }
        let mut bad = !(1 <= used && used <= mmap.file / BTREE_PAGE_SIZE as u64);
        bad = bad || root >= used;
        if bad {
            return Err(io::Error::new(io::ErrorKind::Other, "bad master page"));
        }
        self.tree.root = root;
        page.flushed = used;
        Ok(())
    }

    fn master_save(&mut self) -> io::Result<()> {
        let page = self.page.borrow();

        let mut data = [0; 32];
        // Convert signature to bytes
        assert!(DB_SIG.len() == 16, "const DG_SIG must be 16 bytes");
        data[..16].copy_from_slice(DB_SIG.as_bytes());
        LittleEndian::write_u64(&mut data[16..], self.tree.root);
        LittleEndian::write_u64(&mut data[24..], page.flushed);

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

    fn extend_file(&mut self, npages: u64) -> io::Result<()> {
        let mut file_pages = self.mmap.borrow().file / BTREE_PAGE_SIZE as u64;
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

        self.mmap.borrow_mut().file = file_size;
        Ok(())
    }

    fn flush_pages(&mut self) -> io::Result<()> {
        self.write_pages()?;
        self.sync_pages()
    }

    fn extend_mmap(&mut self, npages: u64) -> io::Result<()> {
        let mut mmap = self.mmap.borrow_mut();
        mmap.extend_mmap(&self.fp, npages as usize)
    }

    fn write_pages(&mut self) -> io::Result<()> {
        let n_pages = {
            let page = self.page.borrow();
            page.flushed + page.temp.len() as u64
        };

        self.extend_file(n_pages)?;
        self.extend_mmap(n_pages)?;

        // copy temp data to mmap
        let page = self.page.borrow();
        let mut mmap = self.mmap.borrow_mut();
        for (i, temp_page) in page.temp.iter().enumerate() {
            let ptr = page.flushed + i as u64;
            mmap.page_set(ptr, temp_page);
        }

        Ok(())
    }

    fn sync_pages(&mut self) -> io::Result<()> {
        // Flush data to the disk. Must be done before updating the master page.
        self.fp.flush()?;

        {
            let mut page = self.page.borrow_mut();
            page.flushed += page.temp.len() as u64;
            page.temp.clear();
        }

        // update and flush the master page
        self.master_save()?;
        self.fp.flush()?;

        Ok(())
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


    // Without page reuse the database size is 7.7MB
    #[test]
    fn test_kv() {
        let mut kv = new_kv("test_kv.db", true);

        for i in 0..10000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            kv.set(&key, &value).unwrap();
        }

        kv.close();

        let mut kv = new_kv("test_kv.db", false);
        for i in 0..10000 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            let result = kv.get(&key).unwrap();
            assert_eq!(value, result);
        }
    }
}
