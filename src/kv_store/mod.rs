use std::{
    fs::OpenOptions,
    io::{self, Error, ErrorKind},
};

extern crate byteorder;

use crate::{
    b_tree::{BTree, InsertMode, InsertRequest},
    free_list::FreeList,
};

pub struct KV {
    tree: BTree<FreeList>,
}

impl KV {
    /** Opens the database. Callers responsiblity to close even if open results in an error */
    pub fn open(path: String) -> io::Result<KV> {
        // Open or create the file
        let file_open = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path);

        if file_open.is_err() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("failed to open file: {:?}", file_open.unwrap_err()),
            ));
        }

        let file_pointer = file_open.unwrap();

        let free = FreeList::new(file_pointer)?;

        let mut kv = KV {
            tree: BTree::new(free),
        };

        kv.master_load()?;

        // done
        Ok(kv)
    }

    pub fn close(self) {
        self.tree.page_manager.close();
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.tree.get_value(key)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.tree.insert(key, value);
        self.flush_pages()
    }

    pub fn del(&mut self, key: &[u8]) -> io::Result<bool> {
        let deleted = self.tree.delete(key);
        self.flush_pages()?;

        Ok(deleted)
    }

    fn master_load(&mut self) -> io::Result<()> {
        let master_page = self.tree.page_manager.master_load()?;
        self.tree.root = master_page.btree_root;
        Ok(())
    }

    fn flush_pages(&mut self) -> io::Result<()> {
        self.tree.page_manager.flush_pages(self.tree.root)?;
        Ok(())
    }

    pub fn update(&mut self, key: &[u8], value: &[u8], mode: InsertMode) -> io::Result<bool> {
        let req = InsertRequest::new(key.to_vec(), value.to_vec()).mode(mode);
        let res = self.tree.insert_exec(req);
        self.flush_pages()?;
        Ok(res.added)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs};

    use super::*;
    extern crate rand;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn new_kv(path: &str, delete_old: bool) -> KV {
        fs::create_dir_all("test_run_dir").unwrap();
        let file_name = format!("test_run_dir/{}", path);
        if delete_old {
            fs::remove_file(&file_name).unwrap_or(());
        }
        KV::open(file_name).unwrap()
    }

    fn debug_free_list(kv: &KV) {
        kv.tree.page_manager.debug_free_list();
    }

    fn get_free_list_total(kv: &KV) -> u64 {
        kv.tree.page_manager.get_free_list_total()
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

        for i in 0..10000 {
            println!("Step 1: {}", i);
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
        for i in 0..10000 {
            println!("Step 2: {}", i);
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            let result = kv.get(&key);
            if deleted_keys.contains(&i) {
                assert!(result.is_none());
            } else {
                assert_eq!(result.unwrap(), value);
            }
            println!("{}: FL.total() = {}", i, get_free_list_total(&kv));
            kv.del(&key).unwrap();
        }

        kv.close();
        let kv = new_kv("test_kv.db", false);
        for i in 0..10000 {
            println!("Step 3: {}", i);
            let key = format!("key{}", i).as_bytes().to_vec();
            let result = kv.get(&key);
            assert!(result.is_none());
        }
        kv.close();
    }

    // #[test]
    // fn test_fl_full_node() {
    //     let mut kv = new_kv("test_fl_full_node.db", true);

    //     let mut pages: HashSet<u64> = HashSet::new();
    //     let mut free_pages: u64 = 0;
    //     for i in 0..15 * MAX_FREE_LIST_IN_PAGE {
    //         let new_page = kv.free.page_new(BNode::new(NodeType::Leaf, 0));
    //         free_pages = free_pages.saturating_sub(1);
    //         assert!(!pages.contains(&new_page));
    //         pages.insert(new_page);
    //         if i != 0 && i % 10 == 0 && pages.contains(&(new_page - 5)) {
    //             kv.free.page_del(new_page - 5);
    //             free_pages += 1;
    //             pages.remove(&(new_page - 5));
    //         }
    //         kv.flush_pages().unwrap();

    //         // Assert free list matchs
    //     }

    //     for page in pages {
    //         kv.free.page_del(page);
    //         kv.flush_pages().unwrap();
    //     }
    // }

    #[test]
    fn test_database_merging_ability() {
        let mut kv = new_kv("test_database_merging_ability.db", true);

        let mut rng = StdRng::seed_from_u64(2131);
        let mut keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for _ in 0..20000 {
            let length: usize = rng.gen_range(10..=65);
            let key: Vec<u8> = (0..length).map(|_| rng.gen()).collect();
            let length: usize = rng.gen_range(10..=150);
            let value: Vec<u8> = (0..length).map(|_| rng.gen()).collect();
            kv.set(&key, &value).unwrap();
            keys.push((key, value));
        }

        while !keys.is_empty() {
            let section_size: u32 = rng.gen_range(5..15);
            let section_start: u32 = if section_size > keys.len() as u32 {
                0
            } else {
                rng.gen_range(0..keys.len().saturating_sub(section_size as usize) as u32)
            };

            for _ in 0..section_size {
                if section_start >= keys.len() as u32 {
                    break;
                }
                let (key, value) = keys.remove(section_start as usize);
                let result = kv.get(&key);
                assert_eq!(result.unwrap(), value);
                kv.del(&key).unwrap();
            }
        }
    }
}
