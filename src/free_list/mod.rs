pub mod cloneable;
pub mod fl_node;
pub mod master_page;
pub mod mmap;
pub mod page_manager;
use crate::prelude::*;

use crate::{
    b_tree::{b_node::BNode, BTreePageManager},
    free_list::fl_node::MAX_FREE_LIST_IN_PAGE,
};

use std::{collections::VecDeque, fs::File};

use self::cloneable::RcRWLockBTreePageManager;
use self::{fl_node::FLNode, master_page::MasterPage, page_manager::PageManager};
pub struct FreeList {
    /// Pointer to first node of the free list
    head: u64,
    /// Number of pages taken from the free list
    nfree: i64,
    page_manager: PageManager,
}

impl FreeList {
    pub fn new(file_pointer: File) -> Result<Self> {
        Ok(Self {
            head: 0,
            nfree: 0,
            page_manager: PageManager::new(file_pointer)?,
        })
    }

    pub fn master_load(&mut self) -> Result<MasterPage> {
        let master_page = self.page_manager.master_load()?;
        self.head = master_page.free_list_head;
        Ok(master_page)
    }

    pub fn set_master_page(&mut self, btree_root: u64) -> Result<()> {
        self.page_manager.set_master_page(btree_root, self.head)
    }

    pub fn close(self) {
        self.page_manager.close();
    }

    pub fn total(&self) -> i64 {
        if self.head == 0 {
            return 0;
        }
        self.page_manager
            .page_get::<FLNode>(self.head)
            .total()
            .try_into()
            .unwrap()
    }

    pub fn get(&self, mut topn: i64) -> u64 {
        assert!(0 <= topn && topn < self.total());
        assert!(self.head != 0);
        let mut node: FLNode = self.page_manager.page_get(self.head);
        while node.size() as i64 <= topn {
            topn -= node.size() as i64;
            let next = node.next();
            assert!(next != 0);
            node = self.page_manager.page_get(next);
        }
        node.get_ptr(node.size() - topn as u16 - 1)
    }

    pub fn page_new(&mut self, node: BNode) -> u64 {
        let ptr: u64;
        let total = self.total();
        if self.nfree < total {
            // reuse deallocated page
            ptr = self.get(self.nfree);
            self.nfree += 1;
        } else {
            // allocate new page
            ptr = self.page_manager.flushed + self.page_manager.nappend as u64;
            self.page_manager.nappend += 1;
        }
        self.page_manager.updates.insert(ptr, Some(node.get_data()));
        ptr
    }

    pub fn flush_pages(&mut self, btree_root: u64) -> Result<()> {
        self.write_pages()?;
        self.sync_pages(btree_root)?;
        Ok(())
    }

    fn sync_pages(&mut self, btree_root: u64) -> Result<()> {
        self.page_manager.flush()?;
        self.nfree = 0;

        // update and flush the master page
        self.set_master_page(btree_root)?;
        self.page_manager.flush()?;

        Ok(())
    }

    fn write_pages(&mut self) -> Result<()> {
        // update the free list
        let freed_ptrs = self.page_manager.get_freed_ptrs();
        self.update(self.nfree, freed_ptrs);

        self.page_manager.write_pages()?;

        Ok(())
    }

    pub fn update(&mut self, mut popn: i64, mut freed_ptrs: VecDeque<u64>) {
        assert!(popn <= self.total());
        if popn == 0 && freed_ptrs.is_empty() {
            return; // No updates required
        }

        // prepare to construct new list
        let mut total = self.total();
        let mut reuse: VecDeque<u64> = VecDeque::new();
        while self.head != 0 && reuse.len() * MAX_FREE_LIST_IN_PAGE < freed_ptrs.len() {
            let node: FLNode = self.page_manager.page_get(self.head);
            freed_ptrs.push_back(self.head); // recycle the head node
            if popn >= node.size() as i64 {
                // phase 1 - remove all pointers in this node (popn is large enough we can just discard this node)
                popn -= node.size() as i64;
            } else {
                // phase 2 - remove some pointers in this node
                let mut remain = node.size() - popn as u16;
                popn = 0;
                // reuse pointers from the free list
                while remain > 0 && reuse.len() * MAX_FREE_LIST_IN_PAGE < freed_ptrs.len()
                // + remain as usize
                // Maybe check this
                {
                    remain -= 1;
                    reuse.push_back(node.get_ptr(remain));
                }

                // move the node into the `freed_ptrs` list
                for i in 0..remain {
                    freed_ptrs.push_back(node.get_ptr(i));
                }
            }

            // discard this node and move to the next node
            total -= node.size() as i64;
            self.head = node.next();
        }
        assert!(reuse.len() * MAX_FREE_LIST_IN_PAGE >= freed_ptrs.len() || self.head == 0);

        // Phase 3: prepend new nodes
        let new_total = total + freed_ptrs.len() as i64;

        self.push(freed_ptrs, reuse);

        // update the total
        let fl_head = self.page_manager.page_get_raw_mut(self.head);
        FLNode::set_total(fl_head, new_total.try_into().unwrap());
    }

    fn push(&mut self, mut freed_ptrs: VecDeque<u64>, mut reuse: VecDeque<u64>) {
        while !freed_ptrs.is_empty() {
            // Construct a new FLNode
            let mut size = freed_ptrs.len();
            if size > MAX_FREE_LIST_IN_PAGE {
                size = MAX_FREE_LIST_IN_PAGE;
            }

            let mut new_node = FLNode::new(size as u16, self.head);
            for (i, ptr) in freed_ptrs.iter().take(size).enumerate() {
                new_node.set_ptr(i as u16, *ptr);
            }
            freed_ptrs.drain(0..size);

            if !reuse.is_empty() {
                // reuse a pointer from the list
                self.head = reuse.pop_front().unwrap();
                self.page_manager.page_reuse(self.head, new_node);
            } else {
                // allocate a new page
                self.head = self.page_manager.page_append(new_node);
            }
        }
        assert!(reuse.is_empty());
    }
}

impl BTreePageManager for FreeList {
    fn page_get(&self, ptr: u64) -> BNode {
        self.page_manager.page_get(ptr)
    }

    fn page_new(&mut self, node: BNode) -> u64 {
        self.page_new(node)
    }

    fn page_del(&mut self, ptr: u64) {
        self.page_manager.page_del(ptr)
    }
}

impl RcRWLockBTreePageManager<FreeList> {
    pub fn master_load(&mut self) -> Result<MasterPage> {
        self.page_manager.write().unwrap().master_load()
    }

    pub fn flush_pages(&mut self, btree_root: u64) -> Result<()> {
        self.page_manager.write().unwrap().flush_pages(btree_root)
    }
}

#[cfg(test)]
impl FreeList {
    pub fn debug_free_list(&self) {
        let mut head = self.head;
        if head == 0 {
            println!("free list is empty");
            return;
        }

        while head != 0 {
            let free_node: FLNode = self.page_manager.page_get(head);
            println!("Page {}: {:?}", head, free_node);
            head = free_node.next();
        }
    }

    pub fn get_free_list_total(&self) -> u64 {
        if self.head == 0 {
            0
        } else {
            self.page_manager.page_get::<FLNode>(self.head).total()
        }
    }
}
// mod tests {
//     use std::fs;

//     use crate::b_tree::b_node::BTREE_PAGE_SIZE;

//     use super::*;
//     extern crate rand;

//     fn new_fl(path: &str, delete_old: bool) -> FreeList {
//         fs::create_dir_all("test_run_dir").unwrap();
//         let file_name = format!("test_run_dir/{}", path);
//         if delete_old {
//             fs::remove_file(&file_name);
//         }
//         let file_pointer = fs::OpenOptions::new()
//             .read(true)
//             .write(true)
//             .create(true)
//             .open(&file_name)
//             .unwrap();
//         FreeList::new(&file_pointer).unwrap()
//     }

//     #[test]
//     fn test_fl_full_node() {
//         // Create 509 pages
//         let mut fl = new_fl("test_fl_full_node", true);
//         for i in 0..509 {
//             fl.page_new(BNode::from(&[i as u8; BTREE_PAGE_SIZE]));
//             fl.update(0, freed_ptrs);
//         }
//     }
// }
