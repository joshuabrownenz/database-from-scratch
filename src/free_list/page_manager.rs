use std::{fs::File, io};

use crate::{
    b_tree::b_node::{BNode, Node, BTREE_PAGE_SIZE},
    free_list::fl_node::FLNode,
};

use super::{mmap::MMap, page::Page};

pub struct PageManager {
    pub mmap: MMap,
    pub page: Page,
}

impl PageManager {
    pub fn new(fp: &File) -> io::Result<Self> {
        let mmap = MMap::new(fp)?;
        let page = Page::new();

        Ok(Self { mmap, page })
    }

    pub fn page_get_flnode(&self, ptr: u64) -> FLNode {
        self.page_get(ptr)
    }

    pub fn page_get<T: Node>(&self, ptr: u64) -> T {
        // Get from temp pages if it exists
        match self.page.updates.get(&ptr) {
            Some(data) => T::from(data.as_ref().unwrap()),
            None => self.mmap.page_get_mapped(ptr),
        }
    }

    pub fn page_get_raw_mut(&mut self, ptr: u64) -> &mut [u8] {
        // Get from temp pages if it exists
        match self.page.updates.get_mut(&ptr) {
            Some(data) => &mut data.as_mut().unwrap()[..],
            None => self.mmap.page_get_mapped_raw_mut(ptr),
        }
    }

    // callback for free list, allocate a new page
    pub fn page_append(&mut self, node: FLNode) -> u64 {
        let ptr = self.page.flushed + self.page.nappend as u64;
        self.page.nappend += 1;
        self.page.updates.insert(ptr, Some(node.get_data()));
        ptr
    }

    // callback for free list, reuse a page
    pub fn page_reuse(&mut self, ptr: u64, node: FLNode) {
        self.page.updates.insert(ptr, Some(node.get_data()));
    }

    pub fn page_del(&mut self, ptr: u64) {
        self.page.updates.insert(ptr, None);
    }
}
