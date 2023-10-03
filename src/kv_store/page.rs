use crate::b_tree::b_node::{BNode, BTREE_PAGE_SIZE};

pub struct Page {
    pub flushed: u64,                     // Database size in number of pages
    pub temp: Vec<[u8; BTREE_PAGE_SIZE]>, // Newly allocated pages
}

impl Page {
    pub fn new() -> Self {
        Self {
            flushed: 0,
            temp: Vec::new(),
        }
    }

    pub fn page_new(&mut self, node: BNode) -> u64 {
        // TODO: reuse deallocated pages
        let ptr = self.flushed + self.temp.len() as u64;

        let data = node.get_data();
        self.temp.push(data);

        ptr
    }

    pub fn page_del(&mut self, ptr: u64) {
        // TODO: Implement this
    }
}
