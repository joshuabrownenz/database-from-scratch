use std::collections::HashMap;

use crate::b_tree::b_node::BTREE_PAGE_SIZE;

pub struct Page {
    pub flushed: u64, // Database size in number of pages
    pub nfree: i64,   // Number of pages taken from the free list
    pub nappend: i64, // Number of pages appended to the database
    // newly allocated or deallocated pages keyed by the pointer.
    // empty vector means the page is deallocated
    pub updates: HashMap<u64, Option<[u8; BTREE_PAGE_SIZE]>>,
}

impl Page {
    pub fn new() -> Self {
        Self {
            flushed: 0,
            nfree: 0,
            nappend: 0,
            updates: HashMap::new(),
        }
    }
}
