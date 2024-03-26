use std::{rc::Rc, sync::RwLock};

use crate::b_tree::{b_node::BNode, BTreePageManager};

pub struct RcRWLockBTreePageManager<B: BTreePageManager> {
    pub page_manager: Rc<RwLock<B>>,
}

impl<B: BTreePageManager> Clone for RcRWLockBTreePageManager<B> {
    fn clone(&self) -> Self {
        Self {
            page_manager: self.page_manager.clone(),
        }
    }
}

impl<B: BTreePageManager> RcRWLockBTreePageManager<B> {
    pub fn new(page_manager: B) -> Self {
        Self {
            page_manager: Rc::new(RwLock::new(page_manager)),
        }
    }
}
pub trait CloneableBTreePageManager: Clone {
    fn page_get(&self, ptr: u64) -> BNode;
    fn page_new(&mut self, node: BNode) -> u64;
    fn page_del(&mut self, ptr: u64);
    fn close(self);
}

impl<B: BTreePageManager> CloneableBTreePageManager for RcRWLockBTreePageManager<B> {
    fn page_new(&mut self, node: BNode) -> u64 {
        self.page_manager.write().unwrap().page_new(node)
    }

    fn page_get(&self, ptr: u64) -> BNode {
        self.page_manager.read().unwrap().page_get(ptr)
    }

    fn page_del(&mut self, ptr: u64) {
        self.page_manager.write().unwrap().page_del(ptr)
    }

    fn close(self) {
        // TODO: Figure out how to close a cloned BTreePageManager
        // self.page_manager.().unwrap().close();
    }
}
