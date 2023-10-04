use crate::{
    b_tree::{b_node::BNode, b_tree::BTreePageManager},
    free_list::fl_node::MAX_FREE_LIST_IN_PAGE,
};

use std::{collections::VecDeque, fs::File, io};

use super::{fl_node::FLNode, page_manager::PageManager};
pub struct FreeList {
    pub head: u64,

    pub page_manager: PageManager,
}

impl FreeList {
    pub fn new(fp: &File) -> io::Result<Self> {
        Ok(Self {
            head: 0,
            page_manager: PageManager::new(fp)?,
        })
    }

    pub fn total(&self) -> i64 {
        if self.head == 0 {
            return 0;
        }
        self.page_manager
            .page_get_flnode(self.head)
            .total()
            .try_into()
            .unwrap()
    }

    pub fn get(&self, mut topn: i64) -> u64 {
        assert!(0 <= topn && topn < self.total());
        assert!(self.head != 0);
        let mut node = self.page_manager.page_get_flnode(self.head);
        while node.size() as i64 <= topn {
            topn -= node.size() as i64;
            let next = node.next();
            assert!(next != 0);
            node = self.page_manager.page_get_flnode(next);
        }
        node.get_ptr(node.size() - topn as u16 - 1)
    }

    pub fn page_new(&mut self, node: BNode) -> u64 {
        let ptr: u64;
        let total = self.total();
        if self.page_manager.page.nfree < total{
            // reuse deallocated page
            ptr = self.get(self.page_manager.page.nfree);
            self.page_manager.page.nfree += 1;
        } else {
            // allocate new page
            ptr = self.page_manager.page.flushed + self.page_manager.page.nappend as u64;
            self.page_manager.page.nappend += 1;
        }
        self.page_manager
            .page
            .updates
            .insert(ptr, Some(node.get_data()));
        ptr
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
            let node: FLNode = self.page_manager.page_get_flnode(self.head);
            freed_ptrs.push_back(self.head); // recycle the head node
            if popn >= node.size() as i64 {
                // phase 1 - remove all pointers in this node (popn is large enough we can just discard this node)
                popn -= node.size() as i64;
            } else {
                // phase 2 - remove some pointers in this node
                let mut remain = node.size() - popn as u16;
                popn = 0;
                // reuse pointers from the free list
                while remain > 0
                    && reuse.len() * MAX_FREE_LIST_IN_PAGE < freed_ptrs.len() + remain as usize
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
