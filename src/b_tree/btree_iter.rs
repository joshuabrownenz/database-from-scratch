use std::path;

use crate::{b_tree::b_node::NodeType, free_list::page_manager};

use super::{b_node::BNode, BTree, BTreePageManager};

pub struct BTreeIterator<'a, B: BTreePageManager> {
    tree: &'a BTree<B>,
    path: Vec<BNode>,
    positions: Vec<u16>,
}

type Item = (Vec<u8>, Vec<u8>);

impl<'a, B: BTreePageManager> BTreeIterator<'a, B> {
    pub fn new(tree: &'a BTree<B>, path: Vec<BNode>, positions: Vec<u16>) -> BTreeIterator<'a, B> {
        BTreeIterator {
            tree,
            path,
            positions,
        }
    }

    /** Gets the current key value pair */
    pub fn deref(&self) -> Item {
        let node = &self.path[self.positions.len() - 1];
        let key = node.get_key(self.positions[self.positions.len() - 1]);
        let value = node.get_val(self.positions[self.positions.len() - 1]);
        (key, value)
    }

    /** Moves forward along the iterator */
    pub fn next(&mut self) -> bool {
        self.nextIter(self.positions.len() - 1)
    }

    /** Moves forward along the iterator, returns wether the move was a success or not */
    fn nextIter(&mut self, level: usize) -> bool {
        let node = &self.path[level];
        let num_keys = node.num_keys();
        if self.positions[level] < node.num_keys() - 1 {
            // move within this node
            self.positions[level] += 1;
        } else if level > 0 {
            // move to a slibing node
            if !self.nextIter(level - 1) {
                //
                return false;
            }
        } else {
            // we are at the end. Don't move
            return false;
        };

        // If level is not the leaf level, move to the first key of the kid node
        if level + 1 < self.positions.len() {
            // update the kid node
            let node = &self.path[level];
            assert!(node.b_type() == NodeType::Node);
            let child_node = self
                .tree
                .page_manager
                .page_get(node.get_ptr(self.positions[level]));
            self.positions[level + 1] = 0;
            self.path[level + 1] = child_node;
        }

        true
    }

    /** Moves backward along the iterator */
    pub fn prev(&mut self) -> bool {
        self.prevIter(self.positions.len() - 1)
    }

    /** Moves forward along the iterator, returns wether the move was a success or not */
    fn prevIter(&mut self, level: usize) -> bool {
        if self.positions[level] > 0 {
            // move within this node
            self.positions[level] -= 1;
        } else if level > 0 {
            // move to a slibing node
            if !self.prevIter(level - 1) {
                //
                return false;
            }
        } else {
            // we are at the beginning. Don't move
            return false;
        };

        // If level is not the leaf level, move to the last key of the kid node
        if level + 1 < self.positions.len() {
            // update the kid node
            let node = &self.path[level];
            assert!(node.b_type() == NodeType::Node);
            let child_node = self
                .tree
                .page_manager
                .page_get(node.get_ptr(self.positions[level]));
            self.positions[level + 1] = child_node.num_keys() - 1;
            self.path[level + 1] = child_node;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::Ordering,
        collections::{HashMap, HashSet},
        fmt::format,
    };

    use crate::b_tree::b_node::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE, BTREE_PAGE_SIZE};

    use super::*;
    extern crate rand;

    use rand::{rngs::StdRng, Rng, SeedableRng};

    struct PageManager {
        pub pages: HashMap<u64, [u8; BTREE_PAGE_SIZE]>,
    }

    impl PageManager {
        fn new() -> PageManager {
            PageManager {
                pages: HashMap::<u64, [u8; BTREE_PAGE_SIZE]>::new(),
            }
        }

        fn get_page(&self, ptr: u64) -> BNode {
            BNode::from(self.pages.get(&ptr).unwrap())
        }

        fn new_page(&mut self, node: BNode) -> u64 {
            assert!(node.num_bytes() <= BTREE_PAGE_SIZE as u16);
            let mut rng = rand::thread_rng();
            let mut random_ptr: u64 = rng.gen();
            while self.pages.contains_key(&random_ptr) {
                random_ptr = rng.gen();
            }
            self.pages.insert(random_ptr, node.get_data());
            random_ptr
        }

        fn del_page(&mut self, ptr: u64) {
            self.pages.remove(&ptr);
        }
    }

    impl BTreePageManager for PageManager {
        fn page_new(&mut self, node: BNode) -> u64 {
            self.new_page(node)
        }

        fn page_get(&self, ptr: u64) -> BNode {
            self.get_page(ptr)
        }

        fn page_del(&mut self, ptr: u64) {
            self.del_page(ptr);
        }
    }
    // TODO use this struct in other test files

    struct C<B: BTreePageManager> {
        pub tree: BTree<B>,
        pub reference: HashMap<String, String>,
    }
    impl C<PageManager> {
        fn new() -> C<PageManager> {
            let page_manager = PageManager::new();

            C {
                tree: BTree::new(page_manager),
                reference: HashMap::new(),
            }
        }

        fn add(&mut self, key: &str, val: &str) {
            self.tree
                .insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
            self.reference.insert(key.to_string(), val.to_string());
        }

        fn get(&self, key: &str) -> Option<Vec<u8>> {
            self.tree.get_value(&key.as_bytes().to_vec())
        }

        fn delete(&mut self, key: &str) -> bool {
            let remove = self.reference.remove(key);
            let did_remove = self.tree.delete(&key.as_bytes().to_vec());
            assert_eq!(remove.is_some(), did_remove);
            did_remove
        }
    }

    #[test]
    fn test_small_number_of_values_next() {
        let mut c = C::new();
        c.add("a", "a");
        c.add("b", "b");
        c.add("c", "c");
        c.add("d", "d");
        c.add("e", "e");

        assert_eq!(c.get("a").unwrap(), "a".as_bytes().to_vec());
        assert_eq!(c.get("b").unwrap(), "b".as_bytes().to_vec());
        assert_eq!(c.get("c").unwrap(), "c".as_bytes().to_vec());
        assert_eq!(c.get("d").unwrap(), "d".as_bytes().to_vec());
        assert_eq!(c.get("e").unwrap(), "e".as_bytes().to_vec());

        let mut iter = BTreeIterator {
            tree: &c.tree,
            path: vec![c.tree.page_manager.get_page(c.tree.root)],
            positions: vec![0],
        };

        assert_eq!(iter.deref(), (vec![], vec![]));
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("a".as_bytes().to_vec(), "a".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("b".as_bytes().to_vec(), "b".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("c".as_bytes().to_vec(), "c".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("d".as_bytes().to_vec(), "d".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("e".as_bytes().to_vec(), "e".as_bytes().to_vec())
        );
        assert!(!iter.next());
    }

    #[test]
    fn test_small_number_of_values_prev() {
        let mut c = C::new();
        c.add("a", "a");
        c.add("b", "b");
        c.add("c", "c");
        c.add("d", "d");
        c.add("e", "e");

        assert_eq!(c.get("a").unwrap(), "a".as_bytes().to_vec());
        assert_eq!(c.get("b").unwrap(), "b".as_bytes().to_vec());
        assert_eq!(c.get("c").unwrap(), "c".as_bytes().to_vec());
        assert_eq!(c.get("d").unwrap(), "d".as_bytes().to_vec());
        assert_eq!(c.get("e").unwrap(), "e".as_bytes().to_vec());

        let mut iter = BTreeIterator {
            tree: &c.tree,
            path: vec![c.tree.page_manager.get_page(c.tree.root)],
            positions: vec![5],
        };

        assert_eq!(
            iter.deref(),
            ("e".as_bytes().to_vec(), "e".as_bytes().to_vec())
        );
        assert!(iter.prev());
        assert_eq!(
            iter.deref(),
            ("d".as_bytes().to_vec(), "d".as_bytes().to_vec())
        );
        assert!(iter.prev());
        assert_eq!(
            iter.deref(),
            ("c".as_bytes().to_vec(), "c".as_bytes().to_vec())
        );
        assert!(iter.prev());
        assert_eq!(
            iter.deref(),
            ("b".as_bytes().to_vec(), "b".as_bytes().to_vec())
        );
        assert!(iter.prev());
        assert_eq!(
            iter.deref(),
            ("a".as_bytes().to_vec(), "a".as_bytes().to_vec())
        );
        assert!(iter.prev());
        assert_eq!(iter.deref(), (vec![], vec![]));
        assert!(!iter.prev());
    }

    #[test]
    fn test_small_number_of_values_in_random_order() {
        let mut c = C::new();
        c.add("d", "d");
        c.add("b", "b");
        c.add("a", "a");
        c.add("e", "e");
        c.add("c", "c");

        assert_eq!(c.get("a").unwrap(), "a".as_bytes().to_vec());
        assert_eq!(c.get("b").unwrap(), "b".as_bytes().to_vec());
        assert_eq!(c.get("c").unwrap(), "c".as_bytes().to_vec());
        assert_eq!(c.get("d").unwrap(), "d".as_bytes().to_vec());
        assert_eq!(c.get("e").unwrap(), "e".as_bytes().to_vec());

        let mut iter = BTreeIterator {
            tree: &c.tree,

            path: vec![c.tree.page_manager.get_page(c.tree.root)],
            positions: vec![0],
        };

        assert_eq!(iter.deref(), (vec![], vec![]));
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("a".as_bytes().to_vec(), "a".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("b".as_bytes().to_vec(), "b".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("c".as_bytes().to_vec(), "c".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("d".as_bytes().to_vec(), "d".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("e".as_bytes().to_vec(), "e".as_bytes().to_vec())
        );
        assert!(!iter.next());
    }

    #[test]
    fn test_small_number_of_values_in_random_order_with_deletes() {
        let mut c = C::new();
        c.add("d", "d");
        c.add("b", "b");
        c.add("a", "a");
        c.add("e", "e");
        c.add("c", "c");

        assert_eq!(c.get("a").unwrap(), "a".as_bytes().to_vec());
        assert_eq!(c.get("b").unwrap(), "b".as_bytes().to_vec());
        assert_eq!(c.get("c").unwrap(), "c".as_bytes().to_vec());
        assert_eq!(c.get("d").unwrap(), "d".as_bytes().to_vec());
        assert_eq!(c.get("e").unwrap(), "e".as_bytes().to_vec());

        c.delete("d");
        c.delete("e");

        let mut iter = BTreeIterator {
            tree: &c.tree,

            path: vec![c.tree.page_manager.get_page(c.tree.root)],
            positions: vec![0],
        };

        assert_eq!(iter.deref(), (vec![], vec![]));
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("a".as_bytes().to_vec(), "a".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("b".as_bytes().to_vec(), "b".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("c".as_bytes().to_vec(), "c".as_bytes().to_vec())
        );
        assert!(!iter.next());
    }

    fn fmix32(mut h: u32) -> u32 {
        h ^= h >> 16;
        h = h.wrapping_mul(0x85ebca6b);
        h ^= h >> 13;
        h = h.wrapping_mul(0xc2b2ae35);
        h ^= h >> 16;
        h
    }

    #[test]
    fn test_more_keys_and_values_next() {
        let mut c = C::new();
        for i in 0..200 {
            let key: String = format!("key{}", i);
            let val: String = format!("val{}", i);

            c.add(&key, &val);
        }

        let mut path = vec![c.tree.page_manager.get_page(c.tree.root)];
        let mut positions = vec![0];
        while path[path.len() - 1].b_type() == NodeType::Node {
            let new_node_ptr = path[path.len() - 1].get_ptr(positions[positions.len() - 1]);
            let new_node = c.tree.page_manager.get_page(new_node_ptr);
            path.push(new_node);
            positions.push(0);
        }
        let mut iter = BTreeIterator {
            tree: &c.tree,

            path,
            positions,
        };

        let (last_key, _) = iter.deref();
        let mut iter_count = 1;

        while iter.next() {
            iter_count += 1;
            let (key, _) = iter.deref();
            assert_eq!(key.cmp(&last_key), Ordering::Greater);
        }

        assert_eq!(iter_count, c.reference.len() + 1)
    }

    #[test]
    fn test_more_keys_and_values_prev() {
        let mut c = C::new();
        for i in 0..200 {
            let key: String = format!("key{}", i);
            let val: String = format!("val{}", i);

            c.add(&key, &val);
        }

        let root = c.tree.page_manager.get_page(c.tree.root);
        let mut positions = vec![root.num_keys() - 1];
        let mut path = vec![root];
        while path[path.len() - 1].b_type() == NodeType::Node {
            let new_node_ptr = path[path.len() - 1].get_ptr(positions[positions.len() - 1]);
            let new_node = c.tree.page_manager.get_page(new_node_ptr);
            positions.push(new_node.num_keys() - 1);
            path.push(new_node);
        }
        let mut iter = BTreeIterator {
            tree: &c.tree,

            path,
            positions,
        };

        let (last_key, _) = iter.deref();
        let mut iter_count = 1;

        while iter.prev() {
            iter_count += 1;
            let (key, _) = iter.deref();
            assert_eq!(key.cmp(&last_key), Ordering::Less);
        }

        assert_eq!(iter_count, c.reference.len() + 1)
    }

    #[test]
    fn test_random_key_and_val_lengths_next() {
        let mut c = C::new();
        let mut rng = StdRng::seed_from_u64(0);
        for i in 0..50 {
            let klen = fmix32(2 * i) % BTREE_MAX_KEY_SIZE as u32;
            let vlen = fmix32(2 * i + 1) % BTREE_MAX_VAL_SIZE as u32;
            if klen == 0 {
                continue;
            }

            let key: String = (0..klen)
                .map(|_| (rng.gen_range(32..127)) as u8 as char)
                .collect();

            let val: String = (0..vlen)
                .map(|_| (rng.gen_range(32..127)) as u8 as char)
                .collect();

            c.add(&key, &val);
        }

        let mut path = vec![c.tree.page_manager.get_page(c.tree.root)];
        let mut positions = vec![0];
        while path[path.len() - 1].b_type() == NodeType::Node {
            let new_node_ptr = path[path.len() - 1].get_ptr(positions[positions.len() - 1]);
            let new_node = c.tree.page_manager.get_page(new_node_ptr);
            path.push(new_node);
            positions.push(0);
        }
        let mut iter = BTreeIterator {
            tree: &c.tree,

            path,
            positions,
        };

        let (last_key, _) = iter.deref();
        let mut iter_count = 1;

        while iter.next() {
            iter_count += 1;
            let (key, _) = iter.deref();
            assert_eq!(key.cmp(&last_key), Ordering::Greater);
        }

        assert_eq!(iter_count, c.reference.len() + 1)
    }

    #[test]
    fn test_random_key_and_val_lengths_prev() {
        let mut c = C::new();
        let mut rng = StdRng::seed_from_u64(0);
        for i in 0..50 {
            let klen = fmix32(2 * i) % BTREE_MAX_KEY_SIZE as u32;
            let vlen = fmix32(2 * i + 1) % BTREE_MAX_VAL_SIZE as u32;
            if klen == 0 {
                continue;
            }

            let key: String = (0..klen)
                .map(|_| (rng.gen_range(32..127)) as u8 as char)
                .collect();

            let val: String = (0..vlen)
                .map(|_| (rng.gen_range(32..127)) as u8 as char)
                .collect();

            c.add(&key, &val);
        }

        let root = c.tree.page_manager.get_page(c.tree.root);
        let mut positions = vec![root.num_keys() - 1];
        let mut path = vec![root];
        while path[path.len() - 1].b_type() == NodeType::Node {
            let new_node_ptr = path[path.len() - 1].get_ptr(positions[positions.len() - 1]);
            let new_node = c.tree.page_manager.get_page(new_node_ptr);
            positions.push(new_node.num_keys() - 1);
            path.push(new_node);
        }
        let mut iter = BTreeIterator {
            tree: &c.tree,
            path,
            positions,
        };

        let (last_key, _) = iter.deref();
        let mut iter_count = 1;

        while iter.prev() {
            iter_count += 1;
            let (key, _) = iter.deref();
            assert_eq!(key.cmp(&last_key), Ordering::Less);
        }

        assert_eq!(iter_count, c.reference.len() + 1)
    }
}
