pub mod b_node;
pub mod btree_iter;

use self::{
    b_node::{BNode, NodeType, BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE, BTREE_PAGE_SIZE, HEADER},
    btree_iter::BTreeIterator,
};
use std::cmp::Ordering;

enum MergeDirection {
    Left(BNode),
    Right(BNode),
    None,
}

pub enum CmpOption {
    GT,
    GE,
    LT,
    LE,
}

#[derive(PartialEq)]
pub enum InsertMode {
    Upsert,     // insert or replace
    UpdateOnly, // update existing keys
    InsertOnly, // only add new keys
}

pub struct InsertRequest {
    // tree: &'a mut BTree, // Not sure why we need this
    // out
    pub added: bool, // added a new key
    // in
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub mode: InsertMode,
}

impl InsertRequest {
    pub fn new(key: Vec<u8>, val: Vec<u8>) -> InsertRequest {
        InsertRequest {
            key,
            val,
            mode: InsertMode::Upsert,
            added: false,
        }
    }
    pub fn mode(mut self, mode: InsertMode) -> InsertRequest {
        self.mode = mode;
        self
    }
}

pub trait BTreePageManager {
    fn page_get(&self, ptr: u64) -> BNode;
    fn page_new(&mut self, node: BNode) -> u64;
    fn page_del(&mut self, ptr: u64);
}

pub struct BTree<B: BTreePageManager> {
    // pointer (a nonzero page number)
    pub root: u64,
    pub page_manager: B,
}

impl<'a, B: BTreePageManager> BTree<B> {
    pub fn new(page_manager: B) -> BTree<B> {
        BTree {
            root: 0,
            page_manager,
        }
    }

    /**
     * insert a KV into a node, the result might be split into 2 nodes.
    * the caller is responsible for deallocating the input node
    * and splitting and allocating result nodes. Returns the result node which is double sized

    * Returns Some(BNode) if an update takes place
     */
    fn tree_insert(
        &mut self,
        node_to_have_key: BNode,
        request: &mut InsertRequest,
    ) -> Option<BNode> {
        // Where to insert
        let idx = node_to_have_key.node_lookup_le(&request.key);

        match node_to_have_key.b_type() {
            NodeType::Leaf => {
                match node_to_have_key.get_key(idx).cmp(&request.key) {
                    Ordering::Equal => {
                        if request.mode == InsertMode::InsertOnly {
                            // Key already in the tree and mode is insert only. Don't insert.
                            return None;
                        }
                        if node_to_have_key.get_val(idx).cmp(&request.val) == Ordering::Equal {
                            // Key and value already in the tree so don't insert.
                            return None;
                        }

                        Some(node_to_have_key.leaf_update(idx, &request.key, &request.val))
                    }
                    _ => {
                        if request.mode == InsertMode::UpdateOnly {
                            // Key not in the tree and mode is update only. Don't insert.
                            return None;
                        }
                        request.added = true;
                        Some(node_to_have_key.leaf_insert(idx + 1, &request.key, &request.val))
                    }
                }
            }
            NodeType::Node => self.node_insert(node_to_have_key, idx, request),
        }
    }

    fn tree_delete(&mut self, node_with_key: BNode, key: &Vec<u8>) -> Option<BNode> {
        // Where to insert
        let idx = node_with_key.node_lookup_le(key);

        match node_with_key.b_type() {
            NodeType::Leaf => match node_with_key.get_key(idx).cmp(key) {
                Ordering::Equal => Some(node_with_key.leaf_delete(idx)),
                _ => None,
            },
            NodeType::Node => self.node_delete(node_with_key, idx, key),
        }
    }

    /** inserts a key into an internal node, the result will be a double sized node */
    fn node_insert(
        &mut self,
        node_to_have_key: BNode,
        idx: u16,
        request: &mut InsertRequest,
    ) -> Option<BNode> {
        // get and deallocate the kid node
        let kid_ptr = node_to_have_key.get_ptr(idx);
        let kid_node = self.page_manager.page_get(kid_ptr);

        //recursive insertion to the kid node
        let kid_node = self.tree_insert(kid_node, request)?;

        self.page_manager.page_del(kid_ptr);

        //split the result
        let (_, splited) = kid_node.split3();

        // update the kids links
        Some(self.node_replace_kid_n(2 * BTREE_PAGE_SIZE, node_to_have_key, idx, splited))
    }

    fn node_delete(&mut self, node_with_key: BNode, idx: u16, key: &Vec<u8>) -> Option<BNode> {
        // recurse into the kid
        let kid_ptr = node_with_key.get_ptr(idx);
        let node_with_key_removed = self.tree_delete(self.page_manager.page_get(kid_ptr), key);
        node_with_key_removed.as_ref()?;

        let updated_node = node_with_key_removed.unwrap();
        self.page_manager.page_del(kid_ptr);

        // merge or redistribute
        let merge_direction = self.should_merge(&node_with_key, idx, &updated_node);
        Some(match merge_direction {
            MergeDirection::Left(sibling) => {
                let merged = sibling.node_merge(updated_node);
                self.page_manager.page_del(node_with_key.get_ptr(idx - 1));
                let merged_first_key = merged.get_key(0);
                node_with_key.node_replace_2_kid(
                    idx - 1,
                    self.page_manager.page_new(merged),
                    &merged_first_key,
                )
            }
            MergeDirection::Right(sibling) => {
                let merged = updated_node.node_merge(sibling);
                self.page_manager.page_del(node_with_key.get_ptr(idx + 1));
                let merged_first_key = merged.get_key(0);
                node_with_key.node_replace_2_kid(
                    idx,
                    self.page_manager.page_new(merged),
                    &merged_first_key,
                )
            }
            MergeDirection::None => {
                if updated_node.num_keys() == 0 {
                    // kid is empty after deletion and has no sibling to merge with.
                    // this happens when its parent has only one kid.
                    // discard the empty kid and return the parent as an empty node.
                    assert!(node_with_key.num_keys() == 1 && idx == 0);
                    BNode::new(NodeType::Node, 0)
                    // the empty node will be eliminated before reaching root.
                } else {
                    self.node_replace_kid_n(BTREE_PAGE_SIZE, node_with_key, idx, vec![updated_node])
                }
            }
        })
    }

    /** Replace the kid node with the new children (2 or 3) */
    fn node_replace_kid_n(
        &mut self,
        new_node_size: usize,
        old_node: BNode,
        idx: u16,
        new_children: Vec<BNode>,
    ) -> BNode {
        // replace the kid node with the splited node
        let num_new = new_children.len() as u16;
        let old_num_keys = old_node.num_keys();

        // Replacing one old child node with new children (2 or 3)
        let mut new_node =
            BNode::new_with_size(NodeType::Node, old_num_keys - 1 + num_new, new_node_size);
        new_node.node_append_range(&old_node, 0, 0, idx);
        for (i, node) in new_children.into_iter().enumerate() {
            let node_first_key = node.get_key(0);
            new_node.node_append_kv(
                idx + i as u16,
                self.page_manager.page_new(node),
                &node_first_key,
                &vec![],
            )
        }
        new_node.node_append_range(&old_node, idx + num_new, idx + 1, old_num_keys - (idx + 1));

        new_node
    }

    fn should_merge(
        &self,
        node_with_key: &BNode,
        idx: u16,
        updated_node: &BNode,
    ) -> MergeDirection {
        if updated_node.num_bytes() > BTREE_PAGE_SIZE as u16 / 4 {
            return MergeDirection::None;
        }

        if idx > 0 {
            let sibling: BNode = self.page_manager.page_get(node_with_key.get_ptr(idx - 1));
            let merged_size = sibling.num_bytes() + updated_node.num_bytes() - HEADER;

            if merged_size <= BTREE_PAGE_SIZE as u16 {
                return MergeDirection::Left(sibling);
            };
        }

        if idx + 1 < node_with_key.num_keys() {
            let sibling: BNode = self.page_manager.page_get(node_with_key.get_ptr(idx + 1));
            let merged_size = sibling.num_bytes() + updated_node.num_bytes() - HEADER;

            if merged_size <= BTREE_PAGE_SIZE as u16 {
                return MergeDirection::Right(sibling);
            };
        }

        MergeDirection::None
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> bool {
        assert!(!key.is_empty());
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);

        if self.root == 0 {
            return false;
        };

        let node_with_removed_key = self.tree_delete(self.page_manager.page_get(self.root), key);
        if node_with_removed_key.is_none() {
            return false;
        };
        let updated_node = node_with_removed_key.unwrap();

        self.page_manager.page_del(self.root);
        if updated_node.b_type() == NodeType::Node && updated_node.num_keys() == 1 {
            // Remove a level
            self.root = updated_node.get_ptr(0);
        } else {
            self.root = self.page_manager.page_new(updated_node);
        };

        true
    }

    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) -> bool {
        let request = InsertRequest::new(key, val);
        let response = self.insert_exec(request);
        response.added
    }

    pub fn insert_exec(&mut self, mut request: InsertRequest) -> InsertRequest {
        assert!(!request.key.is_empty());
        assert!(request.key.len() <= BTREE_MAX_KEY_SIZE);
        assert!(request.val.len() <= BTREE_MAX_VAL_SIZE);

        if self.root == 0 {
            let mut root = BNode::new(NodeType::Leaf, 2);

            root.node_append_kv(0, 0, &vec![], &vec![]);
            root.node_append_kv(1, 0, &request.key, &request.val);
            self.root = self.page_manager.page_new(root);

            request.added = true;
            return request;
        };

        let node = self.page_manager.page_get(self.root);

        let updated = self.tree_insert(node, &mut request);
        if updated.is_none() {
            return request;
        }

        self.page_manager.page_del(self.root);

        let node = updated.unwrap();
        let (n_split, mut splitted) = node.split3();
        if n_split > 1 {
            // the root was split, add a new level
            let mut root = BNode::new(NodeType::Node, n_split);
            for (i, k_node) in splitted.into_iter().enumerate() {
                let key = k_node.get_key(0);
                let ptr = self.page_manager.page_new(k_node);
                root.node_append_kv(i as u16, ptr, &key, &vec![]);
            }
            self.root = self.page_manager.page_new(root);
        } else {
            self.root = self.page_manager.page_new(splitted.remove(0));
        };

        request
    }

    pub fn get_value(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        assert!(!key.is_empty());
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);

        if self.root == 0 {
            return None;
        };

        let mut node = self.page_manager.page_get(self.root);
        loop {
            let idx = node.node_lookup_le(key);
            match node.b_type() {
                NodeType::Leaf => match node.get_key(idx).cmp(key) {
                    Ordering::Equal => return Some(node.get_val(idx)),
                    _ => return None,
                },
                NodeType::Node => {
                    let ptr = node.get_ptr(idx);
                    node = self.page_manager.page_get(ptr);
                }
            }
        }
    }

    fn seek_le(&'a mut self, key: &Vec<u8>) -> BTreeIterator<'a, B> {
        let mut path = Vec::new();
        let mut positions = Vec::new();

        let mut ptr = self.root;
        while ptr != 0 {
            let node = self.page_manager.page_get(ptr);
            let node_type = node.b_type();
            let idx = node.node_lookup_le(key);
            if node_type == NodeType::Node {
                ptr = node.get_ptr(idx);
            } else {
                ptr = 0;
            }
            path.push(node);
            positions.push(idx);
        }

        BTreeIterator::new(self, path, positions)
    }

    pub fn seek(&'a mut self, key: &Vec<u8>, compare: CmpOption) -> BTreeIterator<'a, B> {
        let mut iter = self.seek_le(key);
        if let CmpOption::LE = compare {
        } else {
            let (current_key, _) = iter.deref();
            if !Self::cmp_ok(&current_key, &compare, key) {
                // Off by one
                match compare {
                    CmpOption::GE | CmpOption::GT => iter.next(),
                    CmpOption::LE | CmpOption::LT => iter.prev(),
                };
            };
        };

        iter
    }

    fn cmp_ok(key: &Vec<u8>, compare: &CmpOption, reference: &Vec<u8>) -> bool {
        match compare {
            CmpOption::GT => key > reference,
            CmpOption::GE => key >= reference,
            CmpOption::LT => key < reference,
            CmpOption::LE => key <= reference,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    extern crate rand;

    use rand::seq::SliceRandom;
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

    struct C {
        pub tree: BTree<PageManager>,
        pub reference: HashMap<String, String>,
    }
    impl C {
        fn new() -> C {
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

        fn node_dump(&mut self, ptr: u64, keys: &mut Vec<String>, vals: &mut Vec<String>) {
            if ptr == 0 {
                panic!("ptr can't be 0");
            }

            let node = self.tree.page_manager.get_page(ptr);
            let n_keys = node.num_keys();
            match node.b_type() {
                NodeType::Node => {
                    for i in 0..n_keys {
                        let ptr = node.get_ptr(i);
                        self.node_dump(ptr, keys, vals);
                    }
                }
                NodeType::Leaf => {
                    for i in 0..n_keys {
                        let key = node.get_key(i).clone();
                        keys.push(String::from_utf8(key).unwrap());
                        vals.push(String::from_utf8(node.get_val(i).clone()).unwrap());
                    }
                }
            };
        }

        fn dump(&mut self) -> (Vec<String>, Vec<String>) {
            let mut keys = Vec::new();
            let mut vals = Vec::new();

            self.node_dump(self.tree.root, &mut keys, &mut vals);

            keys.remove(0);
            vals.remove(0);
            (keys, vals)
        }

        fn node_verify(&self, node: BNode) {
            let num_keys = node.num_keys();
            assert!(num_keys >= 1);
            if node.b_type() == NodeType::Leaf {
                return;
            };

            for i in 0..num_keys {
                let key = node.get_key(i);
                let kid = self.tree.page_manager.page_get(node.get_ptr(i));
                assert_eq!(
                    kid.get_key(0),
                    key,
                    "First key of kid is not equal to associated key"
                );
                self.node_verify(kid);
            }
        }

        fn verify(&mut self) {
            if self.tree.root == 0 {
                assert_eq!(self.reference.len(), 0);
                return;
            }

            let (keys, vals) = self.dump();
            let unique_keys: HashSet<_> = keys.iter().cloned().collect();
            assert_eq!(
                keys.len(),
                unique_keys.len(),
                "There are duplicate keys in the tree"
            );

            assert_eq!(keys.len(), self.reference.len());
            assert_eq!(keys.len(), vals.len());
            for i in 0..keys.len() {
                let key = &keys[i];
                let ref_value = self.reference.get(key).unwrap();
                let val_value = &vals[i];
                assert_eq!(ref_value, val_value);
            }

            // Verify node relationships are correct
            self.node_verify(self.tree.page_manager.page_get(self.tree.root));
        }
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
    fn test_perform_opperations_on_empty_kv() {
        let mut c = C::new();
        assert!(c.get("k").is_none());
        c.verify();
        assert!(!c.delete("k"));
        c.verify();
    }

    #[test]
    fn test_insert_two_items() {
        let mut c = C::new();
        c.add("1", "1");
        c.add("2", "2");
        c.verify();
    }

    #[test]
    fn test_insert() {
        let mut c = C::new();
        for i in 0..10000 {
            c.add(&i.to_string(), &i.to_string());
        }
        c.verify();
    }

    // With BNode Cursor<Vec<u8>> time 6.75, 6.76, 6.73
    // With BNode data: [u8; BTREE_PAGE_SIZE] time 3.78, 3.77, 3.78
    // With PageManager Trait time 3.66, 3.70
    #[test]
    fn test_basic() {
        let mut c = C::new();
        c.add("k", "v");
        c.verify();

        // Insert tests
        for i in 0..10000 {
            let key = format!("key{}", i);
            let val = format!("vvv{}", fmix32(-i as u32));
            c.add(&key.to_string(), &val.to_string());
        }
        c.verify();

        // Delete tests
        for i in 2000..10000 {
            let key = format!("key{}", i);
            assert!(c.delete(&key.to_string()));
        }
        c.verify();

        // Overwrite tests
        for i in 0..2000 {
            let key = format!("key{}", i);
            let val = format!("vvv{}", fmix32(i as u32));
            c.add(&key.to_string(), &val.to_string());
        }
        c.verify();

        assert!(!c.delete("kk"));

        for i in 0..2000 {
            let key = format!("key{}", i);
            assert!(c.delete(&key.to_string()));
        }
        c.verify();

        c.add("k", "v2");
        c.verify();
        c.delete("k");
        c.verify();

        // The dummy empty key
        assert_eq!(1, c.tree.page_manager.pages.len());
        assert_eq!(1, c.tree.page_manager.page_get(c.tree.root).num_keys());
    }

    #[test]
    fn test_random_key_and_val_lengths() {
        let mut c = C::new();
        let mut rng = StdRng::seed_from_u64(0);
        for i in 0..2000 {
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
        c.verify();
    }

    #[test]
    fn test_fit_of_different_key_lengths() {
        let mut rng = StdRng::seed_from_u64(0);
        for l in (1..BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE).step_by(20) {
            let mut c = C::new();

            let mut klen = l;
            if klen > BTREE_MAX_KEY_SIZE {
                klen = BTREE_MAX_KEY_SIZE;
            }

            let vlen = l - klen;

            let factor = BTREE_PAGE_SIZE / l;
            let mut size = factor * factor * 2;

            if size > 2000 {
                size = 2000;
            }

            if size < 10 {
                size = 10;
            }

            let mut kv_pairs: HashMap<String, String> = HashMap::new();
            for _ in 0..size {
                let key: String = (0..klen)
                    .map(|_| (rng.gen_range(32..127)) as u8 as char)
                    .collect();

                let val: String = (0..vlen)
                    .map(|_| (rng.gen_range(32..127)) as u8 as char)
                    .collect();

                c.add(&key, &val);
                kv_pairs.insert(key, val);
            }
            c.verify();

            let mut keys = kv_pairs.keys().cloned().collect::<Vec<String>>();
            let keys_len: usize = keys.len();
            for _ in 0..keys_len {
                let idx = rng.gen_range(0..keys.len());
                let key = keys.remove(idx);
                let value = kv_pairs.remove(&key).unwrap();
                assert_eq!(c.get(&key), Some(value.as_bytes().to_vec()));
                assert!(c.delete(&key));
            }
            c.verify();
        }
    }

    #[test]
    fn insert_exec_mode_upsert() {
        let mut c = C::new();
        // Insert root
        c.add("key", "val1");

        // Test that upsert works
        let request = InsertRequest::new("key".as_bytes().to_vec(), "val2".as_bytes().to_vec())
            .mode(InsertMode::Upsert);
        let response = c.tree.insert_exec(request);
        assert!(!response.added); // Not added because it was updated

        // Test that insert works
        assert_eq!(c.get("key"), Some("val2".as_bytes().to_vec()));
    }

    #[test]
    fn insert_exec_mode_insert_only() {
        let mut c = C::new();
        // Insert root
        c.add("key", "val1");

        // Test that insert only works
        let request = InsertRequest::new("key".as_bytes().to_vec(), "val2".as_bytes().to_vec())
            .mode(InsertMode::InsertOnly);
        let response = c.tree.insert_exec(request);
        assert!(!response.added); // Not added because it was updated

        // Test that insert works
        assert_eq!(c.get("key"), Some("val1".as_bytes().to_vec()));
    }

    #[test]
    fn insert_exec_mode_update_only_success() {
        let mut c = C::new();
        // Insert root
        c.add("key", "val1");

        // Test that update only works
        let request = InsertRequest::new("key".as_bytes().to_vec(), "val2".as_bytes().to_vec())
            .mode(InsertMode::UpdateOnly);
        let response = c.tree.insert_exec(request);
        assert!(!response.added); // Added because it was inserted

        // Test that insert works
        assert_eq!(c.get("key"), Some("val2".as_bytes().to_vec()));
    }

    #[test]
    fn insert_exec_mode_update_only_fail() {
        let mut c = C::new();
        // Insert root
        c.add("key", "val1");

        // Test that update only works
        let request =
            InsertRequest::new("new_key".as_bytes().to_vec(), "new_val".as_bytes().to_vec())
                .mode(InsertMode::UpdateOnly);
        let response = c.tree.insert_exec(request);
        assert!(!response.added); // Not added because it was updated

        // Test that insert works
        assert_eq!(c.get("new_key"), None);
    }

    #[test]
    fn seek_le_test_small_equal_to() {
        let mut c = C::new();
        c.add("key1", "val1");
        c.add("key2", "val2");
        c.add("key3", "val3");
        c.add("key4", "val4");
        c.add("key5", "val5");

        // Test seek_le with existing key
        let mut iter = c.tree.seek_le(&"key3".as_bytes().to_vec());
        assert_eq!(
            iter.deref(),
            ("key3".as_bytes().to_vec(), "val3".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("key4".as_bytes().to_vec(), "val4".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("key5".as_bytes().to_vec(), "val5".as_bytes().to_vec())
        );
        assert!(!iter.next());
    }

    #[test]
    fn seek_le_test_small_less_than() {
        let mut c = C::new();
        c.add("key1", "val1");
        c.add("key2", "val2");
        c.add("key4", "val4");
        c.add("key5", "val5");

        // Test seek_le with existing key
        let mut iter = c.tree.seek_le(&"key3".as_bytes().to_vec());
        assert_eq!(
            iter.deref(),
            ("key2".as_bytes().to_vec(), "val2".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("key4".as_bytes().to_vec(), "val4".as_bytes().to_vec())
        );
        assert!(iter.next());
        assert_eq!(
            iter.deref(),
            ("key5".as_bytes().to_vec(), "val5".as_bytes().to_vec())
        );
        assert!(!iter.next());
    }

    #[test]
    fn seek_le_test_large_equal_to() {
        let mut c = C::new();
        for i in 1..=100 {
            c.add(&format!("key{}", i), &format!("val{}", i));
        }

        let mut orderedItems = (1..=100)
            .map(|i| {
                (
                    format!("key{}", i).as_bytes().to_vec(),
                    format!("val{}", i).as_bytes().to_vec(),
                )
            })
            .collect::<Vec<(Vec<u8>, Vec<u8>)>>();
        orderedItems.sort();

        // Test seek_le with existing key
        let mut iter = c.tree.seek_le(&"key50".as_bytes().to_vec());

        let index = orderedItems
            .iter()
            .position(|(key, _)| key == &"key51".as_bytes().to_vec());
        for (expected_key, expected_value) in orderedItems.iter().skip(index.unwrap()) {
            assert!(iter.next());
            let (key, value) = iter.deref();
            assert_eq!(expected_key, &key);
            assert_eq!(expected_value, &value);
        }

        assert!(!iter.next());
    }

    #[test]
    fn seek_le_large_test_access() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut c = C::new();
        for i in 1..=10000 {
            c.add(
                &format!("key{}", fmix32(i as u32)),
                &format!("val{}", fmix32(-i as u32)),
            );
        }

        let mut randomised_items = (1..=10000)
            .map(|i| {
                (
                    format!("key{}", fmix32(i as u32)).as_bytes().to_vec(),
                    format!("val{}", fmix32(-i as u32)).as_bytes().to_vec(),
                )
            })
            .collect::<Vec<(Vec<u8>, Vec<u8>)>>();

        randomised_items.shuffle(&mut rng);

        for (key, value) in randomised_items.iter() {
            let iter = c.tree.seek_le(key);
            assert_eq!(iter.deref(), (key.clone(), value.clone()));
        }
    }

    #[test]
    fn seek_le_test_large_greater_than() {
        let mut c = C::new();
        c.add("key1", "val1");
        c.add("key2", "val2");
        c.add("key4", "val4");
        c.add("key5", "val5");

        // Test seek_le with key larger than any key in the tree
        let mut iter = c.tree.seek_le(&"key6".as_bytes().to_vec());
        assert_eq!(
            iter.deref(),
            ("key5".as_bytes().to_vec(), "val5".as_bytes().to_vec())
        );
        assert!(!iter.next());
    }

    #[test]
    fn seek_test() {
        let mut c = C::new();
        c.add("key1", "val1");
        c.add("key2", "val2");
        c.add("key3", "val3");
        c.add("key4", "val4");
        c.add("key5", "val5");

        // GE
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::GE);
        assert_eq!(
            iter.deref(),
            ("key3".as_bytes().to_vec(), "val3".as_bytes().to_vec())
        );

        // GT
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::GT);
        assert_eq!(
            iter.deref(),
            ("key4".as_bytes().to_vec(), "val4".as_bytes().to_vec())
        );

        // LE
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::LE);
        assert_eq!(
            iter.deref(),
            ("key3".as_bytes().to_vec(), "val3".as_bytes().to_vec())
        );

        // LT
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::LT);
        assert_eq!(
            iter.deref(),
            ("key2".as_bytes().to_vec(), "val2".as_bytes().to_vec())
        );
    }

    #[test]
    fn seek_test_missing_key() {
        let mut c = C::new();
        c.add("key1", "val1");
        c.add("key2", "val2");
        c.add("key4", "val4");
        c.add("key5", "val5");

        // GE
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::GE);
        assert_eq!(
            iter.deref(),
            ("key4".as_bytes().to_vec(), "val4".as_bytes().to_vec())
        );

        // GT
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::GT);
        assert_eq!(
            iter.deref(),
            ("key4".as_bytes().to_vec(), "val4".as_bytes().to_vec())
        );

        // LE
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::LE);
        assert_eq!(
            iter.deref(),
            ("key2".as_bytes().to_vec(), "val2".as_bytes().to_vec())
        );

        // LT
        let iter = c.tree.seek(&"key3".as_bytes().to_vec(), CmpOption::LT);
        assert_eq!(
            iter.deref(),
            ("key2".as_bytes().to_vec(), "val2".as_bytes().to_vec())
        );
    }
}
