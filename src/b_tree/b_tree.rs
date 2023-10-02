use crate::b_tree::b_node::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE};
use std::cmp::Ordering;

use super::b_node::{BNode, BNodeType, BTREE_PAGE_SIZE, HEADER};

enum MergeDirection {
    LEFT(BNode),
    RIGHT(BNode),
    NONE,
}

pub struct BTree {
    // pointer (a nonzero page number)
    pub root: u64,

    // callbacks for managing on-disk pages (to keep the b-tree logic pure)
    pub get: Box<dyn Fn(u64) -> BNode>,
    pub new: Box<dyn Fn(BNode) -> u64>,
    pub del: Box<dyn Fn(u64)>,
}

impl BTree {
    // insert a KV into a node, the result might be split into 2 nodes.
    // the caller is responsible for deallocating the input node
    // and splitting and allocating result nodes. Returns the result node which is double sized
    fn tree_insert(&mut self, mut node_to_have_key: BNode, key: &Vec<u8>, val: &Vec<u8>) -> BNode {
        let new_node: BNode;
        // Where to insert
        let idx = node_to_have_key.node_lookup_le(&key);

        match node_to_have_key.b_type() {
            BNodeType::LEAF => match node_to_have_key.get_key(idx).cmp(key) {
                Ordering::Equal => node_to_have_key.leaf_update(idx, key, val),
                _ => node_to_have_key.leaf_insert(idx + 1, key, val),
            },
            BNodeType::NODE => self.node_insert(node_to_have_key, idx, key, val),
        }
    }

    fn tree_delete(&mut self, mut node_with_key: BNode, key: &Vec<u8>) -> Option<BNode> {
        let new_node: BNode;
        // Where to insert
        let idx = node_with_key.node_lookup_le(&key);

        match node_with_key.b_type() {
            BNodeType::LEAF => match node_with_key.get_key(idx).cmp(key) {
                Ordering::Equal => Some(node_with_key.leaf_delete(idx)),
                _ => None,
            },
            BNodeType::NODE => self.node_delete(node_with_key, idx, key),
        }
    }

    /** inserts a key into an internal node, the result will be a double sized node */
    fn node_insert(
        &mut self,
        node_to_have_key: BNode,
        idx: u16,
        key: &Vec<u8>,
        val: &Vec<u8>,
    ) -> BNode {
        // get and deallocate the kid node
        let kid_ptr = node_to_have_key.get_ptr(idx);
        let mut kid_node = (self.get)(kid_ptr);
        (self.del)(kid_ptr);

        //recursive insertion to the kid node
        kid_node = self.tree_insert(kid_node, key, val);

        //split the result
        let (n_split, splited) = kid_node.split3();

        // update the kids links
        self.node_replace_kid_n(2 * BTREE_PAGE_SIZE, node_to_have_key, idx, splited)
    }

    fn node_delete(&mut self, node_with_key: BNode, idx: u16, key: &Vec<u8>) -> Option<BNode> {
        // recurse into the kid
        let kid_ptr = node_with_key.get_ptr(idx);
        let node_with_key_removed = self.tree_delete((self.get)(kid_ptr), key);
        node_with_key_removed.as_ref()?;

        let mut updated_node = node_with_key_removed.unwrap();
        (self.del)(kid_ptr);

        // merge or redistribute
        let merge_direction = self.should_merge(&node_with_key, idx, &mut updated_node);
        Some(match merge_direction {
            MergeDirection::LEFT(sibling) => {
                let mut merged = sibling.node_merge(updated_node);
                (self.del)(node_with_key.get_ptr(idx - 1));
                let merged_first_key = merged.get_key(0);
                node_with_key.node_replace_2_kid(idx - 1, (self.new)(merged), &merged_first_key)
            }
            MergeDirection::RIGHT(sibling) => {
                let mut merged = updated_node.node_merge(sibling);
                (self.del)(node_with_key.get_ptr(idx + 1));
                let merged_first_key = merged.get_key(0);
                node_with_key.node_replace_2_kid(idx, (self.new)(merged), &merged_first_key)
            }
            MergeDirection::NONE => {
                assert!(updated_node.num_keys() > 0);
                self.node_replace_kid_n(BTREE_PAGE_SIZE, node_with_key, idx, vec![updated_node])
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
            BNode::new_with_size(BNodeType::NODE, old_num_keys - 1 + num_new, new_node_size);
        new_node.node_append_range(&old_node, 0, 0, idx);
        for (i, mut node) in new_children.into_iter().enumerate() {
            let node_first_key = node.get_key(0);
            new_node.node_append_kv(idx + i as u16, (self.new)(node), &node_first_key, &vec![])
        }
        new_node.node_append_range(
            &old_node,
            idx + num_new,
            idx + 1,
            old_num_keys - (idx + 1),
        );

        new_node
    }

    fn should_merge(
        &self,
        node_with_key: &BNode,
        idx: u16,
        updated_node: &BNode,
    ) -> MergeDirection {
        if updated_node.num_bytes() > BTREE_PAGE_SIZE as u16 / 4 {
            return MergeDirection::NONE;
        }

        if idx > 0 {
            let mut sibling: BNode = (self.get)(node_with_key.get_ptr(idx - 1));
            let merged_size = sibling.num_bytes() + updated_node.num_bytes() - HEADER;

            if merged_size <= BTREE_PAGE_SIZE as u16 {
                return MergeDirection::LEFT(sibling);
            };
        }

        if idx + 1 < node_with_key.num_keys() {
            let mut sibling: BNode = (self.get)(node_with_key.get_ptr(idx + 1));
            let merged_size = sibling.num_bytes() + updated_node.num_bytes() - HEADER;

            if merged_size <= BTREE_PAGE_SIZE as u16 {
                return MergeDirection::RIGHT(sibling);
            };
        }

        MergeDirection::NONE
    }

    pub fn Delete(&mut self, key: &Vec<u8>) -> bool {
        assert!(!key.is_empty());
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);

        if self.root == 0 {
            return false;
        };

        let node_with_removed_key = self.tree_delete((self.get)(self.root), key);
        if node_with_removed_key.is_none() {
            return false;
        };
        let mut updated_node = node_with_removed_key.unwrap();

        (self.del)(self.root);
        if updated_node.b_type() == BNodeType::NODE && updated_node.num_keys() == 1 {
            // Remove a level
            self.root = updated_node.get_ptr(0);
        } else {
            self.root = (self.new)(updated_node);
        };

        true
    }

    pub fn Insert(&mut self, key: &Vec<u8>, val: &Vec<u8>) {
        assert!(!key.is_empty());
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);
        assert!(val.len() <= BTREE_MAX_VAL_SIZE);

        if self.root == 0 {
            let mut root = BNode::new(BNodeType::LEAF, 2);

            root.node_append_kv(0, 0, &vec![], &vec![]);
            root.node_append_kv(1, 0, key, val);
            self.root = (self.new)(root);
            return;
        };

        let node = (self.get)(self.root);
        (self.del)(self.root);

        let node = self.tree_insert(node, key, val);
        let (n_split, mut splitted) = node.split3();
        if n_split > 1 {
            // the root was split, add a new level
            let mut root = BNode::new(BNodeType::NODE, n_split);
            for (i, mut k_node) in splitted.into_iter().enumerate() {
                let key = k_node.get_key(0);
                let ptr = (self.new)(k_node);
                root.node_append_kv(i as u16, ptr, &key, &vec![]);
            }
            self.root = (self.new)(root);
        } else {
            self.root = (self.new)(splitted.remove(0));
        };
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        rc::Rc, iter::StepBy,
    };

    use super::*;
    extern crate rand;

    use rand::Rng;

    struct C {
        pub tree: BTree,
        pub reference: HashMap<String, String>,
        pub pages: Rc<RefCell<HashMap<u64, BNode>>>,
    }
    impl C {
        fn new() -> C {
            let pages: Rc<RefCell<HashMap<u64, BNode>>> =
                Rc::new(RefCell::new(HashMap::<u64, BNode>::new()));

            let get = {
                let pages: Rc<RefCell<HashMap<u64, BNode>>> = Rc::clone(&pages);
                Box::new(move |ptr| {
                    pages.borrow().get(&ptr).unwrap().clone()
                })
            };

            let new = {
                let pages: Rc<RefCell<HashMap<u64, BNode>>> = Rc::clone(&pages);
                Box::new(move |mut node: BNode| {
                    assert!(node.num_bytes() <= BTREE_PAGE_SIZE as u16);
                    let mut rng = rand::thread_rng();
                    let mut random_ptr: u64 = rng.gen();
                    while pages.borrow().contains_key(&random_ptr) {
                        random_ptr = rng.gen();
                    }
                    pages.borrow_mut().insert(random_ptr, node);
                    random_ptr
                })
            };

            let del = {
                let pages: Rc<RefCell<HashMap<u64, BNode>>> = Rc::clone(&pages);
                Box::new(move |ptr| {
                    pages.borrow_mut().remove(&ptr);
                })
            };

            C {
                tree: BTree {
                    root: 0,
                    get,
                    new,
                    del,
                },
                reference: HashMap::new(),
                pages,
            }
        }

        fn add(&mut self, key: &str, val: &str) {
            self.tree
                .Insert(&key.as_bytes().to_vec(), &val.as_bytes().to_vec());
            self.reference.insert(key.to_string(), val.to_string());
        }

        fn delete(&mut self, key: &str) -> bool {
            let remove = self.reference.remove(key);
            let did_remove = self.tree.Delete(&key.as_bytes().to_vec());
            assert_eq!(remove.is_some(), did_remove);
            did_remove
        }

        fn node_dump(&mut self, ptr: u64, keys: &mut Vec<String>, vals: &mut Vec<String>) {
            let mut node = (self.tree.get)(ptr);
            let n_keys = node.num_keys();
            match node.b_type() {
                BNodeType::NODE => {
                    for i in 0..n_keys {
                        let ptr = node.get_ptr(i);
                        self.node_dump(ptr, keys, vals);
                    }
                }
                BNodeType::LEAF => {
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

        fn node_verify(&self, mut node: BNode) {
            let num_keys = node.num_keys();
            assert!(num_keys >= 1);
            if node.b_type() == BNodeType::LEAF {
                return;
            };

            for i in 0..num_keys {
                let key = node.get_key(i);
                let mut kid = (self.tree.get)(node.get_ptr(i));
                assert_eq!(
                    kid.get_key(0),
                    key,
                    "First key of kid is not equal to associated key"
                );
                self.node_verify(kid);
            }
        }

        fn verify(&mut self) {
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
            self.node_verify((self.tree.get)(self.tree.root));
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
        assert_eq!(1, c.pages.borrow().len());
        assert_eq!(1, (c.tree.get)(c.tree.root).num_keys());
    }

    #[test]
    fn test_random_key_and_val_lengths() {
        let mut c = C::new();
        let mut rng = rand::thread_rng();
        for i in 0..2000 {
            let klen = fmix32(2*i) % BTREE_MAX_KEY_SIZE as u32;
            let vlen = fmix32(2*i+1) % BTREE_MAX_VAL_SIZE as u32;
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
        let mut rng = rand::thread_rng();
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

            for i in 0..size {
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
        
    }
}
