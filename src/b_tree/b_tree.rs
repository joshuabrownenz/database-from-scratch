use core::num;
use std::cmp::Ordering;

use crate::b_tree::b_node::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE};

use super::b_node::{BNode, BNodeType, BTREE_PAGE_SIZE, HEADER};

pub enum MergeDirection {
    LEFT(BNode),
    RIGHT(BNode),
    NONE,
}

pub struct BTree {
    // pointer (a nonzero page number)
    pub root: u64,

    // callbacks for managing on-disk pages (to keep the b-tree logic pure)
    pub get: fn(u64) -> BNode,
    pub new: fn(BNode) -> u64,
    pub del: fn(u64),
}

impl BTree {
    // insert a KV into a node, the result might be split into 2 nodes.
    // the caller is responsible for deallocating the input node
    // and splitting and allocating result nodes. Returns the result node
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
            BNodeType::NODE => Some(self.node_delete(node_with_key, idx, key)),
        }
    }

    fn node_insert(
        &mut self,
        mut node_to_have_key: BNode,
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
        self.node_replace_kid_n(node_to_have_key, idx, splited)
    }

    fn node_delete(&mut self, mut node_with_key: BNode, idx: u16, key: &Vec<u8>) -> BNode {
        // recurse into the kid
        let kid_ptr = node_with_key.get_ptr(idx);
        let node_with_key_removed = self.tree_delete((self.get)(kid_ptr), key);
        if node_with_key_removed.is_none() {
            return node_with_key;
        };
        let mut updated_node = node_with_key_removed.unwrap();
        (self.del)(kid_ptr);

        // merge or redistribute
        let merge_direction = self.should_merge(&mut node_with_key, idx, &mut updated_node);
        match merge_direction {
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
                self.node_replace_kid_n(node_with_key, idx, vec![updated_node])
            }
        }
    }

    fn node_replace_kid_n(
        &mut self,
        mut old_node: BNode,
        idx: u16,
        new_children: Vec<BNode>,
    ) -> BNode {
        // replace the kid node with the splited node
        let num_new = new_children.len() as u16;
        let old_num_keys = old_node.num_keys();

        // Replacing one old child node with new children (2 or 3)
        let mut new_node = BNode::new(BNodeType::NODE, old_num_keys - 1 + num_new);
        new_node.node_append_range(&mut old_node, 0, 0, idx);
        for (i, mut node) in new_children.into_iter().enumerate() {
            let node_first_key = node.get_key(0);
            new_node.node_append_kv(idx + i as u16, (self.new)(node), &node_first_key, &vec![])
        }
        new_node.node_append_range(
            &mut old_node,
            idx + num_new,
            idx + 1,
            old_num_keys - (idx + 1),
        );

        new_node
    }

    fn should_merge(
        &mut self,
        node_with_key: &mut BNode,
        idx: u16,
        updated_node: &mut BNode,
    ) -> MergeDirection {
        if updated_node.num_keys() > BTREE_PAGE_SIZE as u16 / 4 {
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
        assert!(key.len() > 0);
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
            self.root = updated_node.get_ptr(0);
        } else {
            self.root = (self.new)(updated_node);
        };

        true
    }

    pub fn Insert(&mut self, key: &Vec<u8>, val: &Vec<u8>) {
        assert!(key.len() > 0);
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);
        assert!(val.len() <= BTREE_MAX_VAL_SIZE);

        if self.root == 0 {
            let mut root = BNode::new(BNodeType::LEAF, 1);

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

// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     use super::*;

//     struct C {
//         pub tree: BTree,
//         pub references: HashMap<String, String>,
//         pub pages: HashMap<u64, BNode>,
//     }

//     fn newC() -> C {
//         let pages = HashMap::<u64, BNode>::new();

//         fn get(ptr :u64) -> BNode {
//             pages.get(&ptr).unwrap().clone()
//         }

//         C {
//             tree: BTree {
//                 root: 0,
//                 get: |ptr|
//                 { 
//                     pages.get(&ptr).unwrap().clone()
//                 },
//                 new: |node| {
//                     let ptr = pages.len() as u64 + 1;
//                     pages.insert(ptr, node.clone());
//                     ptr
//                 },
//                 del: |ptr| {
//                     pages.remove(&ptr);
//                 },
//             },
//             references: HashMap::new(),
//             pages: pages,
//         }
//     }
// }
