extern crate byteorder;
use std::vec::Vec;
use std::io::{Cursor, Write, Read};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

// node format:
// | type | num_keys |  pointers  |   offsets  | key-values
// |  2B  |   2B  | num_keys * 8B | num_keys * 2B | ...

// key-value format:
// | klen | vlen | key | val |
// |  2B  |  2B  | ... | ... |


pub const HEADER: u16 = 4;

pub const BTREE_PAGE_SIZE: usize = 4096;
pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BNodeType {
    NODE = 1,
    LEAF = 2,
}

impl BNodeType {
    pub fn value(&self) -> u16 {
        *self as u16
    }
}

#[derive(Clone)]
pub struct BNode {
    pub cursor: Cursor<Vec<u8>>,
} 

impl BNode {
    pub fn new(b_type : BNodeType, num_keys: u16) -> BNode {
        let mut new_node = BNode { 
            cursor: Cursor::new(vec![0; BTREE_PAGE_SIZE as usize]) 
        };
        new_node.set_header(b_type, num_keys);
        new_node
    }
    pub fn new_with_size(b_type : BNodeType, num_keys: u16, size : usize) -> BNode {
        let mut new_node = BNode {
            cursor: Cursor::new(vec![0; size]),
        };
        new_node.set_header(b_type, num_keys);
        new_node
    }

    // Header
    fn set_header(&mut self, b_type: BNodeType, num_keys: u16) {
        self.cursor.set_position(0);
        self.cursor.write_u16::<LittleEndian>(b_type.value()).unwrap();
        self.cursor.write_u16::<LittleEndian>(num_keys).unwrap();
    }

    pub fn b_type(&mut self) -> BNodeType {
        self.cursor.set_position(0);

        match self.cursor.read_u16::<LittleEndian>().unwrap() {
            1 => BNodeType::NODE,
            2 => BNodeType::LEAF,
            n => panic!("Invalid BNode type {}", n),
        }
    }

    pub fn num_keys(&mut self) -> u16 {
        self.cursor.set_position(2);
        self.cursor.read_u16::<LittleEndian>().unwrap()
    }

    // Page Pointers
    pub fn get_ptr(&mut self, idx: u16) -> u64 {
        assert!(idx < self.num_keys());
        let pos = HEADER + 8 * idx;
        self.cursor.set_position(pos as u64);
        self.cursor.read_u64::<LittleEndian>().unwrap()
    }

    fn set_ptr(&mut self, idx : u16, val : u64) {
        assert!(idx < self.num_keys());
        let pos = HEADER + 8 * idx;
        self.cursor.set_position(pos as u64);
        self.cursor.write_u64::<LittleEndian>(val).unwrap();
    }

    // Offset List
    pub fn offset_pos(&mut self, idx : u16) -> u16 {
        assert!(1 <= idx && idx <= self.num_keys());
         HEADER + 8 * self.num_keys() + 2 * (idx - 1)
    }

    pub fn get_offset(&mut self, idx : u16) -> u16 {
        if idx == 0 {
            return 0;
        }
        let pos = self.offset_pos(idx);
        self.cursor.set_position(pos as u64);
        self.cursor.read_u16::<LittleEndian>().unwrap()
    }

    fn set_offset(&mut self, idx : u16, val : u16) {
        assert!(idx <= self.num_keys());
        let pos = self.offset_pos(idx);
        self.cursor.set_position(pos as u64);
        self.cursor.write_u16::<LittleEndian>(val).unwrap();
    }

    // key-values
    pub fn kv_pos(&mut self, idx : u16) -> u16 {
        let num_keys = self.num_keys();
        assert!(idx <= num_keys);
        HEADER as u16 + 10 * num_keys + self.get_offset(idx)
    }

    pub fn get_key(&mut self, idx: u16) -> Vec<u8> {
        assert!(idx < self.num_keys());

        // Position of the key
        let pos = self.kv_pos(idx);
        self.cursor.set_position(pos as u64);

        // Length of the key
        let key_length = self.cursor.read_u16::<LittleEndian>().unwrap();
        self.cursor.set_position(pos as u64 + 4);
        let mut key = vec![0; key_length as usize];
        self.cursor.read_exact(&mut key).unwrap();
        key
    }

    pub fn get_val(&mut self, idx: u16) -> Vec<u8> {
        assert!(idx < self.num_keys());

        // Position of the value
        let pos = self.kv_pos(idx);
        self.cursor.set_position(pos as u64);

        // Length of the key
        let key_length = self.cursor.read_u16::<LittleEndian>().unwrap();
        // Length of the value
        let val_length = self.cursor.read_u16::<LittleEndian>().unwrap();

        self.cursor.set_position(pos as u64 + 4 + key_length as u64);
        let mut val = vec![0; val_length as usize];
        self.cursor.read_exact(&mut val).unwrap();
        val
    }


    // node size in bytes
    pub fn num_bytes(&mut self) -> u16 {
        let num_keys = self.num_keys();
        self.kv_pos(num_keys)
    }

    // returns the first kid node whose range intersects the key. (kid[i] <= key)
    // TODO: bisect
    pub fn node_lookup_le(&mut self, key: &Vec<u8>) -> u16 {
        let mut found = 0;
        // the first key is a copy from the parent node,
        // thus it's always less than or equal to the key.
        for i in 1..self.num_keys() {
            let cmp = self.get_key(i).cmp(key);
            if cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal {
                found = i;
            }
            if cmp == std::cmp::Ordering::Greater {
                break;
            }
        }
        found
    }

    /** Add a new key to a leaf node. Returns a double sized node which needs to be dealt with */
    pub fn leaf_insert(mut self, idx : u16, key : &Vec<u8>, val : &Vec<u8>) -> BNode {
        let old_num_keys = self.num_keys();

        let mut new_node = BNode::new_with_size(BNodeType::LEAF, old_num_keys + 1, 2 * BTREE_PAGE_SIZE);
        new_node.node_append_range(&mut self, 0, 0, idx);
        new_node.node_append_kv(idx, 0, key, val);
        new_node.node_append_range(&mut self, idx + 1, idx,  old_num_keys-idx);

        new_node
    }

    /** Update a key in a leaf node. Returns a double sized node which needs to be dealt with */
    pub fn leaf_update(mut self, idx : u16, key : &Vec<u8>, val : &Vec<u8>) -> BNode {
        let old_num_keys = self.num_keys();

        let mut new_node = BNode::new_with_size(BNodeType::LEAF, old_num_keys, 2 * BTREE_PAGE_SIZE);
        new_node.node_append_range(&mut self, 0, 0, idx);
        new_node.node_append_kv(idx, 0, key, val);
        new_node.node_append_range(&mut self, idx + 1, idx + 1,  old_num_keys-idx-1);

        new_node
    }


    pub fn leaf_delete(mut self, idx : u16) -> BNode {
        let old_num_keys = self.num_keys();

        let mut new_node = BNode::new(BNodeType::LEAF, old_num_keys - 1);
        new_node.node_append_range(&mut self, 0, 0, idx);
        new_node.node_append_range(&mut self, idx, idx + 1, old_num_keys - idx - 1);

        new_node
    }

    pub fn node_merge(mut self, mut right : BNode) -> BNode {
        let left_num_keys = self.num_keys();
        let right_num_keys = right.num_keys();
        let new_num_keys = left_num_keys + right_num_keys;

        let mut new_node = BNode::new(self.b_type(), new_num_keys);
        new_node.node_append_range(&mut self, 0, 0, left_num_keys);
        new_node.node_append_range(&mut right, left_num_keys, 0, right_num_keys);

        new_node
    }

    pub fn node_replace_2_kid(mut self, idx : u16, ptr: u64, key : &Vec<u8>) -> BNode {
        let old_num_keys = self.num_keys();
        let mut new_node = BNode::new(BNodeType::NODE, old_num_keys - 1);
        
        new_node.node_append_range(&mut self, 0, 0, idx);
        new_node.node_append_kv(idx, ptr, key, &vec![]);
        new_node.node_append_range(&mut self, idx + 1, idx + 2, old_num_keys - idx - 2);

        new_node
    }

    pub fn node_append_range(&mut self, old : &mut BNode, dst_new : u16, src_old : u16, n : u16) {
        assert!(src_old + n <= old.num_keys());
        assert!(dst_new + n <= self.num_keys());
        if n == 0 {
            return;
        }

        // pointers
        for i in 0..n {
            self.set_ptr(dst_new + i, old.get_ptr(src_old + i));
        }
        // offsets
        let dst_begin = self.get_offset(dst_new);
        let src_begin = old.get_offset(src_old);
        for i in 1..=n { // NOTE: the range is [1, n]
            let offset = dst_begin + old.get_offset(src_old + i) - src_begin;
            self.set_offset(dst_new + i, offset);
        }

        // KVs
        let begin = old.kv_pos(src_old);
        let end = old.kv_pos(src_old + n);
        let mut buf = vec![0; (end - begin) as usize];
        old.cursor.set_position(begin as u64);
        old.cursor.read_exact(&mut buf).unwrap();
        let kv_pos = self.kv_pos(dst_new) as u64;
        self.cursor.set_position(kv_pos);
        self.cursor.write_all(&buf).unwrap();
    }

    // copy a KV into the position
    pub fn node_append_kv(&mut self, idx: u16, ptr : u64, key: &Vec<u8>, val : &Vec<u8>) {
        // ptrs
        self.set_ptr(idx, ptr);

        // KVs
        let pos = self.kv_pos(idx);
        self.cursor.set_position(pos as u64);
        // kLen and vLen
        self.cursor.write_u16::<LittleEndian>(key.len() as u16).unwrap();
        self.cursor.write_u16::<LittleEndian>(val.len() as u16).unwrap();
        // key and value
        self.cursor.write_all(key).unwrap();
        self.cursor.write_all(val).unwrap();

        // the offset of the next key
        let idx_offset = self.get_offset(idx);
        self.set_offset(idx+1, idx_offset+4+(key.len() + val.len()) as u16);
    }

    // split a bigger-than-allowed node into two.
    // the second node always fits on a page.
    pub fn split2(mut self, left_size : usize) -> (BNode, BNode) {
        assert!(self.num_keys() >= 2);

        // initial guess for the split point
        let mut n_left = self.num_keys() / 2;

        fn calc_num_left_bytes(old_node : &mut BNode, n_left : u16) -> usize {
            HEADER as usize + 10 * n_left as usize + old_node.get_offset(n_left) as usize
        }

        while calc_num_left_bytes(&mut self, n_left) > BTREE_PAGE_SIZE {
            n_left -= 1;
        };
        assert!(n_left >= 1);

        // try to fit the right half
        fn calc_num_right_bytes(old_node: &mut BNode, n_left : u16) -> usize {
            let num_left_bytes = calc_num_left_bytes(old_node, n_left);
            old_node.num_bytes() as usize - num_left_bytes + HEADER as usize
        }

        while calc_num_right_bytes(&mut self, n_left) > BTREE_PAGE_SIZE {
            n_left += 1;
        };
        assert!(n_left < self.num_keys());
        let n_right = self.num_keys() - n_left;

        // Create new nodes
        let mut left_node = BNode::new_with_size(self.b_type(), n_left, left_size); // Might be split later
        left_node.node_append_range(&mut self, 0, 0, n_left);

        let mut right_node = BNode::new_with_size(self.b_type(), n_right, BTREE_PAGE_SIZE); 
        right_node.node_append_range(&mut self, 0, n_left, n_right);

        // Make sure right side is not too big. Left may still be too big
        assert!(right_node.num_bytes() <= BTREE_PAGE_SIZE as u16);
        (left_node, right_node)
    }

    /** split a node if it's too big. the results are 1~3 correctly sized nodes */
    pub fn split3(mut self) -> (u16, Vec<BNode>) {
        if self.num_bytes() <= BTREE_PAGE_SIZE as u16 {
            self.resize_buffer(BTREE_PAGE_SIZE);
            return (1, vec!{self});
        };

        let (mut left_node, right_node) = self.split2(2 * BTREE_PAGE_SIZE);
        if left_node.num_bytes() <= BTREE_PAGE_SIZE as u16 {
            left_node.resize_buffer(BTREE_PAGE_SIZE);
            return (2, vec![left_node, right_node]);
        };

        let (mut left_left_node, middle_node) = left_node.split2(BTREE_PAGE_SIZE);
        assert!(left_left_node.num_bytes() <= BTREE_PAGE_SIZE as u16);
        (3, vec![left_left_node, middle_node, right_node])
    }

    pub fn resize_buffer(&mut self, new_size: usize) {
        // Create a new buffer with the desired size and initialize with zeros
        let mut new_buffer = vec![0; new_size];

        // Copy the data from the old buffer to the new buffer
        let old_buffer = self.cursor.get_ref();

        new_buffer.copy_from_slice(&old_buffer[..new_size]);

        // Update the cursor to use the new buffer
        self.cursor = Cursor::new(new_buffer);
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_node() {
        const MAX_SIZE : u32 = HEADER as u32 + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE as u32 + BTREE_MAX_VAL_SIZE as u32;
        assert!(MAX_SIZE <= BTREE_PAGE_SIZE as u32);
    }

    #[test]
    fn test_bnode_creation() {
        let mut bnode = BNode::new(BNodeType::LEAF, 10);
        assert_eq!(bnode.b_type(), BNodeType::LEAF);
        assert_eq!(bnode.num_keys(), 10);
    }

    #[test]
    fn test_bnode_get_set_ptr() {
        let mut bnode = BNode::new(BNodeType::NODE, 10);
        for i in 0..10 {
            bnode.set_ptr(i, i as u64);
        }
        for i in 0..10 {
            assert_eq!(bnode.get_ptr(i), i as u64);
        }
    }

    #[test]
    #[should_panic(expected = "assertion failed: idx < self.num_keys()")]
    fn test_bnode_get_ptr_out_of_bounds() {
        let mut bnode = BNode::new(BNodeType::NODE, 10);
        bnode.get_ptr(10); // This should panic
    }

    #[test]
    #[should_panic(expected = "assertion failed: idx < self.num_keys()")]
    fn test_bnode_set_ptr_out_of_bounds() {
        let mut bnode = BNode::new(BNodeType::NODE, 10);
        bnode.set_ptr(10, 10); // This should panic
    }

    #[test]
    fn test_bnode_get_set_offset() {
        let mut bnode = BNode::new(BNodeType::NODE, 10);
        for i in 1..10 {
            bnode.set_offset(i, i as u16);
        }
        assert_eq!(bnode.get_offset(0), 0);
        for i in 1..10 {
            assert_eq!(bnode.get_offset(i), i as u16);
        }
    }

    #[test]
    fn test_bnode_offset_pos() {
        let mut bnode = BNode::new(BNodeType::NODE, 10);
        for i in 1..=10 {
            assert_eq!(bnode.offset_pos(i), (HEADER as u16) + 8 * 10 + 2 * (i - 1));
        }
    }


    #[test]
    fn test_bnode_kv_pos() {
        let mut bnode = BNode::new(BNodeType::NODE, 3);

        // Assuming variable-sized keys and values
        let key_size = 4; // Adjust this based on your actual key size
        let val_size = 6; // Adjust this based on your actual value size

        for i in 0..3 {
            let key = vec![i as u8; key_size];
            let val = vec![i as u8; val_size];
            bnode.node_append_kv(i, 0, &key, &val);
            let expected_pos = HEADER + 8 * 3 + 2 * 3 + (4 + key_size as u16 + val_size as u16) * i;
            assert_eq!(bnode.kv_pos(i), expected_pos);
        }
    }


    #[test]
    fn test_bnode_get_key() {
        let mut bnode = BNode::new(BNodeType::NODE, 3);

        // Assuming the keys are 4 bytes each
        for i in 0..3 {
            let key = vec![i as u8; 4];
            bnode.node_append_kv(i, 0, &key, &vec![]);
        }

        for i in 0..3 {
            let key = vec![i as u8; 4];
            assert_eq!(bnode.get_key(i), key);
        }
    }

    #[test]
    fn test_bnode_get_val() {
        let mut bnode = BNode::new(BNodeType::NODE, 3);

        // Assuming the values are 6 bytes each
        for i in 0..3 {
            let val = vec![i as u8; 6];
            bnode.node_append_kv(i, 0, &vec![], &val);
            assert_eq!(bnode.get_val(i), val);
        }
    }

    #[test]
    fn test_bnode_nbytes() {
        let mut bnode = BNode::new(BNodeType::NODE, 5);

        // Assuming the keys and values are 4 bytes each
        assert_eq!(bnode.num_bytes(), HEADER as u16 + 8 * 5 + 2 * 5 + 0);
    }

    #[test]
    fn test_bnode_node_lookup_le() {
        let mut bnode = BNode::new(BNodeType::NODE, 5);

        // Assuming the keys are [0, 1, 2, 3, 4]
        for i in 0..5 {
            let key = vec![i as u8];
            bnode.node_append_kv(i, 0, &key, &vec![]);
        }

        assert_eq!(bnode.node_lookup_le(&vec![2]), 2);
        assert_eq!(bnode.node_lookup_le(&vec![5]), 4);
        assert_eq!(bnode.node_lookup_le(&vec![0]), 0);
        assert_eq!(bnode.node_lookup_le(&vec![6]), 4);
    }

    #[test]
    fn test_bnode_leaf_insert() {
        // Assuming the keys and values are 4 bytes each
        let mut old_bnode = BNode::new(BNodeType::NODE, 3);
        for i in 0..3 {
            let key = vec![i as u8; 4];
            let val = vec![i as u8; 4];
            old_bnode.node_append_kv(i, 0, &key, &val);
        }

        let new_key = vec![4u8; 4];
        let new_val = vec![4u8; 4];
        let mut bnode = old_bnode.leaf_insert( 1, &new_key, &new_val);

        assert_eq!(bnode.num_keys(), 4);
        assert_eq!(bnode.get_key(0), vec![0u8; 4]);
        assert_eq!(bnode.get_key(1), vec![4u8; 4]);
        assert_eq!(bnode.get_key(2), vec![1u8; 4]);
        assert_eq!(bnode.get_key(3), vec![2u8; 4]);
        assert_eq!(bnode.get_val(1), vec![4u8; 4]);
    }
}
