extern crate byteorder;

use std::io::{Cursor, Write, Read};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

const HEADER: u16 = 4;

const BTREE_PAGE_SIZE: u32 = 4096;
const BTREE_MAX_KEY_SIZE: u32 = 1000;
const BTREE_MAX_VAL_SIZE: u32 = 3000;


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

pub struct BNode {
    pub cursor: Cursor<[u8; 4096]>,
} 

impl BNode {
    pub fn new() -> BNode {
        BNode {
            cursor: Cursor::new([0; 4096]),
        }
    }

    // Header
    pub fn set_header(&mut self, btype: BNodeType, nkeys: u16) {
        self.cursor.set_position(0);
        self.cursor.write_u16::<LittleEndian>(btype.value()).unwrap();
        self.cursor.write_u16::<LittleEndian>(nkeys).unwrap();
    }

    pub fn btype(&self) -> BNodeType {
        let mut rdr = self.cursor.clone();
        rdr.set_position(0);

        match rdr.read_u16::<LittleEndian>().unwrap() {
            1 => BNodeType::NODE,
            2 => BNodeType::LEAF,
            n => panic!("Invalid BNode type {}", n),
        }
    }

    pub fn nkeys(&self) -> u16 {
        let mut rdr = self.cursor.clone();
        rdr.set_position(2);

        rdr.read_u16::<LittleEndian>().unwrap()
    }

    // Page Pointers
    pub fn getPtr(& self, idx: u16) -> u64 {
        assert!(idx < self.nkeys());
        let mut rdr = self.cursor.clone();
        let pos = HEADER + 8 * idx;
        rdr.set_position(pos as u64);
        rdr.read_u64::<LittleEndian>().unwrap()
    }

    pub fn setPtr(&mut self, idx : u16, val : u64) {
        assert!(idx < self.nkeys());
        let pos : u64 = HEADER as u64 + 8 * idx as u64;
        self.cursor.set_position(pos);
        self.cursor.write_u64::<LittleEndian>(val).unwrap();
    }

    // Offset List
    pub fn offsetPos(&self, idx : u16) -> u16 {
        assert!(1 <= idx && idx <= self.nkeys());
         HEADER as u16 + 8 * self.nkeys() as u16 + 2 * (idx - 1)
    }

    pub fn getOffset(&self, idx : u16) -> u16 {
        if idx == 0 {
            return 0;
        }
        let mut rdr = self.cursor.clone();
        let pos = self.offsetPos(idx);
        rdr.set_position(pos as u64);
        rdr.read_u16::<LittleEndian>().unwrap()
    }

    pub fn setOffset(&mut self, idx : u16, val : u16) {
        assert!(idx <= self.nkeys());
        let pos = self.offsetPos(idx);
        self.cursor.set_position(pos as u64);
        self.cursor.write_u16::<LittleEndian>(val).unwrap();
    }

    // key-values
    pub fn kvPos(&self, idx : u16) -> u16 {
        assert!(idx <= self.nkeys());
        HEADER as u16 + 8 * self.nkeys() + 2 * self.nkeys() + self.getOffset(idx)
    }

    pub fn getKey(&self, idx: u16) -> Vec<u8> {
        assert!(idx < self.nkeys());
        let mut rdr = self.cursor.clone();

        // Position of the key
        let pos = self.kvPos(idx);
        rdr.set_position(pos as u64);

        // Length of the key
        let key_length = rdr.read_u16::<LittleEndian>().unwrap();
        rdr.set_position(pos as u64 + 4);
        let mut key = vec![0; key_length as usize];
        rdr.read_exact(&mut key).unwrap();
        key
    }

    pub fn getVal(&self, idx: u16) -> Vec<u8> {
        assert!(idx < self.nkeys());
        let mut rdr = self.cursor.clone();

        // Position of the value
        let pos = self.kvPos(idx);
        rdr.set_position(pos as u64);

        // Length of the key
        let key_length = rdr.read_u16::<LittleEndian>().unwrap();
        // Length of the value
        let val_length = rdr.read_u16::<LittleEndian>().unwrap();

        rdr.set_position(pos as u64 + 4 + key_length as u64);
        let mut val = vec![0; val_length as usize];
        rdr.read_exact(&mut val).unwrap();
        val
    }


    // node size in bytes
    pub fn nbytes(&self) -> u16 {
        self.kvPos(self.nkeys())
    }

    // returns the first kid node whose range intersects the key. (kid[i] <= key)
    // TODO: bisect
    pub fn node_lookup_le(&self, key: &Vec<u8>) -> u16 {
        let mut found = 0;
        // the first key is a copy from the parent node,
        // thus it's always less than or equal to the key.
        for i in 1..self.nkeys() {
            let cmp = self.getKey(i).cmp(key);
            if cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal {
                found = i;
            }
            if cmp == std::cmp::Ordering::Greater {
                break;
            }
        }
        found
    }

    // add a new key to a leaf node
    pub fn leaf_insert(&mut self, old : &BNode, idx : u16, key : &Vec<u8>, val : &Vec<u8>) {
        self.set_header(BNodeType::LEAF, old.nkeys() + 1);
        self.node_append_range(old, 0, 0, idx);
        self.node_append_kv(idx, 0, key, val);
        self.node_append_range(old, idx + 1, idx, old.nkeys() - idx);
    }

    pub fn node_append_range(&mut self, old : &BNode, dst_new : u16, src_old : u16, n : u16) {
        assert!(src_old + n <= old.nkeys());
        assert!(dst_new + n <= self.nkeys());
        if n == 0 {
            return;
        }

        // pointers
        for i in 0..n {
            self.setPtr(dst_new + i, old.getPtr(src_old + i));
        }
        // offsets
        let dst_begin = self.getOffset(dst_new);
        let src_begin = old.getOffset(src_old);
        for i in 1..=n { // NOTE: the range is [1, n]
            let offset = dst_begin + old.getOffset(src_old + i) - src_begin;
            self.setOffset(dst_new + i, offset);
        }

        // KVs
        let begin = old.kvPos(src_old);
        let end = old.kvPos(src_old + n);
        let mut buf = vec![0; (end - begin) as usize];
        let mut rdr = old.cursor.clone();
        rdr.set_position(begin as u64);
        rdr.read_exact(&mut buf).unwrap();
        self.cursor.set_position(self.kvPos(dst_new) as u64);
        self.cursor.write_all(&buf).unwrap();
    }

    // copy a KV into the position

    pub fn node_append_kv(&mut self, idx: u16, ptr : u64, key: &Vec<u8>, val : &Vec<u8>) {
        // ptrs
        self.setPtr(idx, ptr);

        // KVs
        let pos = self.kvPos(idx);
        self.cursor.set_position(pos as u64);
        // kLen and vLen
        self.cursor.write_u16::<LittleEndian>(key.len() as u16).unwrap();
        self.cursor.write_u16::<LittleEndian>(val.len() as u16).unwrap();
        // key and value
        self.cursor.write_all(&key).unwrap();
        self.cursor.write_all(&val).unwrap();

        // the offset of the next key
        self.setOffset(idx+1, self.getOffset(idx)+4+(key.len() + val.len()) as u16);
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_node() {
        const maxSize : u32 = HEADER as u32 + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE;
        assert!(maxSize <= BTREE_PAGE_SIZE);
    }

    #[test]
    fn test_node_values() {
        assert_eq!(BNodeType::NODE.value(), 1);
        assert_eq!(BNodeType::LEAF.value(), 2);
    }

    #[test]
    fn test_bnode_type_value() {
        assert_eq!(BNodeType::NODE.value(), 1);
        assert_eq!(BNodeType::LEAF.value(), 2);
    }

    #[test]
    fn test_bnode_creation() {
        let bnode = BNode::new();
        // Assuming the initial values should be 0
        assert_eq!(bnode.cursor.get_ref()[0], 0);
        assert_eq!(bnode.cursor.get_ref()[1], 0);
    }

    #[test]
    fn test_bnode_set_header() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::LEAF, 10);
        assert_eq!(bnode.btype(), BNodeType::LEAF);
        assert_eq!(bnode.nkeys(), 10);

        bnode.set_header(BNodeType::NODE, 20);
        assert_eq!(bnode.btype(), BNodeType::NODE);
        assert_eq!(bnode.nkeys(), 20);
    }

    #[test]
    #[should_panic(expected = "Invalid BNode type")]
    fn test_invalid_bnode_type() {
        let mut bnode = BNode::new();
        // Setting an invalid value for btype
        bnode.cursor.get_mut()[0] = 0xFF;
        bnode.cursor.get_mut()[1] = 0xFF;
        bnode.btype(); // This should panic
    }

    #[test]
    fn test_bnode_get_set_ptr() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 10);
        for i in 0..10 {
            bnode.setPtr(i, i as u64);
        }
        for i in 0..10 {
            assert_eq!(bnode.getPtr(i), i as u64);
        }
    }

    #[test]
    #[should_panic(expected = "assertion failed: idx < self.nkeys()")]
    fn test_bnode_get_ptr_out_of_bounds() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 10);
        bnode.getPtr(10); // This should panic
    }

    #[test]
    #[should_panic(expected = "assertion failed: idx < self.nkeys()")]
    fn test_bnode_set_ptr_out_of_bounds() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 10);
        bnode.setPtr(10, 10); // This should panic
    }

    #[test]
    fn test_bnode_get_set_offset() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 10);
        for i in 1..10 {
            bnode.setOffset(i, i as u16);
        }
        assert_eq!(bnode.getOffset(0), 0);
        for i in 1..10 {
            assert_eq!(bnode.getOffset(i), i as u16);
        }
    }

    #[test]
    fn test_bnode_offset_pos() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 10);
        for i in 1..=10 {
            assert_eq!(bnode.offsetPos(i), (HEADER as u16) + 8 * 10 + 2 * (i - 1));
        }
    }


    #[test]
    fn test_bnode_kv_pos() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 3);

        // Assuming variable-sized keys and values
        let key_size = 4; // Adjust this based on your actual key size
        let val_size = 6; // Adjust this based on your actual value size

        for i in 0..3 {
            let key = vec![i as u8; key_size];
            let val = vec![i as u8; val_size];
            bnode.node_append_kv(i, 0, &key, &val);
            let expected_pos = HEADER + 8 * 3 + 2 * 3 + (4 + key_size as u16 + val_size as u16) * i;
            assert_eq!(bnode.kvPos(i), expected_pos);
        }
    }


    #[test]
    fn test_bnode_get_key() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 3);

        // Assuming the keys are 4 bytes each
        for i in 0..3 {
            let key = vec![i as u8; 4];
            bnode.node_append_kv(i, 0, &key, &vec![]);
        }

        for i in 0..3 {
            let key = vec![i as u8; 4];
            assert_eq!(bnode.getKey(i), key);
        }
    }

    #[test]
    fn test_bnode_get_val() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 3);

        // Assuming the values are 6 bytes each
        for i in 0..3 {
            let val = vec![i as u8; 6];
            bnode.node_append_kv(i, 0, &vec![], &val);
            assert_eq!(bnode.getVal(i), val);
        }
    }

    #[test]
    fn test_bnode_nbytes() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 5);

        // Assuming the keys and values are 4 bytes each
        assert_eq!(bnode.nbytes(), HEADER as u16 + 8 * 5 + 2 * 5 + 0);
    }

    #[test]
    fn test_bnode_node_lookup_le() {
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 5);

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
        let mut bnode = BNode::new();
        bnode.set_header(BNodeType::NODE, 3);

        // Assuming the keys and values are 4 bytes each
        let mut old_bnode = BNode::new();
        old_bnode.set_header(BNodeType::NODE, 3);
        for i in 0..3 {
            let key = vec![i as u8; 4];
            let val = vec![i as u8; 4];
            old_bnode.node_append_kv(i, 0, &key, &val);
        }

        let new_key = vec![4u8; 4];
        let new_val = vec![4u8; 4];
        bnode.leaf_insert(&old_bnode, 1, &new_key, &new_val);

        assert_eq!(bnode.nkeys(), 4);
        assert_eq!(bnode.getKey(0), vec![0u8; 4]);
        assert_eq!(bnode.getKey(1), vec![4u8; 4]);
        assert_eq!(bnode.getKey(2), vec![1u8; 4]);
        assert_eq!(bnode.getKey(3), vec![2u8; 4]);
        assert_eq!(bnode.getVal(1), vec![4u8; 4]);
    }
}
