extern crate byteorder;
use std::fmt::Debug;

use byteorder::{ByteOrder, LittleEndian};

use crate::b_tree::b_node::{Node, BTREE_PAGE_SIZE};

// node format:
// | type | size | total | next |  pointers  |
// |  2B  |  2B  |  8B   |  8B  |  size * 8B |

pub const FL_NODE_TYPE: u16 = 3;
pub const FL_HEADER: u16 = 4 + 8 + 8;
pub const MAX_FREE_LIST_IN_PAGE: usize = (BTREE_PAGE_SIZE - FL_HEADER as usize) / 8;

pub const U64_SIZE: usize = 8;

impl Node for FLNode {
    fn from(slice: &[u8]) -> Self {
        FLNode::from(slice)
    }
}

pub struct FLNode {
    data: [u8; BTREE_PAGE_SIZE],
}

impl FLNode {
    pub fn new(size: u16, next: u64) -> Self {
        let mut new_node = FLNode {
            data: [0; BTREE_PAGE_SIZE],
        };
        new_node.set_header(size, next);
        new_node
    }

    /** Creates a FLNode from a slice. Slice must be of length BTREE_PAGE_SLICE */
    pub fn from(data_in: &[u8]) -> Self {
        assert!(data_in.len() == BTREE_PAGE_SIZE);
        let data: [u8; 4096] = data_in.try_into().unwrap();
        let new_node = FLNode { data };
        // Makes sure not is of valid type
        assert!(LittleEndian::read_u16(&new_node.data[..2]) == FL_NODE_TYPE);
        new_node
    }

    pub fn get_data(self) -> [u8; BTREE_PAGE_SIZE] {
        self.data[..BTREE_PAGE_SIZE].try_into().unwrap()
    }

    pub fn size(&self) -> u16 {
        LittleEndian::read_u16(&self.data[2..4])
    }

    pub fn total(&self) -> u64 {
        LittleEndian::read_u64(&self.data[4..4 + U64_SIZE])
    }

    pub fn next(&self) -> u64 {
        LittleEndian::read_u64(&self.data[12..12 + U64_SIZE])
    }

    // Header
    fn set_header(&mut self, size: u16, next: u64) {
        LittleEndian::write_u16(&mut self.data[..2], FL_NODE_TYPE);
        LittleEndian::write_u16(&mut self.data[2..4], size);
        LittleEndian::write_u64(&mut self.data[12..12 + U64_SIZE], next);
    }

    pub fn set_total(data: &mut [u8], total: u64) {
        LittleEndian::write_u64(&mut data[4..4 + U64_SIZE], total);
    }

    // Page Pointers
    pub fn get_ptr(&self, idx: u16) -> u64 {
        assert!(idx < self.size());
        let pos: usize = FL_HEADER as usize + U64_SIZE * idx as usize;
        LittleEndian::read_u64(&self.data[pos..pos + U64_SIZE])
    }

    pub fn set_ptr(&mut self, idx: u16, ptr: u64) {
        assert!(idx < self.size());
        let pos: usize = FL_HEADER as usize + U64_SIZE * idx as usize;
        LittleEndian::write_u64(&mut self.data[pos..pos + U64_SIZE], ptr)
    }
}

impl Debug for FLNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let size: u16 = self.size();
        let total: u64 = self.total();
        let next: u64 = self.next();
        let mut ptrs: Vec<u64> = Vec::new();
        for i in 0..size {
            ptrs.push(self.get_ptr(i));
        }
        write!(
            f,
            "FLNode {{ size: {}, total: {}, next: {}, num_ptrs: {} }}",
            size,
            total,
            next,
            ptrs.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let node = FLNode::new(10, 20);
        assert_eq!(node.size(), 10);
        assert_eq!(node.total(), 0);
        assert_eq!(node.next(), 20);
    }

    #[test]
    fn test_from() {
        let mut data = [0; BTREE_PAGE_SIZE];
        LittleEndian::write_u16(&mut data[..2], FL_NODE_TYPE);
        LittleEndian::write_u16(&mut data[2..4], 10);
        LittleEndian::write_u64(&mut data[12..12 + U64_SIZE], 20);
        let node = FLNode::from(&data);
        assert_eq!(node.size(), 10);
        assert_eq!(node.total(), 0);
        assert_eq!(node.next(), 20);
    }

    #[test]
    #[should_panic]
    fn test_from_invalid_type() {
        let mut data = [0; BTREE_PAGE_SIZE];
        LittleEndian::write_u16(&mut data[..2], 0);
        FLNode::from(&data);
    }

    #[test]
    #[should_panic]
    fn test_from_invalid_length() {
        let data = [0; BTREE_PAGE_SIZE - 1];
        FLNode::from(&data);
    }

    #[test]
    fn test_get_data() {
        let node = FLNode::new(10, 20);
        let data = node.get_data();
        assert_eq!(data.len(), BTREE_PAGE_SIZE);
    }

    #[test]
    fn test_set_total() {
        let mut data = [0; BTREE_PAGE_SIZE];
        FLNode::set_total(&mut data, 10);
        assert_eq!(LittleEndian::read_u64(&data[4..4 + U64_SIZE]), 10);
    }

    #[test]
    fn test_get_ptr() {
        let mut node = FLNode::new(2, 0);
        node.set_ptr(0, 10);
        node.set_ptr(1, 20);
        assert_eq!(node.get_ptr(0), 10);
        assert_eq!(node.get_ptr(1), 20);
    }

    #[test]
    #[should_panic]
    fn test_get_ptr_out_of_bounds() {
        let node = FLNode::new(2, 0);
        node.get_ptr(2);
    }

    #[test]
    fn test_set_ptr() {
        let mut node = FLNode::new(2, 0);
        node.set_ptr(0, 10);
        node.set_ptr(1, 20);
        assert_eq!(node.get_ptr(0), 10);
        assert_eq!(node.get_ptr(1), 20);
    }

    #[test]
    #[should_panic]
    fn test_set_ptr_out_of_bounds() {
        let mut node = FLNode::new(2, 0);
        node.set_ptr(2, 10);
    }
}
