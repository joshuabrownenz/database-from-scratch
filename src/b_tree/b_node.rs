extern crate byteorder;
use byteorder::{ByteOrder, LittleEndian};
use std::vec::Vec;

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

pub const U16_SIZE: usize = 2;
pub const U64_SIZE: usize = 8;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NodeType {
    Node = 1,
    Leaf = 2,
}

impl NodeType {
    pub fn value(&self) -> u16 {
        *self as u16
    }
}

pub trait Node {
    fn from(slice: &[u8]) -> Self;
    // fn get_data(self) -> [u8; BTREE_PAGE_SIZE];
}

impl Node for BNode {
    fn from(slice: &[u8]) -> Self {
        BNode::from(slice)
    }
}

pub struct BNode {
    data: [u8; 2 * BTREE_PAGE_SIZE],
    actual_size: usize,
}

impl BNode {
    pub fn new(b_type: NodeType, num_keys: u16) -> Self {
        let mut new_node = BNode {
            data: [0; 2 * BTREE_PAGE_SIZE],
            actual_size: BTREE_PAGE_SIZE,
        };
        new_node.set_header(b_type, num_keys);
        new_node
    }
    pub fn new_with_size(b_type: NodeType, num_keys: u16, size: usize) -> BNode {
        let mut new_node = BNode {
            data: [0; 2 * BTREE_PAGE_SIZE],
            actual_size: size,
        };
        new_node.set_header(b_type, num_keys);
        new_node
    }

    /** Creates a BNode from a slice. Slice must be of length BTREE_PAGE_SLICE */
    pub fn from(data_in: &[u8]) -> BNode {
        assert!(data_in.len() == BTREE_PAGE_SIZE);
        let mut data = [0; 2 * BTREE_PAGE_SIZE];
        data[..BTREE_PAGE_SIZE].copy_from_slice(data_in);
        let new_node = BNode {
            data,
            actual_size: BTREE_PAGE_SIZE,
        };
        // Makes sure not is of valid type
        new_node.b_type();
        new_node
    }

    pub fn get_data(self) -> [u8; BTREE_PAGE_SIZE] {
        assert!(self.actual_size == BTREE_PAGE_SIZE);
        self.data[..BTREE_PAGE_SIZE].try_into().unwrap()
    }

    // Header
    fn set_header(&mut self, b_type: NodeType, num_keys: u16) {
        LittleEndian::write_u16(&mut self.data[..2], b_type.value());
        LittleEndian::write_u16(&mut self.data[2..4], num_keys);
    }

    pub fn b_type(&self) -> NodeType {
        match LittleEndian::read_u16(&self.data[..2]) {
            1 => NodeType::Node,
            2 => NodeType::Leaf,
            n => panic!("Invalid BNode type {}", n),
        }
    }

    pub fn num_keys(&self) -> u16 {
        LittleEndian::read_u16(&self.data[2..4])
    }

    // Page Pointers
    pub fn get_ptr(&self, idx: u16) -> u64 {
        assert!(idx < self.num_keys());
        let pos: usize = HEADER as usize + 8 * idx as usize;
        LittleEndian::read_u64(&self.data[pos..pos + U64_SIZE])
    }

    fn set_ptr(&mut self, idx: u16, val: u64) {
        assert!(idx < self.num_keys());
        let pos: usize = HEADER as usize + 8 * idx as usize;
        LittleEndian::write_u64(&mut self.data[pos..pos + U64_SIZE], val)
    }

    // Offset List
    pub fn offset_pos(&self, idx: u16) -> u16 {
        assert!(1 <= idx && idx <= self.num_keys());
        HEADER + 8 * self.num_keys() + 2 * (idx - 1)
    }

    pub fn get_offset(&self, idx: u16) -> u16 {
        if idx == 0 {
            return 0;
        }
        let pos: usize = self.offset_pos(idx) as usize;
        LittleEndian::read_u16(&self.data[pos..pos + U16_SIZE])
    }

    fn set_offset(&mut self, idx: u16, val: u16) {
        assert!(idx <= self.num_keys());
        let pos: usize = self.offset_pos(idx) as usize;
        LittleEndian::write_u16(&mut self.data[pos..pos + U16_SIZE], val)
    }

    // key-values
    pub fn kv_pos(&self, idx: u16) -> u16 {
        let num_keys: u16 = self.num_keys();
        assert!(idx <= num_keys);
        HEADER + 10 * num_keys + self.get_offset(idx)
    }

    pub fn get_key(&self, idx: u16) -> Vec<u8> {
        assert!(idx < self.num_keys());

        // Position of the key
        let pos: usize = self.kv_pos(idx) as usize;

        // Length of the key
        let key_length = LittleEndian::read_u16(&self.data[pos..pos + U16_SIZE]);

        let key_pos = pos + 4;
        self.data[key_pos..key_pos + key_length as usize].to_vec()
    }

    pub fn get_val(&self, idx: u16) -> Vec<u8> {
        assert!(idx < self.num_keys());

        // Position of the start of the kv block
        let pos: usize = self.kv_pos(idx) as usize;

        // Length of the key
        let key_length = LittleEndian::read_u16(&self.data[pos..pos + U16_SIZE]);
        // Length of the value
        let val_length_pos = pos + U16_SIZE;
        let val_length =
            LittleEndian::read_u16(&self.data[val_length_pos..val_length_pos + U16_SIZE]);

        let val_pos = pos + 2 * U16_SIZE + key_length as usize;
        self.data[val_pos..val_pos + val_length as usize].to_vec()
    }

    // node size in bytes
    pub fn num_bytes(&self) -> u16 {
        let num_keys = self.num_keys();
        let num_bytes = self.kv_pos(num_keys);
        assert!(num_bytes <= self.actual_size as u16);
        num_bytes
    }

    // returns the first kid node whose range intersects the key. (kid[i] <= key)
    // TODO: bisect
    pub fn node_lookup_le(&self, key: &Vec<u8>) -> u16 {
        let mut low: u16 = 1;
        let mut high: u16 = self.num_keys() - 1;
        let mut found: u16 = 0;

        while low <= high {
            let mid = (low + high) / 2;
            let cmp = self.get_key(mid).cmp(key);

            match cmp {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                    found = mid;
                    low = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    high = mid.saturating_sub(1);
                }
            }
        }
        found
    }

    /** Add a new key to a leaf node. Returns a double sized node which needs to be dealt with */
    pub fn leaf_insert(self, idx: u16, key: &Vec<u8>, val: &Vec<u8>) -> BNode {
        let old_num_keys = self.num_keys();

        let mut new_node =
            BNode::new_with_size(NodeType::Leaf, old_num_keys + 1, 2 * BTREE_PAGE_SIZE);
        new_node.node_append_range(&self, 0, 0, idx);
        new_node.node_append_kv(idx, 0, key, val);
        new_node.node_append_range(&self, idx + 1, idx, old_num_keys - idx);

        new_node
    }

    /** Update a key in a leaf node. Returns a double sized node which needs to be dealt with */
    pub fn leaf_update(self, idx: u16, key: &Vec<u8>, val: &Vec<u8>) -> BNode {
        let old_num_keys = self.num_keys();

        let mut new_node = BNode::new_with_size(NodeType::Leaf, old_num_keys, 2 * BTREE_PAGE_SIZE);
        new_node.node_append_range(&self, 0, 0, idx);
        new_node.node_append_kv(idx, 0, key, val);
        new_node.node_append_range(&self, idx + 1, idx + 1, old_num_keys - idx - 1);

        new_node
    }

    pub fn leaf_delete(self, idx: u16) -> BNode {
        let old_num_keys = self.num_keys();

        let mut new_node = BNode::new(NodeType::Leaf, old_num_keys - 1);
        new_node.node_append_range(&self, 0, 0, idx);
        new_node.node_append_range(&self, idx, idx + 1, old_num_keys - idx - 1);

        new_node
    }

    pub fn node_merge(self, right: BNode) -> BNode {
        let left_num_keys = self.num_keys();
        let right_num_keys = right.num_keys();
        let new_num_keys = left_num_keys + right_num_keys;

        let mut new_node = BNode::new(self.b_type(), new_num_keys);
        new_node.node_append_range(&self, 0, 0, left_num_keys);
        new_node.node_append_range(&right, left_num_keys, 0, right_num_keys);

        new_node
    }

    pub fn node_replace_2_kid(self, idx: u16, ptr: u64, key: &Vec<u8>) -> BNode {
        let old_num_keys = self.num_keys();
        let mut new_node = BNode::new(NodeType::Node, old_num_keys - 1);

        new_node.node_append_range(&self, 0, 0, idx);
        new_node.node_append_kv(idx, ptr, key, &vec![]);
        new_node.node_append_range(&self, idx + 1, idx + 2, old_num_keys - idx - 2);

        new_node
    }

    pub fn node_append_range(&mut self, old: &BNode, dst_new: u16, src_old: u16, n: u16) {
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
        for i in 1..=n {
            // NOTE: the range is [1, n]
            let offset = dst_begin + old.get_offset(src_old + i) - src_begin;
            self.set_offset(dst_new + i, offset);
        }

        // KVs
        // Copy from old
        let begin = old.kv_pos(src_old);
        let end = old.kv_pos(src_old + n);
        let buf = &old.data[begin as usize..end as usize];

        // Insert into new
        let kv_pos: usize = self.kv_pos(dst_new) as usize;
        let buf_len = end - begin;
        self.data[kv_pos..kv_pos + buf_len as usize].copy_from_slice(buf);
    }

    // copy a KV into the position
    pub fn node_append_kv(&mut self, idx: u16, ptr: u64, key: &Vec<u8>, val: &Vec<u8>) {
        // ptrs
        self.set_ptr(idx, ptr);

        // KVs
        let pos: usize = self.kv_pos(idx) as usize;
        LittleEndian::write_u16(&mut self.data[pos..pos + U16_SIZE], key.len() as u16);
        let vlen_pos = pos + U16_SIZE;
        LittleEndian::write_u16(
            &mut self.data[vlen_pos..vlen_pos + U16_SIZE],
            val.len() as u16,
        );
        let key_pos = pos + 2 * U16_SIZE;
        self.data[key_pos..key_pos + key.len()].copy_from_slice(key);
        let val_pos = key_pos + key.len();
        self.data[val_pos..val_pos + val.len()].copy_from_slice(val);

        // the offset of the next key
        let idx_offset = self.get_offset(idx);
        self.set_offset(idx + 1, idx_offset + 4 + (key.len() + val.len()) as u16);
    }

    // split a bigger-than-allowed node into two.
    // the second node always fits on a page.
    pub fn split2(self, left_size: usize) -> (BNode, BNode) {
        assert!(self.num_keys() >= 2);

        // initial guess for the split point
        let mut n_left = self.num_keys() / 2;

        fn calc_num_left_bytes(old_node: &BNode, n_left: u16) -> usize {
            HEADER as usize + 10 * n_left as usize + old_node.get_offset(n_left) as usize
        }

        while calc_num_left_bytes(&self, n_left) > BTREE_PAGE_SIZE {
            n_left -= 1;
        }
        assert!(n_left >= 1);

        // try to fit the right half
        fn calc_num_right_bytes(old_node: &BNode, n_left: u16) -> usize {
            let num_left_bytes = calc_num_left_bytes(old_node, n_left);
            old_node.num_bytes() as usize - num_left_bytes + HEADER as usize
        }

        while calc_num_right_bytes(&self, n_left) > BTREE_PAGE_SIZE {
            n_left += 1;
        }
        assert!(n_left < self.num_keys());
        let n_right = self.num_keys() - n_left;

        // Create new nodes
        let mut left_node = BNode::new_with_size(self.b_type(), n_left, left_size); // Might be split later
        left_node.node_append_range(&self, 0, 0, n_left);

        let mut right_node = BNode::new_with_size(self.b_type(), n_right, BTREE_PAGE_SIZE);
        right_node.node_append_range(&self, 0, n_left, n_right);

        // Make sure right side is not too big. Left may still be too big
        assert!(right_node.num_bytes() <= BTREE_PAGE_SIZE as u16);
        (left_node, right_node)
    }

    /** split a node if it's too big. the results are 1~3 correctly sized nodes */
    pub fn split3(mut self) -> (u16, Vec<BNode>) {
        if self.num_bytes() <= BTREE_PAGE_SIZE as u16 {
            self.resize_buffer(BTREE_PAGE_SIZE);
            return (1, vec![self]);
        };

        let (mut left_node, right_node) = self.split2(2 * BTREE_PAGE_SIZE);
        if left_node.num_bytes() <= BTREE_PAGE_SIZE as u16 {
            left_node.resize_buffer(BTREE_PAGE_SIZE);
            return (2, vec![left_node, right_node]);
        };

        let (left_left_node, middle_node) = left_node.split2(BTREE_PAGE_SIZE);
        assert!(left_left_node.num_bytes() <= BTREE_PAGE_SIZE as u16);
        (3, vec![left_left_node, middle_node, right_node])
    }

    pub fn resize_buffer(&mut self, new_size: usize) {
        // Create a new buffer with the desired size and initialize with zeros
        self.actual_size = new_size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Clone for BNode {
        fn clone(&self) -> Self {
            let mut data = [0; 2 * BTREE_PAGE_SIZE];
            data[..BTREE_PAGE_SIZE].copy_from_slice(&self.data[..BTREE_PAGE_SIZE]);
            BNode {
                data,
                actual_size: BTREE_PAGE_SIZE,
            }
        }
    }

    #[test]
    fn test_size_of_node() {
        const MAX_SIZE: u32 =
            HEADER as u32 + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE as u32 + BTREE_MAX_VAL_SIZE as u32;
        assert!(MAX_SIZE <= BTREE_PAGE_SIZE as u32);
    }

    #[test]
    fn test_bnode_creation() {
        let bnode = BNode::new(NodeType::Leaf, 10);
        assert_eq!(bnode.b_type(), NodeType::Leaf);
        assert_eq!(bnode.num_keys(), 10);
    }

    #[test]
    fn test_bnode_get_set_ptr() {
        let mut bnode = BNode::new(NodeType::Node, 10);
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
        let bnode = BNode::new(NodeType::Node, 10);
        bnode.get_ptr(10); // This should panic
    }

    #[test]
    #[should_panic(expected = "Invalid BNode type 20")]
    fn test_bnode_invalid_type() {
        let mut bnode = BNode::new(NodeType::Node, 10);
        bnode.data[0] = 20; // This should panic
        BNode::from(&bnode.get_data());
    }

    #[test]
    #[should_panic(expected = "assertion failed: idx < self.num_keys()")]
    fn test_bnode_set_ptr_out_of_bounds() {
        let mut bnode = BNode::new(NodeType::Node, 10);
        bnode.set_ptr(10, 10); // This should panic
    }

    #[test]
    fn test_bnode_get_set_offset() {
        let mut bnode = BNode::new(NodeType::Node, 10);
        for i in 1..10 {
            bnode.set_offset(i, i);
        }
        assert_eq!(bnode.get_offset(0), 0);
        for i in 1..10 {
            assert_eq!(bnode.get_offset(i), i);
        }
    }

    #[test]
    fn test_bnode_offset_pos() {
        let bnode = BNode::new(NodeType::Node, 10);
        for i in 1..=10 {
            assert_eq!(bnode.offset_pos(i), HEADER + 8 * 10 + 2 * (i - 1));
        }
    }

    #[test]
    fn test_bnode_kv_pos() {
        let mut bnode = BNode::new(NodeType::Node, 3);

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
        let mut bnode = BNode::new(NodeType::Node, 3);

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
        let mut bnode = BNode::new(NodeType::Node, 3);

        // Assuming the values are 6 bytes each
        for i in 0..3 {
            let val = vec![i as u8; 6];
            bnode.node_append_kv(i, 0, &vec![], &val);
            assert_eq!(bnode.get_val(i), val);
        }
    }

    #[test]
    fn test_bnode_nbytes() {
        let bnode = BNode::new(NodeType::Node, 5);

        // Assuming the keys and values are 4 bytes each
        assert_eq!(bnode.num_bytes(), HEADER + 10 * 5);
    }

    #[test]
    fn test_bnode_node_lookup_le() {
        let mut bnode = BNode::new(NodeType::Node, 5);

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
        let mut old_bnode = BNode::new(NodeType::Node, 3);
        for i in 0..3 {
            let key = vec![i as u8; 4];
            let val = vec![i as u8; 4];
            old_bnode.node_append_kv(i, 0, &key, &val);
        }

        let new_key = vec![4u8; 4];
        let new_val = vec![4u8; 4];
        let bnode = old_bnode.leaf_insert(1, &new_key, &new_val);

        assert_eq!(bnode.num_keys(), 4);
        assert_eq!(bnode.get_key(0), vec![0u8; 4]);
        assert_eq!(bnode.get_key(1), vec![4u8; 4]);
        assert_eq!(bnode.get_key(2), vec![1u8; 4]);
        assert_eq!(bnode.get_key(3), vec![2u8; 4]);
        assert_eq!(bnode.get_val(1), vec![4u8; 4]);
    }
}
