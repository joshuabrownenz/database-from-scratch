use crate::b_tree::BTree;

#[derive(PartialEq)]
pub enum InsertMode {
    ModeUpsert,     // insert or replace
    ModeUpdateOnly, // update existing keys
    ModeInsertOnly, // only add new keys
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
            mode: InsertMode::ModeUpsert,
            added: false,
        }
    }
    pub fn mode(mut self, mode: InsertMode) -> InsertRequest {
        self.mode = mode;
        self
    }
}
