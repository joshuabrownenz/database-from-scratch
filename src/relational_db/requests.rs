use crate::b_tree::BTree;

pub enum InsertMode {
    MODE_UPSERT,      // insert or replace
    MODE_UPDATE_ONLY, // update existing keys
    MODE_INSERT_ONLY, // only add new keys
}

pub struct InsertRequest<'a> {
    tree: &'a mut BTree,
    // out
    added: bool, // added a new key
    // in
    key: Vec<u8>,
    val: Vec<u8>,
    mode: InsertMode,
}
