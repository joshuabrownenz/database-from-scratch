use crate::{
    b_tree::{btree_iter::BTreeIterator, CmpOption},
    free_list::{cloneable::RcRWLockBTreePageManager, FreeList},
};

use super::{records::Record, tables::TableDef};

pub struct Scanner {
    pub compare_1: CmpOption,
    pub compare_2: CmpOption,
    pub key_1: Record,
    pub key_2: Record,
    // Internal
    table_def: TableDef,
    iter: Option<BTreeIterator<RcRWLockBTreePageManager<FreeList>>>,
    key_end: Vec<u8>,
}

impl Scanner {
    pub fn valid(&self) -> bool {
        // self.iter.valid()
        panic!("Not implemented")
    }

    pub fn next(&mut self) {
        // self.iter.next();
        panic!("Not implemented")
    }

    pub fn deref(&self) -> Record {
        panic!("Not implemented")
    }

    pub fn set_table_def(&mut self, table_def: TableDef) {
        self.table_def = table_def;
    }

    pub fn set_key_end(&mut self, key_end: Vec<u8>) {
        self.key_end = key_end;
    }

    pub fn set_iter(&mut self, iter: BTreeIterator<RcRWLockBTreePageManager<FreeList>>) {
        self.iter = Some(iter);
    }
}
