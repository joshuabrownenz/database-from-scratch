use std::collections::HashMap;

use crate::kv_store::KV;

pub mod records;
pub mod tables;
use records::{Record, ValueType};

use self::tables::TableDef;

const TABLE_DEF_META: TableDef = TableDef {
    prefix: 1,
    name: "@meta".to_string(),
    types: vec![ValueType::Bytes.value(), ValueType::Int64.value()],
    columns: vec!["key".to_string(), "val".to_string()],
    primary_keys: 1,
};

const TABLE_DEF_TABLE: TableDef = TableDef {
    prefix: 2,
    name: "@table".to_string(),
    types: vec![ValueType::Bytes.value(), ValueType::Bytes.value()],
    columns: vec!["name".to_string(), "def".to_string()],
    primary_keys: 1,
};

struct DB {
    path: String,
    // internals
    kv: KV,
    tables: HashMap<String, TableDef>,
}

// impl DB {
//     pub fn get(&self, table_def : TableDef, record : Record) -> Result<bool, _> {
        
//     }
// }