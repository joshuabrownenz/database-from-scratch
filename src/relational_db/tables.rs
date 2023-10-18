use super::{records::Record, value::Value};
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, Serialize, Deserialize)]
pub struct TableDef {
    // User Defined
    pub name: String,
    pub types: Vec<u32>,
    pub columns: Vec<String>,
    pub primary_keys: i64,
    // Auto-assigned B-tree key prefixes for different tables
    pub prefix: u32,
}

impl TableDef {
    // reorder a record and check for missing columns.
    // n == tdef.PKeys: record is exactly a primary key
    // n == len(tdef.Cols): record contains all columns
    pub fn check_record(&self, record: &Record) -> Result<Vec<Value>, String> {
        panic!("Not implemented")
    }

    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn from_json(json: String) -> TableDef {
        serde_json::from_str(json.as_str()).unwrap()
    }
}
