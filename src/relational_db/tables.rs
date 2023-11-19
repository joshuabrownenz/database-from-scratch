use super::{records::Record, value::Value};
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, Serialize, Deserialize)]
pub struct TableDef {
    // User Defined
    pub name: String,
    pub types: Vec<u32>,
    pub columns: Vec<String>,
    pub primary_keys: usize,
    // Auto-assigned B-tree key prefixes for different tables
    pub prefix: u32,
}

impl TableDef {
    // reorder a record and check for missing columns.
    // n == tdef.PKeys: record is exactly a primary key
    // n == len(tdef.Cols): record contains all columns
    pub fn check_record(&self, record: &Record, n: usize) -> Result<Vec<Value>, String> {
        let values: Vec<Value> = self.reorder_record(record)?;
        self.values_complete(&values, n)?;
        Ok(values)
    }

    fn reorder_record(&self, record: &Record) -> Result<Vec<Value>, String> {
        assert!(record.columns.len() == record.values.len());
        let mut out: Vec<Value> = Vec::with_capacity(self.columns.len());
        for (i, c) in self.columns.iter().enumerate() {
            let v: Option<&Value> = record.get(c);
            if v.is_none() {
                continue; // leave this column uninitialized
            }
            if v.unwrap().type_as_u32() != self.types[i] {
                return Err(format!("bad column type: {}", self.columns[i]));
            }
            out.push(v.unwrap().clone());
        }
        Ok(out)
    }

    fn values_complete(&self, values: &Vec<Value>, n: usize) -> Result<(), String> {
        for (i, v) in values.iter().enumerate() {
            if i < n && v.type_as_u32() == 0 {
                return Err(format!("missing column: {}", self.columns[i]));
            } else if i >= n && v.type_as_u32() != 0 {
                return Err(format!("extra column: {}", self.columns[i]));
            }
        }
        Ok(())
    }

    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn from_json(json: String) -> TableDef {
        serde_json::from_str(json.as_str()).unwrap()
    }
}
