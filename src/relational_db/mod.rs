use std::collections::HashMap;

use crate::kv_store::KV;

pub mod records;
pub mod tables;
pub mod value;
use byteorder::{ByteOrder, LittleEndian};
use records::Record;

use self::{tables::TableDef, value::Value};

lazy_static! {
    pub static ref TABLE_DEF_META: TableDef = TableDef {
        prefix: 1,
        name: "@meta".to_string(),
        types: vec![Value::BYTES_TYPE, Value::INT64_TYPE],
        columns: vec!["key".to_string(), "val".to_string()],
        primary_keys: 1,
    };
    pub static ref TABLE_DEF_TABLE: TableDef = TableDef {
        prefix: 2,
        name: "@table".to_string(),
        types: vec![Value::BYTES_TYPE, Value::BYTES_TYPE],
        columns: vec!["name".to_string(), "def".to_string()],
        primary_keys: 1,
    };
}

struct DB {
    path: String,
    // internals
    kv: KV,
    tables: HashMap<String, TableDef>,
}

impl DB {
    pub fn get(&mut self, table: &String, record: &mut Record) -> Result<bool, String> {
        match self.get_table_def(table) {
            Some(table_def) => self.db_get(&table_def, record),
            None => Err(format!("Table not found {}", table)),
        }
    }

    /// Retrieve value from kv store itself
    fn db_get(&self, table_def: &TableDef, record: &mut Record) -> Result<bool, String> {
        let mut values: Vec<Value> = table_def.check_record(record, table_def.primary_keys)?;

        let key: Vec<u8> = DB::encode_key(
            None,
            table_def.prefix,
            &values[..table_def.primary_keys as usize],
        );
        let value_raw = self.kv.get(&key);
        if value_raw.is_none() {
            return Ok(false);
        }
        let value_raw = value_raw.unwrap();

        for i in table_def.primary_keys as usize..table_def.columns.len() {
            values[i] = Value::u32_to_empty_value(table_def.types[i]);
        }
        DB::decode_values(&value_raw, &mut values[table_def.primary_keys as usize..]);
        record.columns.extend(
            table_def.columns[table_def.primary_keys as usize..]
                .iter()
                .cloned(),
        );
        record
            .values
            .extend(values[table_def.primary_keys as usize..].iter().cloned());
        Ok(true)
    }

    pub fn encode_key(out: Option<Vec<u8>>, prefix: u32, values: &[Value]) -> Vec<u8> {
        let mut out = out.unwrap_or(vec![]);
        let mut buf: [u8; 4] = [0; 4];
        LittleEndian::write_u32(&mut buf, prefix);
        out.extend(buf);
        let out = DB::encode_values(out, values);
        out
    }

    fn decode_values(in_bytes: &Vec<u8>, values_out: &mut [Value]) {
        let mut pos = 0;
        for i in 0..values_out.len() {
            match values_out[i] {
                Value::Int64(_) => {
                    let mut buf: [u8; 8] = [0; 8];
                    buf.copy_from_slice(&in_bytes[pos..8]);
                    let i64 = LittleEndian::read_i64(&buf);
                    values_out[i] = Value::Int64(Some(i64));
                    pos += 8;
                }
                Value::Bytes(_) => {
                    let idx = in_bytes[pos..].iter().position(|&x| x == 0).unwrap();
                    let bytes = Value::unescape_string(&in_bytes[pos..pos + idx].to_vec());
                    values_out[i] = Value::Bytes(Some(bytes));
                    pos += idx + 1;
                }
                Value::Error => {
                    panic!("Error decoding value")
                }
            }
        }
        assert!(pos == in_bytes.len());
    }

    fn encode_values(mut out: Vec<u8>, values: &[Value]) -> Vec<u8> {
        for value in values {
            match value {
                Value::Int64(i) => {
                    let mut buf: [u8; 8] = [0; 8];
                    LittleEndian::write_i64(&mut buf, i.unwrap());
                    out.extend(buf);
                }
                Value::Bytes(b) => {
                    out.extend(Value::escape_string(b.as_ref().unwrap()));
                    out.extend(0..=0); // null-terminated
                }
                Value::Error => {
                    panic!("Error encoding value")
                }
            }
        }
        out
    }

    // Table definition stuff
    fn get_table_def(&mut self, table: &String) -> Option<TableDef> {
        match self.tables.get(table) {
            None => {
                let table_def = self.get_table_def_db(&table);
                if table_def.is_some() {
                    self.tables
                        .insert(table.clone(), table_def.clone().unwrap());
                }
                table_def
            }
            Some(table_def) => Some(table_def.to_owned()),
        }
    }

    fn get_table_def_db(&self, table: &String) -> Option<TableDef> {
        let mut record = Record::new();
        record.add_bytes("name".to_string(), table.as_bytes().to_vec());

        let get_result = self.db_get(&TABLE_DEF_META, &mut record);
        if get_result.is_err() {
            return None;
        }

        Some(TableDef::from_json(
            record
                .get(&"Def".to_string())
                .unwrap()
                .bytes_to_string()
                .unwrap(),
        ))
    }
}
