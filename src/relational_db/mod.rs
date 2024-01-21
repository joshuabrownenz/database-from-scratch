use std::collections::HashMap;

use crate::prelude::*;
use crate::{b_tree::InsertMode, kv_store::KV};

pub mod records;
pub mod tables;
pub mod value;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use records::Record;

use self::{tables::TableDef, value::Value};

lazy_static! {
    pub static ref TABLE_DEF_META: TableDef = TableDef {
        prefix: 1,
        name: "@meta".to_string(),
        types: vec![Value::BYTES_TYPE, Value::BYTES_TYPE],
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
    pub static ref INTERNAL_TABLES: HashMap<String, TableDef> = {
        let mut m = HashMap::new();
        m.insert(TABLE_DEF_META.name.clone(), TABLE_DEF_META.clone());
        m.insert(TABLE_DEF_TABLE.name.clone(), TABLE_DEF_TABLE.clone());
        m
    };
}

const TABLE_PREFIX_MIN: u32 = 100;

pub struct DB {
    path: String,
    // internals
    kv: KV,
    tables: HashMap<String, TableDef>,
}

impl DB {
    pub fn get(&mut self, table: &str, record: &mut Record) -> Result<bool> {
        match self.get_table_def(table) {
            Some(table_def) => self.db_get(&table_def, record),
            None => Err(Error::Generic(format!("Table not found {}", table))),
        }
    }

    /// Retrieve value from kv store itself
    /// TODO: Don't return bool, return Record (make Record immutable)
    fn db_get(&self, table_def: &TableDef, record: &mut Record) -> Result<bool> {
        let mut values: Vec<Value> = table_def.check_record(record, table_def.primary_keys)?;

        let key: Vec<u8> =
            DB::encode_key(None, table_def.prefix, &values[..table_def.primary_keys]);
        let value_raw = self.kv.get(&key);
        if value_raw.is_none() {
            return Ok(false);
        }
        let value_raw = value_raw.unwrap();

        (table_def.primary_keys..table_def.columns.len()).for_each(|i| {
            values[i] = Value::u32_to_empty_value(table_def.types[i]);
        });

        DB::decode_values(&value_raw, &mut values[table_def.primary_keys..]);
        record
            .columns
            .extend(table_def.columns[table_def.primary_keys..].iter().cloned());
        record
            .values
            .extend(values[table_def.primary_keys..].iter().cloned());
        Ok(true)
    }

    pub fn db_update(
        &mut self,
        table_def: &TableDef,
        record: &Record,
        mode: InsertMode,
    ) -> Result<bool> {
        let values: Vec<Value> = table_def.check_record(record, table_def.columns.len())?;

        let key = DB::encode_key(
            None,
            table_def.prefix,
            values[..table_def.primary_keys].as_ref(),
        );

        let value = DB::encode_values(None, &values[table_def.primary_keys..]);
        self.kv.update(&key, &value, mode)
    }

    fn set(&mut self, table: &str, record: Record, mode: InsertMode) -> Result<bool> {
        match self.get_table_def(table) {
            Some(table_def) => self.db_update(&table_def, &record, mode),
            None => Err(Error::Generic(format!("Table not found {}", table))),
        }
    }

    pub fn insert(&mut self, table: &str, record: Record) -> Result<bool> {
        self.set(table, record, InsertMode::InsertOnly)
    }

    pub fn update(&mut self, table: &str, record: Record) -> Result<bool> {
        self.set(table, record, InsertMode::UpdateOnly)
    }

    pub fn upsert(&mut self, table: &str, record: Record) -> Result<bool> {
        self.set(table, record, InsertMode::Upsert)
    }

    fn db_delete(&mut self, table_def: &TableDef, record: Record) -> Result<bool> {
        let values: Vec<Value> = table_def.check_record(&record, table_def.primary_keys)?;

        let key = DB::encode_key(
            None,
            table_def.prefix,
            values[..table_def.primary_keys].as_ref(),
        );

        self.kv.del(&key)
    }

    pub fn delete(&mut self, table: &str, record: Record) -> Result<bool> {
        match self.get_table_def(table) {
            Some(table_def) => self.db_delete(&table_def, record),
            None => Err(Error::Generic(format!("Table not found {}", table))),
        }
    }

    pub fn encode_key(out: Option<Vec<u8>>, prefix: u32, values: &[Value]) -> Vec<u8> {
        let mut out = out.unwrap_or_default();
        let mut buf: [u8; 4] = [0; 4];
        LittleEndian::write_u32(&mut buf, prefix);
        out.extend(buf);
        DB::encode_values(Some(out), values)
    }

    fn decode_values(in_bytes: &Vec<u8>, values_out: &mut [Value]) {
        let mut pos = 0;
        for value in values_out.iter_mut() {
            match value {
                Value::Int64(_) => {
                    let mut buf: [u8; 8] = [0; 8];
                    buf.copy_from_slice(&in_bytes[pos..pos + 8]);
                    let i64 = BigEndian::read_i64(&buf);
                    *value = Value::Int64(Some(i64));
                    pos += 8;
                }
                Value::Bytes(_) => {
                    let end_offset = in_bytes[pos..].iter().position(|&x| x == 0).unwrap();
                    let bytes = Value::unescape_string(&in_bytes[pos..pos + end_offset]);
                    *value = Value::Bytes(Some(bytes));
                    pos += end_offset + 1;
                }
                Value::Error => {
                    panic!("Error decoding value")
                }
            }
        }
        assert!(pos == in_bytes.len());
    }

    fn encode_values(out: Option<Vec<u8>>, values: &[Value]) -> Vec<u8> {
        let mut out = out.unwrap_or_default();
        for value in values {
            match value {
                Value::Int64(i) => {
                    let mut buf: [u8; 8] = [0; 8];
                    BigEndian::write_i64(&mut buf, i.unwrap());
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

    /** Checks if the table definition is loaded in the DB, if it is not in memory then it trys to pull the table from storage  */
    fn get_table_def(&mut self, table: &str) -> Option<TableDef> {
        // Expose internal tables
        if INTERNAL_TABLES.contains_key(table) {
            return Some(INTERNAL_TABLES.get(table).unwrap().clone());
        };

        match self.tables.get(table) {
            None => {
                let table_def = self.get_table_def_db(table);
                if table_def.is_some() {
                    self.tables
                        .insert(table.to_string(), table_def.clone().unwrap());
                }
                table_def
            }
            Some(table_def) => Some(table_def.to_owned()),
        }
    }

    fn get_table_def_db(&self, table: &str) -> Option<TableDef> {
        let mut record = Record::new();
        record.add_bytes("name".to_string(), table.as_bytes().to_vec());

        let get_result = self.db_get(&TABLE_DEF_TABLE, &mut record);
        if get_result.is_err() {
            return None;
        }

        Some(TableDef::from_json(
            record.get("def").unwrap().bytes_to_string().unwrap(),
        ))
    }

    /** Adds a new table to the DB */
    pub fn table_new(&mut self, mut table_def: TableDef) -> Result<()> {
        table_def.check()?;

        // check the existing table
        let mut table = Record::new();
        table.add_bytes("name".to_string(), table_def.name.as_bytes().to_vec());

        let table_exists = self.db_get(&TABLE_DEF_TABLE, &mut table)?;
        if table_exists {
            return Err(Error::Generic(format!("table exists: {}", table_def.name)));
        }

        // allocate the next prefix
        assert!(table_def.prefix == 0);
        table_def.prefix = TABLE_PREFIX_MIN;
        let mut meta = Record::new();
        meta.add_bytes("key".to_string(), "next_prefix".as_bytes().to_vec());

        let ok = self.db_get(&TABLE_DEF_META, &mut meta)?;
        if ok {
            if let Value::Bytes(value) = meta.get("val").unwrap() {
                table_def.prefix = LittleEndian::read_u32(value.as_ref().unwrap());
            } else {
                return Err(Error::Static("bad meta `val`"));
            };

            assert!(table_def.prefix > TABLE_PREFIX_MIN);
        } else {
            meta.add_bytes("val".to_string(), vec![0; 4]);
        }

        // update the next prefix
        let mut next_prefix = vec![0; 4];
        LittleEndian::write_u32(&mut next_prefix, table_def.prefix + 1);
        meta.set_bytes("val".to_string(), next_prefix);
        self.db_update(&TABLE_DEF_META, &meta, InsertMode::Upsert)?;

        // Store the definition
        let definition = table_def.to_json()?;

        table.add_bytes("def".to_string(), definition.as_bytes().to_vec());
        self.db_update(&TABLE_DEF_TABLE, &table, InsertMode::Upsert)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{ByteOrder, LittleEndian};
    use std::fs;

    use crate::{b_tree::InsertMode, kv_store::KV, relational_db::value::Value};

    use super::{records::Record, tables::TableDef, DB, TABLE_DEF_META};
    use std::collections::HashMap;

    struct R {
        db: DB,
        reference: HashMap<String, Vec<Record>>,
    }

    impl R {
        fn new(path: &str, delete_old: bool) -> R {
            fs::create_dir_all("test_run_dir").unwrap();
            let file_name = format!("test_run_dir/{}", path);
            if delete_old {
                fs::remove_file(&file_name).unwrap_or(());
            }

            let db = DB {
                path: file_name.clone(),
                kv: KV::open(file_name).unwrap(),
                tables: HashMap::new(),
            };
            let reference = HashMap::new();

            R { db, reference }
        }

        pub fn create(&mut self, table_def: TableDef) {
            let result = self.db.table_new(table_def);
            assert!(result.is_ok());
        }

        fn find_ref(&self, table: &str, record: &Record) -> Option<usize> {
            let pkeys = self.db.tables[table].primary_keys;
            let empty = vec![];
            let records = self.reference.get(table).unwrap_or(&empty);
            let mut found = None;
            for (i, record_at_i) in records.iter().enumerate() {
                if record_at_i.values[..pkeys] == record.values[..pkeys] {
                    assert!(found.is_none());
                    found = Some(i);
                }
            }
            found
        }

        fn add(&mut self, table: &str, record: Record) -> bool {
            let added = self.db.upsert(table, record.clone());
            assert!(added.is_ok());
            let added = added.unwrap();

            let idx = self.find_ref(table, &record);
            if !added {
                assert!(idx.is_some());
                self.reference.get_mut(table).unwrap()[idx.unwrap()] = record;
            } else {
                assert!(idx.is_none());
                if !self.reference.contains_key(table) {
                    self.reference.insert(table.to_string(), vec![]);
                }
                self.reference.get_mut(table).unwrap().push(record);
            };

            added
        }

        fn get(&mut self, table: &str, record: &mut Record) -> bool {
            let ok = self.db.get(table, record);
            assert!(ok.is_ok());
            let ok = ok.unwrap();

            let idx = self.find_ref(table, record);
            if ok {
                assert!(idx.is_some());
                assert!(self.reference.get(table).unwrap()[idx.unwrap()] == *record);
            } else {
                assert!(idx.is_none());
            }

            ok
        }

        fn del(&mut self, table: &str, record: Record) -> bool {
            let deleted = self.db.delete(table, record.clone());
            assert!(deleted.is_ok());
            let deleted = deleted.unwrap();

            let idx = self.find_ref(table, &record);
            if deleted {
                assert!(idx.is_some());
                let records = self.reference.get_mut(table).unwrap();
                records.remove(idx.unwrap());
            } else {
                assert!(idx.is_none());
            }

            deleted
        }

        // func (r *R) del(table string, rec Record) bool {
        // 	deleted, err := r.db.Delete(table, rec)
        // 	assert(err == nil)

        // 	idx := r.findRef(table, rec)
        // 	if deleted {
        // 		assert(idx >= 0)
        // 		records := r.ref[table]
        // 		copy(records[idx:], records[idx+1:])
        // 		r.ref[table] = records[:len(records)-1]
        // 	} else {
        // 		assert(idx == -1)
        // 	}

        // 	return deleted
        // }
    }

    #[test]
    fn test_set_meta() {
        let mut rdb = R::new("rdb_next_prefix.db", true);

        let mut meta = Record::new();
        meta.add_bytes("key".to_string(), "test_key".as_bytes().to_vec());

        // Confirm doesn't already exist. (It shouldn't in a brand new db)
        let ok = rdb.db.db_get(&TABLE_DEF_META, &mut meta);
        assert!(ok.is_ok());
        assert!(!ok.unwrap());

        // Write the value into test_key
        meta.add_bytes("val".to_string(), vec![5; 4]);
        let ok = rdb.db.db_update(&TABLE_DEF_META, &meta, InsertMode::Upsert);
        assert!(ok.is_ok());
        assert!(ok.unwrap());

        // Read written value
        let mut test_meta = Record::new();
        test_meta.add_bytes("key".to_string(), "test_key".as_bytes().to_vec());
        let ok = rdb.db.db_get(&TABLE_DEF_META, &mut test_meta);
        assert!(ok.is_ok());
        assert!(ok.unwrap());
        assert_eq!(
            meta.get("key").unwrap().bytes_to_string().unwrap(),
            "test_key".to_string()
        );
        assert!(meta.get("val").unwrap().bytes().cmp(&vec![5; 4]).is_eq());
    }

    #[test]
    fn test_encode_decode() {
        let values: Vec<Value> = vec![Value::Int64(Some(123)), Value::Bytes(Some(vec![1, 2, 3]))];

        let encoded = DB::encode_values(None, &values);
        let mut decoded: Vec<Value> = vec![Value::Int64(None), Value::Bytes(None)];
        DB::decode_values(&encoded, &mut decoded);

        assert_eq!(values, decoded);
    }

    #[test]
    fn test_table_create() {
        let mut r = R::new("rdb_test.db", true);
        let table_def = TableDef {
            name: "tbl_test".to_string(),
            columns: vec![
                "ki1".to_string(),
                "ks2".to_string(),
                "s1".to_string(),
                "i2".to_string(),
            ],
            types: vec![2, 1, 1, 2],
            primary_keys: 2,
            prefix: 0,
        };
        r.create(table_def);

        let table_def = TableDef {
            name: "tbl_test2".to_string(),
            columns: vec!["ki1".to_string(), "ks2".to_string()],
            types: vec![2, 1],
            primary_keys: 2,
            prefix: 0,
        };
        r.create(table_def);

        {
            let mut rec = Record::new();
            rec.add_bytes("key".to_string(), "next_prefix".as_bytes().to_vec());
            let ok = r.db.get("@meta", &mut rec).unwrap();
            assert!(ok);
            let mut correct_next_prefix = vec![0; 4];
            LittleEndian::write_u32(&mut correct_next_prefix, 102);
            assert_eq!(rec.get("val").unwrap().bytes(), &correct_next_prefix);
        }
        {
            let mut rec = Record::new();
            rec.add_bytes("name".to_string(), "tbl_test".as_bytes().to_vec());
            let ok = r.db.get("@table", &mut rec).unwrap();
            assert!(ok);
            let expected = r#"{"name":"tbl_test","types":[2,1,1,2],"columns":["ki1","ks2","s1","i2"],"primary_keys":2,"prefix":100}"#;
            assert_eq!(rec.get("def").unwrap().bytes_to_string().unwrap(), expected);
        }
    }

    #[test]
    fn test_table_basic() {
        let mut r = R::new("test_table_basic.db", true);

        let table_def = TableDef {
            name: "tbl_test".to_string(),
            columns: vec![
                "ki1".to_string(),
                "ks2".to_string(),
                "s1".to_string(),
                "i2".to_string(),
            ],
            types: vec![2, 1, 1, 2],
            primary_keys: 2,
            prefix: 0,
        };
        r.create(table_def);

        let mut rec = Record::new();
        rec.add_int64("ki1".to_string(), 1)
            .add_bytes("ks2".to_string(), "hello".as_bytes().to_vec());
        rec.add_bytes("s1".to_string(), "world".as_bytes().to_vec())
            .add_int64("i2".to_string(), 2);
        let added = r.add("tbl_test", rec.clone());
        assert!(added);

        {
            let mut rec = Record::new();
            rec.add_int64("ki1".to_string(), 1)
                .add_bytes("ks2".to_string(), "hello".as_bytes().to_vec());
            let ok = r.get("tbl_test", &mut rec);
            assert!(ok);
            assert_eq!(
                rec.get("s1").unwrap().bytes_to_string().unwrap(),
                "world".to_string()
            );
            assert_eq!(rec.get("i2").unwrap().get_int64().unwrap().unwrap(), 2);
        }

        {
            let mut rec = Record::new();
            rec.add_int64("ki1".to_string(), 1)
                .add_bytes("ks2".to_string(), "hello2".as_bytes().to_vec());
            let ok = r.get("tbl_test", &mut rec);
            assert!(!ok);
        }

        rec.set_bytes("s1".to_string(), "www".as_bytes().to_vec());
        let added = r.add("tbl_test", rec.clone());
        assert!(!added);

        {
            let mut rec = Record::new();
            rec.add_int64("ki1".to_string(), 1)
                .add_bytes("ks2".to_string(), "hello".as_bytes().to_vec());
            let ok = r.get("tbl_test", &mut rec);
            assert!(ok);
        }

        {
            let mut key = Record::new();
            key.add_int64("ki1".to_string(), 1)
                .add_bytes("ks2".to_string(), "hello2".as_bytes().to_vec());
            let deleted = r.del("tbl_test", key.clone());
            assert!(!deleted);

            key.set_bytes("ks2".to_string(), "hello".as_bytes().to_vec());
            let deleted = r.del("tbl_test", key);
            assert!(deleted);
        }
    }

    // func TestTableEncoding(t *testing.T) {
    // 	input := []int{-1, 0, +1, math.MinInt64, math.MaxInt64}
    // 	sort.Ints(input)

    // 	encoded := []string{}
    // 	for _, i := range input {
    // 		v := Value{Type: TYPE_INT64, I64: int64(i)}
    // 		b := encodeValues(nil, []Value{v})
    // 		out := []Value{v}
    // 		decodeValues(b, out)
    // 		assert(out[0].I64 == int64(i))
    // 		encoded = append(encoded, string(b))
    // 	}

    // 	is.True(t, sort.StringsAreSorted(encoded))
    // }
}
