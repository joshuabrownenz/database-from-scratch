use std::io::{self, Error, ErrorKind};

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
    pub fn check_record(&self, record: &Record, n: usize) -> io::Result<Vec<Value>> {
        let values: Vec<Value> = self.reorder_record(record)?;
        self.values_complete(&values, n)?;
        Ok(values)
    }

    // rearrange a record to the defined column order
    // func reorderRecord(tdef *TableDef, rec Record) ([]Value, error) {
    // 	assert(len(rec.Cols) == len(rec.Vals))
    // 	out := make([]Value, len(tdef.Cols))
    // 	for i, c := range tdef.Cols {
    // 		v := rec.Get(c)
    // 		if v == nil {
    // 			continue // leave this column uninitialized
    // 		}
    // 		if v.Type != tdef.Types[i] {
    // 			return nil, fmt.Errorf("bad column type: %s", c)
    // 		}
    // 		out[i] = *v
    // 	}
    // 	return out, nil
    // }
    fn reorder_record(&self, record: &Record) -> io::Result<Vec<Value>> {
        assert!(record.columns.len() == record.values.len());
        let mut out: Vec<Value> = vec![Value::Error; self.columns.len()];
        for (i, c) in self.columns.iter().enumerate() {
            let v: Option<&Value> = record.get(c);
            if v.is_none() {
                continue; // leave this column uninitialized
            }
            if v.unwrap().type_as_u32() != self.types[i] {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("bad column type: {}", self.columns[i]),
                ));
            }
            out[i] = v.unwrap().clone();
        }
        Ok(out)
    }

    fn values_complete(&self, values: &[Value], n: usize) -> io::Result<()> {
        for (i, v) in values.iter().enumerate() {
            if i < n && v.type_as_u32() == 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("missing column: {}", self.columns[i]),
                ));
            } else if i >= n && v.type_as_u32() != 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("extra column: {}", self.columns[i]),
                ));
            }
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String, String> {
        Ok(serde_json::to_string(self).unwrap())
    }

    pub fn from_json(json: String) -> TableDef {
        serde_json::from_str(json.as_str()).unwrap()
    }

    pub fn check(&self) -> io::Result<()> {
        // verify the table definition
        if self.name.is_empty() {
            return Err(Error::new(
                ErrorKind::Other,
                String::from("Table name is empty"),
            ));
        }
        if self.columns.is_empty() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Table '{}' has no columns.", self.name),
            ));
        }
        if self.columns.len() != self.types.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Table '{}' has mismatched number of columns and types.",
                    self.name
                ),
            ));
        }
        if !(1 <= self.primary_keys && self.primary_keys <= self.columns.len()) {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Table '{}' has an invalid number of primary keys.",
                    self.name
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::result;

    use super::*;

    #[test]
    fn test_check_record() {
        let table_def = TableDef {
            name: String::from("test_table"),
            types: vec![1, 2],
            columns: vec![String::from("col1"), String::from("col2")],
            primary_keys: 1,
            prefix: 123,
        };

        let record = Record {
            columns: vec![String::from("col1"), String::from("col2")],
            values: vec![
                Value::Bytes(Some("test".as_bytes().to_vec())),
                Value::Int64(Some(1)),
            ],
        };

        let result = table_def.check_record(&record, 2);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.len() == 2);
        assert_eq!(result[0], Value::Bytes(Some("test".as_bytes().to_vec())));
        assert_eq!(result[1], Value::Int64(Some(1)));
    }

    #[test]
    fn test_check_record_with_wrong_n() {
        let table_def = TableDef {
            name: String::from("test_table"),
            types: vec![1, 2],
            columns: vec![String::from("col1"), String::from("col2")],
            primary_keys: 1,
            prefix: 123,
        };

        let record = Record {
            columns: vec![String::from("col1"), String::from("col2")],
            values: vec![
                Value::Bytes(Some("test".as_bytes().to_vec())),
                Value::Int64(Some(1)),
            ],
        };

        let result = table_def.check_record(&record, 1);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "extra column: col2");
    }

    #[test]
    fn test_to_json() {
        let table_def = TableDef {
            name: String::from("test_table"),
            types: vec![0, 1, 2],
            columns: vec![
                String::from("col1"),
                String::from("col2"),
                String::from("col3"),
            ],
            primary_keys: 1,
            prefix: 123,
        };

        let result = table_def.to_json();
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            r#"{"name":"test_table","types":[0,1,2],"columns":["col1","col2","col3"],"primary_keys":1,"prefix":123}"#
        );
    }

    #[test]
    fn test_from_json() {
        let json = r#"{"name":"test_table","types":[0,1,2],"columns":["col1","col2","col3"],"primary_keys":1,"prefix":123}"#;

        let table_def = TableDef::from_json(json.to_string());
        assert_eq!(table_def.name, "test_table");
        assert_eq!(table_def.types, vec![0, 1, 2]);
        assert_eq!(
            table_def.columns,
            vec![
                String::from("col1"),
                String::from("col2"),
                String::from("col3")
            ]
        );
        assert_eq!(table_def.primary_keys, 1);
        assert_eq!(table_def.prefix, 123);
    }

    #[test]
    fn test_check() {
        let table_def = TableDef {
            name: String::from("test_table"),
            types: vec![0, 1, 2],
            columns: vec![
                String::from("col1"),
                String::from("col2"),
                String::from("col3"),
            ],
            primary_keys: 1,
            prefix: 123,
        };

        let result = table_def.check();
        assert!(result.is_ok());
    }
}
