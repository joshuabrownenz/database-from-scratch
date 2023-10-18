use super::value::Value;

// Table row
pub struct Record {
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

impl Record {
    pub fn new() -> Record {
        Record {
            columns: vec![],
            values: vec![],
        }
    }

    pub fn add_bytes(&mut self, key: String, value: Vec<u8>) {
        assert!(!self.columns.contains(&key));
        self.columns.push(key);
        self.values.push(Value::Bytes(value));
    }

    pub fn add_int64(&mut self, key: String, value: i64) {
        assert!(!self.columns.contains(&key));
        self.columns.push(key);
        self.values.push(Value::Int64(value));
    }

    pub fn get(&self, key: &String) -> Option<&Value> {
        match self.columns.iter().position(|x| x == key) {
            Some(index) => Some(&self.values[index]),
            None => None,
        }
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}
