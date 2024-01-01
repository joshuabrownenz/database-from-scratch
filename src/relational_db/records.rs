use super::value::Value;

// Table row
#[derive(Debug, Clone, PartialEq)]
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

    pub fn add_bytes(&mut self, key: String, value: Vec<u8>) -> &mut Self {
        assert!(!self.columns.contains(&key));
        self.columns.push(key);
        self.values.push(Value::Bytes(Some(value)));
        self
    }

    pub fn add_int64(&mut self, key: String, value: i64) -> &mut Self {
        assert!(!self.columns.contains(&key));
        self.columns.push(key);
        self.values.push(Value::Int64(Some(value)));
        self
    }

    pub fn set_bytes(&mut self, key: String, value: Vec<u8>) {
        match self.columns.iter().position(|x| x == &key) {
            Some(index) => self.values[index] = Value::Bytes(Some(value)),
            None => panic!("set_bytes: Column not found: {}", key),
        }
    }

    pub fn set_int64(&mut self, key: String, value: i64) {
        match self.columns.iter().position(|x| x == &key) {
            Some(index) => self.values[index] = Value::Int64(Some(value)),
            None => panic!("set_in64: Column not found: {}", key),
        }
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
