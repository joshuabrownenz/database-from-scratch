#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ValueType {
    Error = 0,
    Bytes = 1,
    Int64 = 2,
}

impl ValueType {
    pub fn value(&self) -> u32 {
        *self as u32
    }
}

// Table Cell
pub struct Value {
    value_type : u32,
    i64: i64,
    str : Vec<u8>,
}

// Tbale row
pub struct Record {
    columns : Vec<String>,
    values : Vec<Value>,
}

impl Record {
    pub fn addStr(&mut self, key : String, value :  Vec<u8>) {

    }

    pub fn addInt64(&mut self, key : String, value : i64) {

    }

    pub fn get(&self, key : String) -> Value {
        let index = self.columns.iter().position(|x| *x == key).unwrap();
    }
}