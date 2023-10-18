// Table Cell
#[derive(Clone)]
pub enum Value {
    Error,
    Bytes(Vec<u8>),
    Int64(i64),
}

impl Value {
    pub const ERROR_TYPE: u32 = 0;
    pub const BYTES_TYPE: u32 = 1;
    pub const INT64_TYPE: u32 = 2;

    pub fn type_as_u32(&self) -> u32 {
        match self {
            Value::Error => Value::ERROR_TYPE,
            Value::Bytes(_) => Value::BYTES_TYPE,
            Value::Int64(_) => Value::INT64_TYPE,
        }
    }

    pub fn bytes_to_string(&self) -> Result<String, ()> {
        match self {
            Value::Bytes(bytes) => Ok(String::from(String::from_utf8_lossy(bytes))),
            _ => Err(()),
        }
    }

    // Strings are encoded as null-terminated strings,
    // escape the null byte so that strings contain no null byte.
    pub fn escape_string(in_bytes: &Vec<u8>) -> Vec<u8> {
        let num_zeros = in_bytes.iter().filter(|&&x| x == 0).count();
        let num_ones = in_bytes.iter().filter(|&&x| x == 1).count();
        if num_zeros + num_ones == 0 {
            return in_bytes.clone();
        }

        let mut out = Vec::with_capacity(in_bytes.len() + num_zeros + num_ones);
        let mut pos = 0;
        for &ch in in_bytes {
            if ch <= 1 {
                out[pos] = 0x01;
                out[pos + 1] = ch + 1;
                pos += 2;
            } else {
                out[pos] = ch;
                pos += 1;
            }
        }
        out
    }

    pub fn unescape_string(in_bytes: &Vec<u8>) -> Vec<u8> {
        let num_zeros = in_bytes.iter().filter(|&&x| x == 0).count();
        if num_zeros == 0 {
            return in_bytes.clone();
        }

        let mut out = Vec::with_capacity(in_bytes.len());
        let mut pos = 0;
        let mut i = 0;
        while pos  < in_bytes.len() {
            if in_bytes[i] == 0x01 {
                i += 1;
                assert!(in_bytes[i] >= 1);
                out[pos] = in_bytes[i] - 1;
            } else {
                out[pos] = in_bytes[i];
            }
            i += 1;
            pos += 1;
        }

        out[0..pos].to_vec()
    }
}
