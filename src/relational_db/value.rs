use std::fmt;

// Table Cell
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Error,
    Bytes(Option<Vec<u8>>),
    Int64(Option<i64>),
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

    pub fn u32_to_empty_value(u: u32) -> Value {
        match u {
            Value::ERROR_TYPE => Value::Error,
            Value::BYTES_TYPE => Value::Bytes(None),
            Value::INT64_TYPE => Value::Int64(None),
            _ => panic!("Invalid type"),
        }
    }

    pub fn bytes(&self) -> &Vec<u8> {
        match self {
            Value::Bytes(bytes) => bytes.as_ref().unwrap(),
            _ => panic!("Invalid type"),
        }
    }

    pub fn bytes_to_string(&self) -> Result<String, ValueParseError> {
        match self {
            Value::Bytes(bytes) => Ok(String::from(String::from_utf8_lossy(
                bytes.as_ref().unwrap(),
            ))),
            _ => Err(ValueParseError),
        }
    }

    pub fn get_int64(&self) -> Result<Option<i64>, ValueParseError> {
        match self {
            Value::Int64(i) => Ok(i.to_owned()),
            _ => Err(ValueParseError),
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

        let mut out = vec![0; in_bytes.len() + num_zeros + num_ones];
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

    pub fn unescape_string(in_bytes: &[u8]) -> Vec<u8> {
        // Count the number of 0x01 bytes (escape bytes)
        let num_escape_bytes = in_bytes.iter().filter(|&&x| x == 0x01).count();

        // If there are no escape bytes, return the input as is
        if num_escape_bytes == 0 {
            return in_bytes.to_vec();
        }

        // Allocate a new Vec<u8> with an adjusted size
        let mut out = Vec::with_capacity(in_bytes.len() - num_escape_bytes);

        let mut i = 0;
        while i < in_bytes.len() {
            if in_bytes[i] == 0x01 {
                // Move to the next byte and ensure it is within bounds
                i += 1;
                if i >= in_bytes.len() {
                    // Handle potential out-of-bounds error or malformed input
                    panic!("Malformed input: escape byte at end of input");
                }
                // Unescape the byte and add to output
                out.push(in_bytes[i] - 1);
            } else {
                // Add non-escaped byte to output
                out.push(in_bytes[i]);
            }
            i += 1;
        }

        out
    }
}

#[derive(Debug)]
pub struct ValueParseError;

impl fmt::Display for ValueParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Value Parse Error")
    }
}

impl std::error::Error for ValueParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_empty() {
        let empty_vec: Vec<u8> = vec![];
        assert_eq!(Value::escape_string(&vec![]), empty_vec);
    }

    #[test]
    fn test_escape_no_special_bytes() {
        assert_eq!(Value::escape_string(&vec![2, 3, 4, 5]), vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_escape_with_null_bytes() {
        assert_eq!(
            Value::escape_string(&vec![0, 2, 0, 3]),
            vec![1, 1, 2, 1, 1, 3]
        );
    }

    #[test]
    fn test_escape_with_one_bytes() {
        assert_eq!(
            Value::escape_string(&vec![1, 3, 1, 4]),
            vec![1, 2, 3, 1, 2, 4]
        );
    }

    #[test]
    fn test_escape_mixed_null_and_one_bytes() {
        assert_eq!(
            Value::escape_string(&vec![0, 1, 2, 0, 1]),
            vec![1, 1, 1, 2, 2, 1, 1, 1, 2]
        );
    }

    #[test]
    fn test_unescape_empty() {
        let empty_vec: Vec<u8> = vec![];
        assert_eq!(Value::unescape_string(&[]), empty_vec);
    }

    #[test]
    fn test_unescape_no_escape_bytes() {
        assert_eq!(Value::unescape_string(&[2, 3, 4, 5]), vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_unescape_escaped_null_bytes() {
        assert_eq!(
            Value::unescape_string(&[1, 1, 2, 1, 1, 3]),
            vec![0, 2, 0, 3]
        );
    }

    #[test]
    fn test_unescape_escaped_one_bytes() {
        assert_eq!(
            Value::unescape_string(&[1, 2, 3, 1, 2, 4]),
            vec![1, 3, 1, 4]
        );
    }

    #[should_panic(expected = "Malformed input: escape byte at end of input")]
    #[test]
    fn test_unescape_malformed_input() {
        Value::unescape_string(&[1, 2, 3, 1]);
    }

    #[test]
    fn test_escape_unescape() {
        let values = vec![0, 1, 2, 3, 4, 5];

        let escaped = Value::escape_string(&values);
        let unescaped = Value::unescape_string(&escaped);

        assert_eq!(values, unescaped);
    }

    #[test]
    fn test_escape_unescape_mixed_bytes() {
        let values = vec![0, 1, 2, 3, 4, 5, 0, 1];

        let escaped = Value::escape_string(&values);
        let unescaped = Value::unescape_string(&escaped);

        assert_eq!(values, unescaped);
    }

    #[test]
    fn test_escape_unescape_with_repeated_sequences() {
        let values = vec![0, 0, 1, 1, 2, 2, 3, 3];

        let escaped = Value::escape_string(&values);
        let unescaped = Value::unescape_string(&escaped);

        assert_eq!(values, unescaped);
    }

    #[test]
    fn test_escape_unescape_with_no_special_characters() {
        let values = vec![2, 3, 4, 5, 6, 7, 8, 9];

        let escaped = Value::escape_string(&values);
        let unescaped = Value::unescape_string(&escaped);

        assert_eq!(values, unescaped);
    }

    #[test]
    fn test_escape_unescape_long_sequence() {
        let values = (0..100).collect::<Vec<u8>>();

        let escaped = Value::escape_string(&values);
        let unescaped = Value::unescape_string(&escaped);

        assert_eq!(values, unescaped);
    }

    #[test]
    fn test_escape_unescape_all_possible_bytes() {
        let values = (0u8..=255).collect::<Vec<u8>>();

        let escaped = Value::escape_string(&values);
        let unescaped = Value::unescape_string(&escaped);

        assert_eq!(values, unescaped);
    }
}
