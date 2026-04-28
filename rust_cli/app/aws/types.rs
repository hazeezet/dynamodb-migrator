/// DynamoDB type conversion utilities.
///
/// Converts between `serde_json::Value` and AWS SDK `AttributeValue`.
use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;

/// Convert a `serde_json::Value` to a DynamoDB `AttributeValue`.
///
/// Mapping:
/// - String → S
/// - Number → N (as string)
/// - Bool → BOOL
/// - Null → NUL
/// - Array → L (recursive)
/// - Object → M (recursive)
pub fn to_attribute_value(value: &Value) -> AttributeValue {
    match value {
        Value::String(string_val) => AttributeValue::S(string_val.clone()),
        Value::Number(number_val) => AttributeValue::N(number_val.to_string()),
        Value::Bool(bool_val) => AttributeValue::Bool(*bool_val),
        Value::Null => AttributeValue::Null(true),
        Value::Array(array_val) => {
            let items: Vec<AttributeValue> = array_val.iter().map(to_attribute_value).collect();
            AttributeValue::L(items)
        }
        Value::Object(object_map) => {
            let items = object_map
                .iter()
                .map(|(key, val)| (key.clone(), to_attribute_value(val)))
                .collect();
            AttributeValue::M(items)
        }
    }
}

/// Convert a DynamoDB `AttributeValue` to a `serde_json::Value`.
///
/// Used when reading items from scan results.
pub fn from_attribute_value(attribute: &AttributeValue) -> Value {
    match attribute {
        AttributeValue::S(string_val) => Value::String(string_val.clone()),
        AttributeValue::N(number_str) => {
            // Try to parse as integer first, then float
            if let Ok(integer_val) = number_str.parse::<i64>() {
                Value::Number(integer_val.into())
            } else if let Ok(float_val) = number_str.parse::<f64>() {
                serde_json::Number::from_f64(float_val)
                    .map(Value::Number)
                    .unwrap_or(Value::String(number_str.clone()))
            } else {
                Value::String(number_str.clone())
            }
        }
        AttributeValue::Bool(bool_val) => Value::Bool(*bool_val),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::L(list_items) => {
            Value::Array(list_items.iter().map(from_attribute_value).collect())
        }
        AttributeValue::M(map_items) => {
            let object: serde_json::Map<String, Value> = map_items
                .iter()
                .map(|(key, val)| (key.clone(), from_attribute_value(val)))
                .collect();
            Value::Object(object)
        }
        AttributeValue::Ss(string_set) => Value::Array(
            string_set
                .iter()
                .map(|item| Value::String(item.clone()))
                .collect(),
        ),
        AttributeValue::Ns(number_set) => Value::Array(
            number_set
                .iter()
                .map(|number_str| {
                    if let Ok(integer_val) = number_str.parse::<i64>() {
                        Value::Number(integer_val.into())
                    } else {
                        Value::String(number_str.clone())
                    }
                })
                .collect(),
        ),
        AttributeValue::Bs(binary_set) => Value::Array(
            binary_set
                .iter()
                .map(|blob| Value::String(base64_encode(blob.as_ref())))
                .collect(),
        ),
        AttributeValue::B(binary_blob) => Value::String(base64_encode(binary_blob.as_ref())),
        _ => Value::Null,
    }
}

/// Simple base64 encoding for binary data.
fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 {
            chunk[1] as usize
        } else {
            0
        };
        let b2 = if chunk.len() > 2 {
            chunk[2] as usize
        } else {
            0
        };
        let _ = write!(result, "{}", CHARS[(b0 >> 2) & 0x3F] as char);
        let _ = write!(result, "{}", CHARS[((b0 << 4) | (b1 >> 4)) & 0x3F] as char);
        if chunk.len() > 1 {
            let _ = write!(result, "{}", CHARS[((b1 << 2) | (b2 >> 6)) & 0x3F] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            let _ = write!(result, "{}", CHARS[b2 & 0x3F] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_string_roundtrip() {
        let val = json!("hello");
        let av = to_attribute_value(&val);
        assert_eq!(av, AttributeValue::S("hello".to_string()));
        assert_eq!(from_attribute_value(&av), val);
    }

    #[test]
    fn test_number_roundtrip() {
        let val = json!(42);
        let av = to_attribute_value(&val);
        assert_eq!(av, AttributeValue::N("42".to_string()));
        assert_eq!(from_attribute_value(&av), val);
    }

    #[test]
    fn test_bool_roundtrip() {
        let val = json!(true);
        let av = to_attribute_value(&val);
        assert_eq!(av, AttributeValue::Bool(true));
        assert_eq!(from_attribute_value(&av), val);
    }

    #[test]
    fn test_null_roundtrip() {
        let val = json!(null);
        let av = to_attribute_value(&val);
        assert_eq!(av, AttributeValue::Null(true));
        assert_eq!(from_attribute_value(&av), val);
    }

    #[test]
    fn test_nested_object() {
        let val = json!({"name": "test", "count": 5});
        let av = to_attribute_value(&val);
        let back = from_attribute_value(&av);
        assert_eq!(back, val);
    }

    #[test]
    fn test_array() {
        let val = json!(["a", "b", "c"]);
        let av = to_attribute_value(&val);
        let back = from_attribute_value(&av);
        assert_eq!(back, val);
    }
}
