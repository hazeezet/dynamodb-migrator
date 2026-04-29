/// Template processor for column mapping transformations.
use anyhow::{anyhow, Result};
use regex::Regex;
use serde_json::Value;
use std::str::FromStr;
use tracing::debug;

use super::{number_ops, string_ops};

/// Supported transformations in the system.
/// Adding a variant here forces you to update all match blocks!
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transformation {
    Upper,
    Lower,
    Title,
    Strip,
    Replace,
    Split,
    Substring,
    PadLeft,
    PadRight,
    Add,
    Subtract,
    Multiply,
    Divide,
    RoundTo,
    AbsValue,
    Power,
    Sqrt,
    Floor,
    Ceil,
    Mod,
}

impl FromStr for Transformation {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "upper" => Ok(Transformation::Upper),
            "lower" => Ok(Transformation::Lower),
            "title" => Ok(Transformation::Title),
            "strip" => Ok(Transformation::Strip),
            "replace" => Ok(Transformation::Replace),
            "split" => Ok(Transformation::Split),
            "substring" => Ok(Transformation::Substring),
            "pad_left" => Ok(Transformation::PadLeft),
            "pad_right" => Ok(Transformation::PadRight),
            "add" => Ok(Transformation::Add),
            "subtract" => Ok(Transformation::Subtract),
            "multiply" => Ok(Transformation::Multiply),
            "divide" => Ok(Transformation::Divide),
            "round_to" => Ok(Transformation::RoundTo),
            "abs_value" => Ok(Transformation::AbsValue),
            "power" => Ok(Transformation::Power),
            "sqrt" => Ok(Transformation::Sqrt),
            "floor" => Ok(Transformation::Floor),
            "ceil" => Ok(Transformation::Ceil),
            "mod" => Ok(Transformation::Mod),
            _ => Err(anyhow!("Unknown transformation: '{}'", s)),
        }
    }
}

impl Transformation {
    /// Returns (min_args, max_args) for this transformation.
    pub fn arg_range(&self) -> (usize, usize) {
        match self {
            Transformation::Upper
            | Transformation::Lower
            | Transformation::Title
            | Transformation::Strip
            | Transformation::AbsValue
            | Transformation::Sqrt
            | Transformation::Floor
            | Transformation::Ceil => (0, 0),

            Transformation::Split
            | Transformation::Add
            | Transformation::Subtract
            | Transformation::Multiply
            | Transformation::Divide
            | Transformation::Power
            | Transformation::Mod
            | Transformation::RoundTo => (1, 1),

            Transformation::Replace => (2, 2),

            Transformation::Substring | Transformation::PadLeft | Transformation::PadRight => {
                (1, 2)
            }
        }
    }
}

/// Apply a template string against a source item.
pub fn apply_template(template: &str, item: &serde_json::Map<String, Value>) -> Result<String> {
    validate_template(template)?;

    let template_regex = Regex::new(r"\{([^}]+)\}").expect("invalid regex");
    let mut result = template.to_string();

    for template_match in template_regex.captures_iter(template) {
        let full_match = template_match.get(0).unwrap().as_str();
        let content = template_match.get(1).unwrap().as_str().trim();

        let parts: Vec<&str> = content.splitn(2, ' ').collect();
        let field_name = parts[0];
        let transformation = if parts.len() > 1 {
            Some(parts[1])
        } else {
            None
        };

        let raw_value = item.get(field_name).cloned().unwrap_or(Value::Null);

        let transformed = if let Some(transformation_str) = transformation {
            apply_transformation(&raw_value, transformation_str)?
        } else {
            format_value(&raw_value)
        };

        result = result.replacen(full_match, &transformed, 1);
    }

    Ok(result)
}

/// Validate a template string for syntax and transformation rules.
pub fn validate_template(template: &str) -> Result<()> {
    let open_count = template.chars().filter(|&c| c == '{').count();
    let close_count = template.chars().filter(|&c| c == '}').count();
    if open_count != close_count {
        return Err(anyhow!(
            "Brackets are not balanced. Ensure every '{{' has a closing '}}'."
        ));
    }

    let template_regex = Regex::new(r"\{([^}]+)\}").expect("invalid regex");
    for template_match in template_regex.captures_iter(template) {
        let content = template_match.get(1).unwrap().as_str().trim();
        let parts: Vec<&str> = content.split_whitespace().collect();

        if parts.len() > 1 {
            let op_str = parts[1];
            let args = &parts[2..];
            validate_operation(op_str, args)?;
        }
    }

    Ok(())
}

/// Verify that an operation is supported and has the correct number of arguments.
fn validate_operation(op_str: &str, args: &[&str]) -> Result<()> {
    // Parsing into the Enum ensures the name is valid
    let op = Transformation::from_str(op_str)?;
    let (min, max) = op.arg_range();

    if args.len() < min || args.len() > max {
        if min == max {
            return Err(anyhow!(
                "Transformation '{}' expects exactly {} arguments, but got {}",
                op_str,
                min,
                args.len()
            ));
        } else {
            return Err(anyhow!(
                "Transformation '{}' expects {}-{} arguments, but got {}",
                op_str,
                min,
                max,
                args.len()
            ));
        }
    }
    Ok(())
}

/// Apply a single transformation to a value.
fn apply_transformation(value: &Value, transformation_str: &str) -> Result<String> {
    let parts: Vec<&str> = transformation_str.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(format_value(value));
    }

    // Parse the transformation into our Enum
    let op = Transformation::from_str(parts[0])?;
    let args = &parts[1..];

    debug!(
        "Applying transformation {:?} with args {:?} to value {:?}",
        op, args, value
    );

    // RUST COMPILER WILL ERROR HERE IF YOU FORGET A VARIANT
    match op {
        Transformation::Upper => Ok(string_ops::upper(&value_as_str(value))),
        Transformation::Lower => Ok(string_ops::lower(&value_as_str(value))),
        Transformation::Title => Ok(string_ops::title(&value_as_str(value))),
        Transformation::Strip => Ok(string_ops::strip(&value_as_str(value))),
        Transformation::Replace => {
            let old = args.first().ok_or_else(|| anyhow!("Missing 'old' arg"))?;
            let new = args.get(1).ok_or_else(|| anyhow!("Missing 'new' arg"))?;
            Ok(string_ops::replace(&value_as_str(value), old, new))
        }
        Transformation::Split => {
            let delimiter = args.first().copied().unwrap_or(",");
            let parts = string_ops::split(&value_as_str(value), delimiter);
            Ok(serde_json::to_string(&parts).unwrap_or_default())
        }
        Transformation::Substring => {
            let start: usize = args
                .first()
                .and_then(|a| a.parse().ok())
                .ok_or_else(|| anyhow!("Invalid start index"))?;
            let end: Option<usize> = args.get(1).and_then(|a| a.parse().ok());
            Ok(string_ops::substring(&value_as_str(value), start, end))
        }
        Transformation::PadLeft => {
            let width: usize = args
                .first()
                .and_then(|a| a.parse().ok())
                .ok_or_else(|| anyhow!("Invalid width"))?;
            let fill = args.get(1).and_then(|a| a.chars().next()).unwrap_or('0');
            Ok(string_ops::pad_left(&value_as_str(value), width, fill))
        }
        Transformation::PadRight => {
            let width: usize = args
                .first()
                .and_then(|a| a.parse().ok())
                .ok_or_else(|| anyhow!("Invalid width"))?;
            let fill = args.get(1).and_then(|a| a.chars().next()).unwrap_or('0');
            Ok(string_ops::pad_right(&value_as_str(value), width, fill))
        }
        Transformation::Add => {
            let amount = parse_num_arg(args, 0, "add")?;
            Ok(format_number(number_ops::add(value_as_f64(value)?, amount)))
        }
        Transformation::Subtract => {
            let amount = parse_num_arg(args, 0, "subtract")?;
            Ok(format_number(number_ops::subtract(
                value_as_f64(value)?,
                amount,
            )))
        }
        Transformation::Multiply => {
            let factor = parse_num_arg(args, 0, "multiply")?;
            Ok(format_number(number_ops::multiply(
                value_as_f64(value)?,
                factor,
            )))
        }
        Transformation::Divide => {
            let divisor = parse_num_arg(args, 0, "divide")?;
            Ok(format_number(number_ops::divide(
                value_as_f64(value)?,
                divisor,
            )?))
        }
        Transformation::RoundTo => {
            let decimals: u32 = args.first().and_then(|a| a.parse().ok()).unwrap_or(0);
            Ok(format_number(number_ops::round_to(
                value_as_f64(value)?,
                decimals,
            )))
        }
        Transformation::AbsValue => Ok(format_number(number_ops::abs_value(value_as_f64(value)?))),
        Transformation::Power => {
            let exponent = parse_num_arg(args, 0, "power")?;
            Ok(format_number(number_ops::power(
                value_as_f64(value)?,
                exponent,
            )))
        }
        Transformation::Sqrt => Ok(format_number(number_ops::sqrt(value_as_f64(value)?)?)),
        Transformation::Floor => Ok(format_number(number_ops::floor(value_as_f64(value)?))),
        Transformation::Ceil => Ok(format_number(number_ops::ceil(value_as_f64(value)?))),
        Transformation::Mod => {
            let divisor = parse_num_arg(args, 0, "mod")?;
            Ok(format_number(number_ops::modulo(
                value_as_f64(value)?,
                divisor,
            )))
        }
    }
}

/// Convert a serde_json::Value to a display string.
fn value_as_str(value: &Value) -> String {
    match value {
        Value::String(string_val) => string_val.clone(),
        Value::Number(number_val) => number_val.to_string(),
        Value::Bool(bool_val) => bool_val.to_string(),
        Value::Null => "null".to_string(),
        other_value => other_value.to_string(),
    }
}

/// Convert a serde_json::Value to f64 for numeric operations.
fn value_as_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Number(number_val) => number_val
            .as_f64()
            .ok_or_else(|| anyhow!("Cannot convert number to f64: {number_val}")),
        Value::String(string_val) => string_val
            .parse::<f64>()
            .map_err(|_| anyhow!("Cannot parse '{string_val}' as a number")),
        _ => Err(anyhow!("Value is not numeric: {value}")),
    }
}

/// Parse a required numeric argument from the args slice.
fn parse_num_arg(args: &[&str], index: usize, op_name: &str) -> Result<f64> {
    args.get(index)
        .ok_or_else(|| anyhow!("'{}' requires a numeric argument", op_name))?
        .parse::<f64>()
        .map_err(|_| anyhow!("Invalid numeric argument for '{}'", op_name))
}

/// Format a float, removing trailing `.0` for whole numbers.
fn format_number(number: f64) -> String {
    if (number - number.trunc()).abs() < f64::EPSILON && number.abs() < i64::MAX as f64 {
        format!("{}", number as i64)
    } else {
        format!("{number}")
    }
}

/// Format a `serde_json::Value` for template replacement.
pub fn format_value(value: &Value) -> String {
    match value {
        Value::String(string_val) => string_val.clone(),
        Value::Number(number_val) => number_val.to_string(),
        Value::Bool(bool_val) => bool_val.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_item() -> serde_json::Map<String, Value> {
        let obj = json!({
            "id": "user123",
            "name": "john doe",
            "email": "JOHN.DOE@EXAMPLE.COM",
            "age": 25,
            "price": "100.50",
            "description": "This is a test description"
        });
        obj.as_object().unwrap().clone()
    }

    #[test]
    fn test_validator_success() {
        assert!(validate_template("{name upper}").is_ok());
        assert!(validate_template("{age add 10}").is_ok());
        assert!(validate_template("{name replace a b}").is_ok());
        assert!(validate_template("Static {id} text").is_ok());
    }

    #[test]
    fn test_validator_unbalanced_brackets() {
        let err = validate_template("{name upper").unwrap_err();
        assert!(err.to_string().contains("balanced"));
    }

    #[test]
    fn test_validator_unknown_op() {
        let err = validate_template("{name magic}").unwrap_err();
        assert!(err.to_string().contains("Unknown transformation"));
    }

    #[test]
    fn test_validator_wrong_args() {
        let err = validate_template("{name upper something}").unwrap_err();
        assert!(err.to_string().contains("expects exactly 0 arguments"));

        let err = validate_template("{age add 10 20}").unwrap_err();
        assert!(err.to_string().contains("expects exactly 1 argument"));

        let err = validate_template("{name replace john}").unwrap_err();
        assert!(err.to_string().contains("expects exactly 2 arguments"));
    }

    #[test]
    fn test_simple_placeholder() {
        let item = make_item();
        let result = apply_template("{id}", &item).unwrap();
        assert_eq!(result, "user123");
    }

    #[test]
    fn test_upper_transformation() {
        let item = make_item();
        let result = apply_template("{name upper}", &item).unwrap();
        assert_eq!(result, "JOHN DOE");
    }

    #[test]
    fn test_lower_transformation() {
        let item = make_item();
        let result = apply_template("{email lower}", &item).unwrap();
        assert_eq!(result, "john.doe@example.com");
    }

    #[test]
    fn test_title_transformation() {
        let item = make_item();
        let result = apply_template("{name title}", &item).unwrap();
        assert_eq!(result, "John Doe");
    }

    #[test]
    fn test_add_transformation() {
        let item = make_item();
        let result = apply_template("{age add 5}", &item).unwrap();
        assert_eq!(result, "30");
    }

    #[test]
    fn test_multiply_string_number() {
        let item = make_item();
        let result = apply_template("{price multiply 2}", &item).unwrap();
        assert_eq!(result, "201");
    }

    #[test]
    fn test_complex_template() {
        let item = make_item();
        let result = apply_template("USER#{id upper}", &item).unwrap();
        assert_eq!(result, "USER#USER123");
    }

    #[test]
    fn test_multiple_placeholders() {
        let item = make_item();
        let result = apply_template("{name title} <{email lower}>", &item).unwrap();
        assert_eq!(result, "John Doe <john.doe@example.com>");
    }

    #[test]
    fn test_substring() {
        let item = make_item();
        let result = apply_template("{description substring 0 4}", &item).unwrap();
        assert_eq!(result, "This");
    }

    #[test]
    fn test_missing_field() {
        let item = make_item();
        let result = apply_template("{nonexistent}", &item).unwrap();
        assert_eq!(result, "null");
    }
}
