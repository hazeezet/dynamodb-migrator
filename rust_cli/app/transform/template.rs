/// Apply a template string against a source item.
pub fn apply_template(template: &str, item: &serde_json::Map<String, Value>) -> Result<String> {
    // First validate the template structure
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

        let transformed = if let Some(transformation_name) = transformation {
            apply_transformation(&raw_value, transformation_name)?
        } else {
            format_value(&raw_value)
        };

        result = result.replacen(full_match, &transformed, 1);
    }

    Ok(result)
}

/// Validate a template string for syntax and transformation rules.
pub fn validate_template(template: &str) -> Result<()> {
    // 1. Basic balanced bracket check
    let open_count = template.chars().filter(|&c| c == '{').count();
    let close_count = template.chars().filter(|&c| c == '}').count();
    if open_count != close_count {
        return Err(anyhow!("Brackets are not balanced. Ensure every '{{' has a closing '}}'."));
    }

    let template_regex = Regex::new(r"\{([^}]+)\}").expect("invalid regex");
    for template_match in template_regex.captures_iter(template) {
        let content = template_match.get(1).unwrap().as_str().trim();
        let parts: Vec<&str> = content.split_whitespace().collect();
        
        if parts.len() > 1 {
            let op = parts[1].to_lowercase();
            let args = &parts[2..];
            validate_operation(&op, args)?;
        }
    }

    Ok(())
}

/// Verify that an operation is supported and has the correct number of arguments.
fn validate_operation(op: &str, args: &[&str]) -> Result<()> {
    match op {
        // String operations
        "upper" | "lower" | "title" | "strip" | "abs_value" | "sqrt" | "floor" | "ceil" => {
            if !args.is_empty() {
                return Err(anyhow!("Transformation '{}' expects 0 arguments, but got {}", op, args.len()));
            }
        }
        "split" | "add" | "subtract" | "multiply" | "divide" | "power" | "mod" | "round_to" => {
            if args.len() != 1 {
                return Err(anyhow!("Transformation '{}' expects exactly 1 argument, but got {}", op, args.len()));
            }
        }
        "replace" => {
            if args.len() != 2 {
                return Err(anyhow!("Transformation 'replace' expects exactly 2 arguments (old new), but got {}", args.len()));
            }
        }
        "substring" | "pad_left" | "pad_right" => {
            if args.is_empty() || args.len() > 2 {
                return Err(anyhow!("Transformation '{}' expects 1 or 2 arguments, but got {}", op, args.len()));
            }
        }
        _ => return Err(anyhow!("Unknown transformation: '{}'", op)),
    }
    Ok(())
}

/// Apply a single transformation to a value.
fn apply_transformation(value: &Value, transformation: &str) -> Result<String> {
    let parts: Vec<&str> = transformation.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(format_value(value));
    }

    let operation = parts[0].to_lowercase();
    let args = &parts[1..];

    debug!(
        "Applying transformation '{}' with args {:?} to value {:?}",
        operation, args, value
    );

    match operation.as_str() {
        // String operations
        "upper" => Ok(string_ops::upper(&value_as_str(value))),
        "lower" => Ok(string_ops::lower(&value_as_str(value))),
        "title" => Ok(string_ops::title(&value_as_str(value))),
        "strip" => Ok(string_ops::strip(&value_as_str(value))),
        "replace" => {
            if args.len() < 2 {
                return Err(anyhow!("'replace' requires two arguments: old new"));
            }
            Ok(string_ops::replace(&value_as_str(value), args[0], args[1]))
        }
        "split" => {
            let delimiter = args.first().copied().unwrap_or(",");
            let parts = string_ops::split(&value_as_str(value), delimiter);
            Ok(serde_json::to_string(&parts).unwrap_or_default())
        }
        "substring" => {
            let start: usize = args.first().and_then(|a| a.parse().ok()).unwrap_or(0);
            let end: Option<usize> = args.get(1).and_then(|a| a.parse().ok());
            Ok(string_ops::substring(&value_as_str(value), start, end))
        }
        "pad_left" => {
            let width: usize = args.first().and_then(|a| a.parse().ok()).unwrap_or(10);
            let fill = args.get(1).and_then(|a| a.chars().next()).unwrap_or('0');
            Ok(string_ops::pad_left(&value_as_str(value), width, fill))
        }
        "pad_right" => {
            let width: usize = args.first().and_then(|a| a.parse().ok()).unwrap_or(10);
            let fill = args.get(1).and_then(|a| a.chars().next()).unwrap_or('0');
            Ok(string_ops::pad_right(&value_as_str(value), width, fill))
        }

        // Number operations
        "add" => {
            let amount = parse_num_arg(args, 0, "add")?;
            Ok(format_number(number_ops::add(value_as_f64(value)?, amount)))
        }
        "subtract" => {
            let amount = parse_num_arg(args, 0, "subtract")?;
            Ok(format_number(number_ops::subtract(
                value_as_f64(value)?,
                amount,
            )))
        }
        "multiply" => {
            let factor = parse_num_arg(args, 0, "multiply")?;
            Ok(format_number(number_ops::multiply(
                value_as_f64(value)?,
                factor,
            )))
        }
        "divide" => {
            let divisor = parse_num_arg(args, 0, "divide")?;
            Ok(format_number(number_ops::divide(
                value_as_f64(value)?,
                divisor,
            )?))
        }
        "round_to" => {
            let decimals: u32 = args.first().and_then(|a| a.parse().ok()).unwrap_or(0);
            Ok(format_number(number_ops::round_to(
                value_as_f64(value)?,
                decimals,
            )))
        }
        "abs_value" => Ok(format_number(number_ops::abs_value(value_as_f64(value)?))),
        "power" => {
            let exponent = parse_num_arg(args, 0, "power")?;
            Ok(format_number(number_ops::power(
                value_as_f64(value)?,
                exponent,
            )))
        }
        "sqrt" => Ok(format_number(number_ops::sqrt(value_as_f64(value)?)?)),
        "floor" => Ok(format_number(number_ops::floor(value_as_f64(value)?))),
        "ceil" => Ok(format_number(number_ops::ceil(value_as_f64(value)?))),
        "mod" => {
            let divisor = parse_num_arg(args, 0, "mod")?;
            Ok(format_number(number_ops::modulo(
                value_as_f64(value)?,
                divisor,
            )))
        }

        _ => {
            error!("Unknown transformation: {}", operation);
            Err(anyhow!("Unknown transformation: {}", operation))
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
    if number == number.trunc() && number.abs() < i64::MAX as f64 {
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
        // upper expects 0, gets 1
        let err = validate_template("{name upper something}").unwrap_err();
        assert!(err.to_string().contains("expects 0 arguments"));

        // add expects 1, gets 2
        let err = validate_template("{age add 10 20}").unwrap_err();
        assert!(err.to_string().contains("expects exactly 1 argument"));

        // replace expects 2, gets 1
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
