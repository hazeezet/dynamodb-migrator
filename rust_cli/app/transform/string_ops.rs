/// String transformation operations for template processing.
///
/// Each function takes a string value (and optional arguments) and returns
/// the transformed result.
///
/// Convert string to uppercase.
pub fn upper(value: &str) -> String {
    value.to_uppercase()
}

/// Convert string to lowercase.
pub fn lower(value: &str) -> String {
    value.to_lowercase()
}

/// Convert string to title case (first letter of each word capitalized).
pub fn title(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut characters = word.chars();
            match characters.next() {
                Some(first_char) => {
                    let uppercase_char: String = first_char.to_uppercase().collect();
                    let remaining_chars: String = characters.as_str().to_lowercase();
                    format!("{uppercase_char}{remaining_chars}")
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Remove leading and trailing whitespace.
pub fn strip(value: &str) -> String {
    value.trim().to_string()
}

/// Replace all occurrences of `old` with `new` in the string.
pub fn replace(value: &str, old: &str, new: &str) -> String {
    value.replace(old, new)
}

/// Split string by delimiter into a JSON array string.
pub fn split(value: &str, delimiter: &str) -> Vec<String> {
    value
        .split(delimiter)
        .map(|substring_part| substring_part.to_string())
        .collect()
}

/// Extract a substring from `start` to `end` (exclusive).
///
/// If `end` is `None`, extracts from `start` to the end of the string.
pub fn substring(value: &str, start: usize, end: Option<usize>) -> String {
    let characters: Vec<char> = value.chars().collect();
    let total_length = characters.len();
    let start_index = start.min(total_length);
    let end_index = end
        .map(|end_pos| end_pos.min(total_length))
        .unwrap_or(total_length);
    characters[start_index..end_index].iter().collect()
}

/// Pad string on the left to reach `width`, using `fill_char`.
pub fn pad_left(value: &str, width: usize, fill_char: char) -> String {
    if value.len() >= width {
        return value.to_string();
    }
    let padding = width - value.len();
    let pad: String = std::iter::repeat_n(fill_char, padding).collect();
    format!("{value}{pad}")
}

/// Pad string on the right to reach `width`, using `fill_char`.
pub fn pad_right(value: &str, width: usize, fill_char: char) -> String {
    if value.len() >= width {
        return value.to_string();
    }
    let padding = width - value.len();
    let pad: String = std::iter::repeat_n(fill_char, padding).collect();
    format!("{pad}{value}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upper() {
        assert_eq!(upper("hello"), "HELLO");
    }

    #[test]
    fn test_lower() {
        assert_eq!(lower("HELLO"), "hello");
    }

    #[test]
    fn test_title() {
        assert_eq!(title("hello world"), "Hello World");
        assert_eq!(title("HELLO WORLD"), "Hello World");
    }

    #[test]
    fn test_strip() {
        assert_eq!(strip("  hello  "), "hello");
    }

    #[test]
    fn test_replace() {
        assert_eq!(replace("hello world", "world", "rust"), "hello rust");
    }

    #[test]
    fn test_split() {
        assert_eq!(
            split("a,b,c", ","),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_substring() {
        assert_eq!(substring("hello world", 0, Some(5)), "hello");
        assert_eq!(substring("hello", 2, None), "llo");
    }

    #[test]
    fn test_pad_left() {
        assert_eq!(pad_left("42", 5, '0'), "42000");
    }

    #[test]
    fn test_pad_right() {
        assert_eq!(pad_right("42", 5, '0'), "00042");
    }
}
