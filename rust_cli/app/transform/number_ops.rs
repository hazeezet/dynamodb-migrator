/// Number transformation operations for template processing.
///
/// All functions accept `f64` to handle both integer and floating-point
/// values uniformly, matching the Python tool's `float()` conversion behavior.
use anyhow::{bail, Result};

/// Add `amount` to `value`.
pub fn add(value: f64, amount: f64) -> f64 {
    value + amount
}

/// Subtract `amount` from `value`.
pub fn subtract(value: f64, amount: f64) -> f64 {
    value - amount
}

/// Multiply `value` by `factor`.
pub fn multiply(value: f64, factor: f64) -> f64 {
    value * factor
}

/// Divide `value` by `divisor`. Returns error on division by zero.
pub fn divide(value: f64, divisor: f64) -> Result<f64> {
    if divisor == 0.0 {
        bail!("Cannot divide by zero");
    }
    Ok(value / divisor)
}

/// Round `value` to `decimals` decimal places.
pub fn round_to(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

/// Get absolute value.
pub fn abs_value(value: f64) -> f64 {
    value.abs()
}

/// Raise `value` to the power of `exponent`.
pub fn power(value: f64, exponent: f64) -> f64 {
    value.powf(exponent)
}

/// Square root of `value`. Returns error for negative numbers.
pub fn sqrt(value: f64) -> Result<f64> {
    if value < 0.0 {
        bail!("Cannot take square root of negative number");
    }
    Ok(value.sqrt())
}

/// Floor (round down) to nearest integer.
pub fn floor(value: f64) -> f64 {
    value.floor()
}

/// Ceiling (round up) to nearest integer.
pub fn ceil(value: f64) -> f64 {
    value.ceil()
}

/// Modulo operation (`value % divisor`).
pub fn modulo(value: f64, divisor: f64) -> f64 {
    value % divisor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(10.0, 5.0), 15.0);
    }

    #[test]
    fn test_subtract() {
        assert_eq!(subtract(10.0, 3.0), 7.0);
    }

    #[test]
    fn test_multiply() {
        assert_eq!(multiply(4.0, 2.5), 10.0);
    }

    #[test]
    fn test_divide() {
        assert!((divide(10.0, 3.0).unwrap() - 3.3333333).abs() < 0.001);
    }

    #[test]
    fn test_divide_by_zero() {
        assert!(divide(10.0, 0.0).is_err());
    }

    #[test]
    fn test_round_to() {
        assert_eq!(round_to(3.14159, 2), 3.14);
    }

    #[test]
    fn test_abs_value() {
        assert_eq!(abs_value(-42.0), 42.0);
    }

    #[test]
    fn test_power() {
        assert_eq!(power(2.0, 3.0), 8.0);
    }

    #[test]
    fn test_sqrt() {
        assert_eq!(sqrt(16.0).unwrap(), 4.0);
    }

    #[test]
    fn test_sqrt_negative() {
        assert!(sqrt(-1.0).is_err());
    }

    #[test]
    fn test_floor() {
        assert_eq!(floor(3.7), 3.0);
    }

    #[test]
    fn test_ceil() {
        assert_eq!(ceil(3.2), 4.0);
    }

    #[test]
    fn test_modulo() {
        assert_eq!(modulo(10.0, 3.0), 1.0);
    }
}
