//! Conversion utilities between `etl::types::PgNumeric` and `rust_decimal::Decimal`.

use etl::types::PgNumeric;
use rust_decimal::Decimal;
use std::str::FromStr;

/// Error type for PgNumeric to Decimal conversion failures.
#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum PgNumericConversionError {
    /// The PgNumeric value is NaN, which cannot be represented as a Decimal.
    #[error("NaN cannot be converted to Decimal")]
    NaN,
    /// The PgNumeric value is positive infinity, which cannot be represented as a Decimal.
    #[error("Positive infinity cannot be converted to Decimal")]
    PositiveInfinity,
    /// The PgNumeric value is negative infinity, which cannot be represented as a Decimal.
    #[error("Negative infinity cannot be converted to Decimal")]
    NegativeInfinity,
    /// The value is out of range for Decimal representation.
    #[error("Value out of range for Decimal")]
    OutOfRange,
    /// Failed to parse the string representation.
    #[error("Failed to parse numeric: {0}")]
    ParseError(String),
}

/// Convert a `PgNumeric` to a `Decimal`.
///
/// Returns an error if the PgNumeric is NaN, infinity, or out of range for Decimal.
pub fn pg_numeric_to_decimal(
    pg: &PgNumeric,
) -> Result<Decimal, PgNumericConversionError> {
    // Use Display implementation to convert to string, then parse
    let s = pg.to_string();
    match s.as_str() {
        "NaN" => Err(PgNumericConversionError::NaN),
        "Infinity" => Err(PgNumericConversionError::PositiveInfinity),
        "-Infinity" => Err(PgNumericConversionError::NegativeInfinity),
        _ => Decimal::from_str(&s)
            .map_err(|e| PgNumericConversionError::ParseError(e.to_string())),
    }
}

/// Convert a `Decimal` to a `PgNumeric`.
///
/// This conversion is infallible since all Decimal values can be represented as PgNumeric.
pub fn decimal_to_pg_numeric(d: &Decimal) -> PgNumeric {
    // Use Display implementation to convert to string, then parse
    let s = d.to_string();
    // PgNumeric::from_str should never fail for valid Decimal strings
    PgNumeric::from_str(&s).expect("Decimal string should always be valid for PgNumeric")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_simple_integer_conversion() {
        let d = dec!(123);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_negative_integer() {
        let d = dec!(-456);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_decimal_with_scale() {
        let d = dec!(123.45);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_small_decimal() {
        let d = dec!(0.0001);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_large_number() {
        let d = dec!(999999999999.999999);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_zero() {
        let d = dec!(0);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_negative_decimal() {
        let d = dec!(-0.123456);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_nan_conversion_fails() {
        let pg = PgNumeric::from_str("NaN").unwrap();
        let result = pg_numeric_to_decimal(&pg);
        assert!(matches!(result, Err(PgNumericConversionError::NaN)));
    }

    #[test]
    fn test_positive_infinity_conversion_fails() {
        let pg = PgNumeric::from_str("Infinity").unwrap();
        let result = pg_numeric_to_decimal(&pg);
        assert!(matches!(result, Err(PgNumericConversionError::PositiveInfinity)));
    }

    #[test]
    fn test_negative_infinity_conversion_fails() {
        let pg = PgNumeric::from_str("-Infinity").unwrap();
        let result = pg_numeric_to_decimal(&pg);
        assert!(matches!(result, Err(PgNumericConversionError::NegativeInfinity)));
    }

    #[test]
    fn test_reference_conversions() {
        let d = dec!(42.5);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_max_precision() {
        // Test a number with many decimal places
        let d = dec!(1.23456789012345678901234567890);
        let pg = decimal_to_pg_numeric(&d);
        let back = pg_numeric_to_decimal(&pg).unwrap();
        // Note: precision may be limited by Decimal's max scale (28)
        assert_eq!(d, back);
    }
}
