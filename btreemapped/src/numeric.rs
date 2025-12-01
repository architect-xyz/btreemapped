//! Wrapper type for `etl::types::PgNumeric` that adds the following optional
//! functionality:
//!
//! - rust_decimal::Decimal conversion
//! - sqlx::Encode and sqlx::Decode
//!
//! https://github.com/launchbadge/sqlx/blob/main/sqlx-postgres/src/types/rust_decimal.rs

use postgres_types::IsNull;
use tokio_postgres::types::{FromSql, ToSql, Type};

type BoxDynError = Box<dyn std::error::Error + Sync + Send>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct PgNumeric(pub(crate) etl::types::PgNumeric);

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, BoxDynError> {
        <etl::types::PgNumeric as FromSql<'a>>::from_sql(ty, raw).map(PgNumeric)
    }

    fn accepts(ty: &Type) -> bool {
        <etl::types::PgNumeric as FromSql<'a>>::accepts(ty)
    }
}

impl ToSql for PgNumeric {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, BoxDynError> {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <etl::types::PgNumeric as ToSql>::accepts(ty)
    }

    tokio_postgres::types::to_sql_checked!();
}

#[cfg(all(feature = "sqlx", feature = "rust_decimal"))]
impl sqlx::Type<sqlx::Postgres> for PgNumeric {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <rust_decimal::Decimal as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(all(feature = "sqlx", feature = "rust_decimal"))]
impl sqlx::postgres::PgHasArrayType for PgNumeric {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <rust_decimal::Decimal as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Encode<'_, sqlx::Postgres> for PgNumeric {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        const POSITIVE_SIGN: u16 = 0x0000;
        const NEGATIVE_SIGN: u16 = 0x4000;
        const NAN_SIGN: u16 = 0xC000; // NUMERIC_NAN
        const POSITIVE_INFINITY_SIGN: u16 = 0xD000; // NUMERIC_PINF
        const NEGATIVE_INFINITY_SIGN: u16 = 0xF000; // NUMERIC_NINF

        let (sign, weight, scale, digits) = match &self.0 {
            etl::types::PgNumeric::NaN => (NAN_SIGN, &0i16, &0u16, &vec![]),
            etl::types::PgNumeric::PositiveInfinity => {
                (POSITIVE_INFINITY_SIGN, &0i16, &0u16, &vec![])
            }
            etl::types::PgNumeric::NegativeInfinity => {
                (NEGATIVE_INFINITY_SIGN, &0i16, &0u16, &vec![])
            }
            etl::types::PgNumeric::Value {
                sign: etl::types::Sign::Positive,
                weight,
                scale,
                digits,
            } => (POSITIVE_SIGN, weight, scale, digits),
            etl::types::PgNumeric::Value {
                sign: etl::types::Sign::Negative,
                weight,
                scale,
                digits,
            } => (NEGATIVE_SIGN, weight, scale, digits),
        };

        let num_digits: u16 = digits.len().try_into()?;
        buf.extend_from_slice(&num_digits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&scale.to_be_bytes());

        for digit in digits {
            buf.extend_from_slice(&digit.to_be_bytes());
        }

        Ok(sqlx::encode::IsNull::No)
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Decode<'_, sqlx::Postgres> for PgNumeric {
    fn decode(
        value: sqlx::postgres::PgValueRef<'_>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        match value.format() {
            sqlx::postgres::PgValueFormat::Binary => {
                let inner = etl::types::PgNumeric::from_sql(
                    &postgres_types::Type::NUMERIC, // NB alee: this is arbitrary, callee ignore
                    value.as_bytes()?,
                )?;
                Ok(PgNumeric(inner))
            }
            sqlx::postgres::PgValueFormat::Text => {
                let inner = value.as_str()?.parse::<etl::types::PgNumeric>()?;
                Ok(PgNumeric(inner))
            }
        }
    }
}

#[cfg(feature = "rust_decimal")]
impl TryFrom<PgNumeric> for rust_decimal::Decimal {
    type Error = BoxDynError;

    fn try_from(numeric: PgNumeric) -> Result<Self, BoxDynError> {
        rust_decimal::Decimal::try_from(&numeric)
    }
}

#[cfg(feature = "rust_decimal")]
impl TryFrom<&'_ PgNumeric> for rust_decimal::Decimal {
    type Error = BoxDynError;

    fn try_from(numeric: &'_ PgNumeric) -> Result<Self, BoxDynError> {
        use rust_decimal::MathematicalOps;

        let (digits, sign, mut weight, scale) = match &numeric.0 {
            etl::types::PgNumeric::Value { ref digits, sign, weight, scale } => {
                (digits, sign, *weight, *scale)
            }
            etl::types::PgNumeric::NaN => {
                return Err("Decimal does not support NaN".into());
            }
            etl::types::PgNumeric::PositiveInfinity => {
                return Err("Decimal does not support +Infinity".into());
            }
            etl::types::PgNumeric::NegativeInfinity => {
                return Err("Decimal does not support -Infinity".into());
            }
        };

        if digits.is_empty() {
            // Postgres returns an empty digit array for 0
            return Ok(rust_decimal::Decimal::ZERO);
        }

        let scale = u32::try_from(scale)
            .map_err(|_| format!("invalid scale value for Pg NUMERIC: {scale}"))?;

        let mut value = rust_decimal::Decimal::ZERO;

        // Sum over `digits`, multiply each by its weight and add it to `value`.
        for &digit in digits {
            let mul = rust_decimal::Decimal::from(10_000i16)
                .checked_powi(weight as i64)
                .ok_or("value not representable as rust_decimal::Decimal")?;

            let part = rust_decimal::Decimal::from(digit)
                .checked_mul(mul)
                .ok_or("value not representable as rust_decimal::Decimal")?;

            value = value
                .checked_add(part)
                .ok_or("value not representable as rust_decimal::Decimal")?;

            weight = weight.checked_sub(1).ok_or("weight underflowed")?;
        }

        match sign {
            etl::types::Sign::Positive => value.set_sign_positive(true),
            etl::types::Sign::Negative => value.set_sign_negative(true),
        }

        value.rescale(scale);

        Ok(value)
    }
}

#[cfg(feature = "rust_decimal")]
impl From<rust_decimal::Decimal> for PgNumeric {
    fn from(value: rust_decimal::Decimal) -> Self {
        PgNumeric::from(&value)
    }
}

#[cfg(feature = "rust_decimal")]
impl From<&'_ rust_decimal::Decimal> for PgNumeric {
    // Impl has been manually validated.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn from(decimal: &rust_decimal::Decimal) -> Self {
        if rust_decimal::Decimal::is_zero(decimal) {
            return PgNumeric(etl::types::PgNumeric::Value {
                sign: etl::types::Sign::Positive,
                digits: vec![],
                weight: 0,
                scale: 0,
            });
        }

        assert!(
            (0u32..=28).contains(&decimal.scale()),
            "decimal scale out of range {:?}",
            decimal.unpack(),
        );

        // Cannot overflow: always in the range [0, 28]
        let scale = decimal.scale() as u16;

        let mut mantissa = decimal.mantissa().unsigned_abs();

        // If our scale is not a multiple of 4, we need to go to the next multiple.
        let groups_diff = scale % 4;
        if groups_diff > 0 {
            let remainder = 4 - groups_diff as u32;
            let power = 10u32.pow(remainder) as u128;

            // Impossible to overflow; 0 <= mantissa <= 2^96,
            // and we're multiplying by at most 1,000 (giving us a result < 2^106)
            mantissa *= power;
        }

        // Array to store max mantissa of Decimal in Postgres decimal format.
        let mut digits = Vec::with_capacity(8);

        // Convert to base-10000.
        while mantissa != 0 {
            // Cannot overflow or wrap because of the modulus
            digits.push((mantissa % 10_000) as i16);
            mantissa /= 10_000;
        }

        // We started with the low digits first, but they should actually be at the end.
        digits.reverse();

        // Cannot overflow: strictly smaller than `scale`.
        let digits_after_decimal = scale.div_ceil(4) as i16;

        // `mantissa` contains at most 29 decimal digits (log10(2^96)),
        // split into at most 8 4-digit segments.
        assert!(
            digits.len() <= 8,
            "digits.len() out of range: {}; unpacked: {:?}",
            digits.len(),
            decimal.unpack()
        );

        // Cannot overflow; at most 8
        let num_digits = digits.len() as i16;

        // Find how many 4-digit segments should go before the decimal point.
        // `weight = 0` puts just `digit[0]` before the decimal point, and the rest after.
        let weight = num_digits - digits_after_decimal - 1;

        // Remove non-significant zeroes.
        while let Some(&0) = digits.last() {
            digits.pop();
        }

        PgNumeric(etl::types::PgNumeric::Value {
            sign: match decimal.is_sign_negative() {
                false => etl::types::Sign::Positive,
                true => etl::types::Sign::Negative,
            },
            // Cannot overflow; between 0 and 28
            scale,
            weight,
            digits,
        })
    }
}

#[cfg(all(test, feature = "rust_decimal"))]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::convert::TryFrom;

    #[test]
    fn test_simple_integer_conversion() {
        let d = dec!(123);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_negative_integer() {
        let d = dec!(-456);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_decimal_with_scale() {
        let d = dec!(123.45);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_small_decimal() {
        let d = dec!(0.0001);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_large_number() {
        let d = dec!(999999999999.999999);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_zero() {
        let d = dec!(0);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_negative_decimal() {
        let d = dec!(-0.123456);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(pg).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_nan_conversion_fails() {
        let pg = PgNumeric(etl::types::PgNumeric::NaN);
        let result = rust_decimal::Decimal::try_from(&pg);
        assert!(result.is_err());
    }

    #[test]
    fn test_positive_infinity_conversion_fails() {
        let pg = PgNumeric(etl::types::PgNumeric::PositiveInfinity);
        let result = rust_decimal::Decimal::try_from(&pg);
        assert!(result.is_err());
    }

    #[test]
    fn test_negative_infinity_conversion_fails() {
        let pg = PgNumeric(etl::types::PgNumeric::NegativeInfinity);
        let result = rust_decimal::Decimal::try_from(&pg);
        assert!(result.is_err());
    }

    #[test]
    fn test_reference_conversions() {
        let d = dec!(42.5);
        let pg = PgNumeric::try_from(d).unwrap();
        let back = rust_decimal::Decimal::try_from(&pg).unwrap();
        assert_eq!(d, back);
    }
}
