//! Wrapper type for `etl::types::Cell` that adds primitive conversions.

use anyhow::anyhow;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::PgNumeric;
use trait_gen::trait_gen;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, derive_more::TryInto)]
#[try_into(owned, ref, ref_mut)]
pub enum Cell {
    #[try_into(ignore)]
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(PgNumeric),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    #[try_into(ignore)]
    Array(ArrayCell),
}

impl TryFrom<etl::types::Cell> for Cell {
    type Error = anyhow::Error;

    fn try_from(cell: etl::types::Cell) -> Result<Self, Self::Error> {
        let inner = match cell {
            etl::types::Cell::Null => Cell::Null,
            etl::types::Cell::Bool(bool) => Cell::Bool(bool),
            etl::types::Cell::String(string) => Cell::String(string),
            etl::types::Cell::I16(i16) => Cell::I16(i16),
            etl::types::Cell::I32(i32) => Cell::I32(i32),
            etl::types::Cell::U32(u32) => Cell::U32(u32),
            etl::types::Cell::I64(i64) => Cell::I64(i64),
            etl::types::Cell::F32(f32) => Cell::F32(f32),
            etl::types::Cell::F64(f64) => Cell::F64(f64),
            etl::types::Cell::Numeric(numeric) => Cell::Numeric(numeric),
            etl::types::Cell::Date(date) => Cell::Date(date),
            etl::types::Cell::Time(time) => Cell::Time(time),
            etl::types::Cell::Timestamp(timestamp) => Cell::Timestamp(timestamp),
            etl::types::Cell::TimestampTz(timestamp_tz) => {
                Cell::TimestampTz(timestamp_tz)
            }
            etl::types::Cell::Uuid(uuid) => Cell::Uuid(uuid),
            etl::types::Cell::Json(json) => Cell::Json(json),
            etl::types::Cell::Bytes(bytes) => Cell::Bytes(bytes),
            etl::types::Cell::Array(array) => Cell::Array(array.try_into()?),
        };
        Ok(inner)
    }
}

#[derive(Debug, Clone, PartialEq, derive_more::TryInto)]
#[try_into(owned, ref, ref_mut)]
pub enum ArrayCell {
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    U32(Vec<Option<u32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Numeric(Vec<Option<PgNumeric>>),
    Date(Vec<Option<NaiveDate>>),
    Time(Vec<Option<NaiveTime>>),
    Timestamp(Vec<Option<NaiveDateTime>>),
    TimestampTz(Vec<Option<DateTime<Utc>>>),
    Uuid(Vec<Option<Uuid>>),
    Json(Vec<Option<serde_json::Value>>),
    Bytes(Vec<Option<Vec<u8>>>),
}

impl TryFrom<etl::types::ArrayCell> for ArrayCell {
    type Error = anyhow::Error;

    fn try_from(array: etl::types::ArrayCell) -> Result<Self, Self::Error> {
        let inner = match array {
            etl::types::ArrayCell::Bool(bool) => ArrayCell::Bool(bool),
            etl::types::ArrayCell::String(string) => ArrayCell::String(string),
            etl::types::ArrayCell::I16(i16) => ArrayCell::I16(i16),
            etl::types::ArrayCell::I32(i32) => ArrayCell::I32(i32),
            etl::types::ArrayCell::U32(u32) => ArrayCell::U32(u32),
            etl::types::ArrayCell::I64(i64) => ArrayCell::I64(i64),
            etl::types::ArrayCell::F32(f32) => ArrayCell::F32(f32),
            etl::types::ArrayCell::F64(f64) => ArrayCell::F64(f64),
            etl::types::ArrayCell::Numeric(numeric) => ArrayCell::Numeric(numeric),
            etl::types::ArrayCell::Date(date) => ArrayCell::Date(date),
            etl::types::ArrayCell::Time(time) => ArrayCell::Time(time),
            etl::types::ArrayCell::Timestamp(timestamp) => {
                ArrayCell::Timestamp(timestamp)
            }
            etl::types::ArrayCell::TimestampTz(timestamp_tz) => {
                ArrayCell::TimestampTz(timestamp_tz)
            }
            etl::types::ArrayCell::Uuid(uuid) => ArrayCell::Uuid(uuid),
            etl::types::ArrayCell::Json(json) => ArrayCell::Json(json),
            etl::types::ArrayCell::Bytes(bytes) => ArrayCell::Bytes(bytes),
        };
        Ok(inner)
    }
}

// TryFrom<Cell> for Option<T> implementations.
// These handle Cell::Null by returning None, and extract the inner value otherwise.
#[trait_gen(T -> bool, String, i16, i32, u32, i64, f32, f64, PgNumeric, NaiveDate, NaiveTime, NaiveDateTime, DateTime<Utc>, Uuid, serde_json::Value, Vec<u8>)]
impl TryFrom<Cell> for Option<T> {
    type Error = anyhow::Error;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(None),
            other => {
                let value: T = other.try_into().map_err(|e| {
                    anyhow!("Failed to convert Cell to expected type: {e}")
                })?;
                Ok(Some(value))
            }
        }
    }
}

// TryFrom<Cell> for Vec<Option<T>> implementations.
// These extract array contents from Cell::Array variants.
#[trait_gen(T -> bool, String, i16, i32, u32, i64, f32, f64, PgNumeric, NaiveDate, NaiveTime, NaiveDateTime, DateTime<Utc>, Uuid, serde_json::Value, Vec<u8>)]
impl TryFrom<Cell> for Vec<Option<T>> {
    type Error = anyhow::Error;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Array(array_cell) => {
                let vec: Vec<Option<T>> = array_cell.try_into().map_err(|e| {
                    anyhow!("Failed to convert ArrayCell to expected type: {e}")
                })?;
                Ok(vec)
            }
            _ => Err(anyhow!("Expected Cell::Array, but got {:?}", cell)),
        }
    }
}

#[cfg(feature = "rust_decimal")]
impl TryFrom<Cell> for rust_decimal::Decimal {
    type Error = crate::numeric::PgNumericConversionError;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Numeric(numeric) => crate::numeric::numeric_to_decimal(&numeric),
            _ => Err(crate::numeric::PgNumericConversionError::MalformedValue),
        }
    }
}

#[cfg(feature = "rust_decimal")]
impl TryFrom<Cell> for Option<rust_decimal::Decimal> {
    type Error = crate::numeric::PgNumericConversionError;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(None),
            Cell::Numeric(numeric) => {
                crate::numeric::numeric_to_decimal(&numeric).map(Some)
            }
            _ => Err(crate::numeric::PgNumericConversionError::MalformedValue),
        }
    }
}

// TODO: add ArrayCell implementations for rust_decimal

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_try_from_etl_cell_scalars() {
        // Null
        let cell: Cell = etl::types::Cell::Null.try_into().unwrap();
        assert_eq!(cell, Cell::Null);

        // Bool
        let cell: Cell = etl::types::Cell::Bool(true).try_into().unwrap();
        assert_eq!(cell, Cell::Bool(true));

        // String
        let cell: Cell =
            etl::types::Cell::String("hello".to_string()).try_into().unwrap();
        assert_eq!(cell, Cell::String("hello".to_string()));

        // I16
        let cell: Cell = etl::types::Cell::I16(42).try_into().unwrap();
        assert_eq!(cell, Cell::I16(42));

        // I32
        let cell: Cell = etl::types::Cell::I32(100).try_into().unwrap();
        assert_eq!(cell, Cell::I32(100));

        // U32
        let cell: Cell = etl::types::Cell::U32(200).try_into().unwrap();
        assert_eq!(cell, Cell::U32(200));

        // I64
        let cell: Cell = etl::types::Cell::I64(300).try_into().unwrap();
        assert_eq!(cell, Cell::I64(300));

        // F32
        let cell: Cell = etl::types::Cell::F32(1.5).try_into().unwrap();
        assert_eq!(cell, Cell::F32(1.5));

        // F64
        let cell: Cell = etl::types::Cell::F64(2.5).try_into().unwrap();
        assert_eq!(cell, Cell::F64(2.5));

        // Uuid
        let u = Uuid::new_v4();
        let cell: Cell = etl::types::Cell::Uuid(u).try_into().unwrap();
        assert_eq!(cell, Cell::Uuid(u));

        // Json
        let json = serde_json::json!({"key": "value"});
        let cell: Cell = etl::types::Cell::Json(json.clone()).try_into().unwrap();
        assert_eq!(cell, Cell::Json(json));

        // Bytes
        let cell: Cell =
            etl::types::Cell::Bytes(vec![1, 2, 3]).try_into().unwrap();
        assert_eq!(cell, Cell::Bytes(vec![1, 2, 3]));
    }

    #[test]
    fn test_cell_try_from_etl_cell_date_time() {
        let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let cell: Cell = etl::types::Cell::Date(date).try_into().unwrap();
        assert_eq!(cell, Cell::Date(date));

        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let cell: Cell = etl::types::Cell::Time(time).try_into().unwrap();
        assert_eq!(cell, Cell::Time(time));

        let ts = NaiveDate::from_ymd_opt(2024, 3, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap();
        let cell: Cell = etl::types::Cell::Timestamp(ts).try_into().unwrap();
        assert_eq!(cell, Cell::Timestamp(ts));

        let tstz: DateTime<Utc> = "2024-03-15T12:30:45Z".parse().unwrap();
        let cell: Cell =
            etl::types::Cell::TimestampTz(tstz).try_into().unwrap();
        assert_eq!(cell, Cell::TimestampTz(tstz));
    }

    #[test]
    fn test_cell_try_from_etl_cell_numeric() {
        let numeric = PgNumeric::NaN;
        let cell: Cell =
            etl::types::Cell::Numeric(numeric.clone()).try_into().unwrap();
        assert_eq!(cell, Cell::Numeric(numeric));
    }

    #[test]
    fn test_cell_try_from_etl_cell_array() {
        let arr = etl::types::ArrayCell::I32(vec![Some(1), None, Some(3)]);
        let cell: Cell = etl::types::Cell::Array(arr).try_into().unwrap();
        assert_eq!(
            cell,
            Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)]))
        );
    }

    #[test]
    fn test_array_cell_try_from_etl_array_cell() {
        let arr: ArrayCell = etl::types::ArrayCell::Bool(vec![Some(true), None])
            .try_into()
            .unwrap();
        assert_eq!(arr, ArrayCell::Bool(vec![Some(true), None]));

        let arr: ArrayCell =
            etl::types::ArrayCell::String(vec![Some("a".to_string()), None])
                .try_into()
                .unwrap();
        assert_eq!(arr, ArrayCell::String(vec![Some("a".to_string()), None]));

        let arr: ArrayCell =
            etl::types::ArrayCell::I16(vec![Some(1)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::I16(vec![Some(1)]));

        let arr: ArrayCell =
            etl::types::ArrayCell::U32(vec![Some(42)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::U32(vec![Some(42)]));

        let arr: ArrayCell =
            etl::types::ArrayCell::I64(vec![Some(999)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::I64(vec![Some(999)]));

        let arr: ArrayCell =
            etl::types::ArrayCell::F32(vec![Some(1.0)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::F32(vec![Some(1.0)]));

        let arr: ArrayCell =
            etl::types::ArrayCell::F64(vec![Some(2.0)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::F64(vec![Some(2.0)]));

        let arr: ArrayCell =
            etl::types::ArrayCell::Numeric(vec![Some(PgNumeric::NaN)])
                .try_into()
                .unwrap();
        assert_eq!(arr, ArrayCell::Numeric(vec![Some(PgNumeric::NaN)]));

        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let arr: ArrayCell =
            etl::types::ArrayCell::Date(vec![Some(date)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::Date(vec![Some(date)]));

        let time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let arr: ArrayCell =
            etl::types::ArrayCell::Time(vec![Some(time)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::Time(vec![Some(time)]));

        let ts = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let arr: ArrayCell = etl::types::ArrayCell::Timestamp(vec![Some(ts)])
            .try_into()
            .unwrap();
        assert_eq!(arr, ArrayCell::Timestamp(vec![Some(ts)]));

        let tstz: DateTime<Utc> = "2024-01-01T12:00:00Z".parse().unwrap();
        let arr: ArrayCell =
            etl::types::ArrayCell::TimestampTz(vec![Some(tstz)])
                .try_into()
                .unwrap();
        assert_eq!(arr, ArrayCell::TimestampTz(vec![Some(tstz)]));

        let u = Uuid::new_v4();
        let arr: ArrayCell =
            etl::types::ArrayCell::Uuid(vec![Some(u)]).try_into().unwrap();
        assert_eq!(arr, ArrayCell::Uuid(vec![Some(u)]));

        let json = serde_json::json!({"key": "value"});
        let arr: ArrayCell =
            etl::types::ArrayCell::Json(vec![Some(json.clone())])
                .try_into()
                .unwrap();
        assert_eq!(arr, ArrayCell::Json(vec![Some(json)]));

        let arr: ArrayCell =
            etl::types::ArrayCell::Bytes(vec![Some(vec![1, 2])])
                .try_into()
                .unwrap();
        assert_eq!(arr, ArrayCell::Bytes(vec![Some(vec![1, 2])]));
    }

    #[test]
    fn test_option_from_cell_null() {
        let opt: Option<bool> = Cell::Null.try_into().unwrap();
        assert_eq!(opt, None);

        let opt: Option<String> = Cell::Null.try_into().unwrap();
        assert_eq!(opt, None);

        let opt: Option<i32> = Cell::Null.try_into().unwrap();
        assert_eq!(opt, None);

        let opt: Option<i64> = Cell::Null.try_into().unwrap();
        assert_eq!(opt, None);
    }

    #[test]
    fn test_option_from_cell_value() {
        let opt: Option<bool> = Cell::Bool(true).try_into().unwrap();
        assert_eq!(opt, Some(true));

        let opt: Option<String> =
            Cell::String("test".to_string()).try_into().unwrap();
        assert_eq!(opt, Some("test".to_string()));

        let opt: Option<i16> = Cell::I16(5).try_into().unwrap();
        assert_eq!(opt, Some(5));

        let opt: Option<i32> = Cell::I32(42).try_into().unwrap();
        assert_eq!(opt, Some(42));

        let opt: Option<u32> = Cell::U32(99).try_into().unwrap();
        assert_eq!(opt, Some(99));

        let opt: Option<i64> = Cell::I64(100).try_into().unwrap();
        assert_eq!(opt, Some(100));

        let opt: Option<f32> = Cell::F32(1.5).try_into().unwrap();
        assert_eq!(opt, Some(1.5));

        let opt: Option<f64> = Cell::F64(2.5).try_into().unwrap();
        assert_eq!(opt, Some(2.5));
    }

    #[test]
    fn test_option_from_cell_wrong_type() {
        let result: Result<Option<bool>, _> = Cell::I32(42).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_vec_option_from_cell_array() {
        let cell =
            Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)]));
        let vec: Vec<Option<i32>> = cell.try_into().unwrap();
        assert_eq!(vec, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_vec_option_from_cell_not_array() {
        let result: Result<Vec<Option<i32>>, _> = Cell::I32(42).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_vec_option_from_cell_wrong_array_type() {
        let cell = Cell::Array(ArrayCell::Bool(vec![Some(true)]));
        let result: Result<Vec<Option<i32>>, _> = cell.try_into();
        assert!(result.is_err());
    }

    #[cfg(feature = "rust_decimal")]
    mod rust_decimal_tests {
        use super::*;

        #[test]
        fn test_decimal_from_cell_numeric() {
            let numeric = etl::types::PgNumeric::Value {
                sign: etl::types::Sign::Positive,
                weight: 0,
                scale: 2,
                digits: vec![1234],
            };
            let dec: rust_decimal::Decimal =
                Cell::Numeric(numeric).try_into().unwrap();
            assert!(dec > rust_decimal::Decimal::ZERO);
        }

        #[test]
        fn test_decimal_from_cell_wrong_type() {
            let result: Result<
                rust_decimal::Decimal,
                crate::numeric::PgNumericConversionError,
            > = Cell::I32(42).try_into();
            assert!(result.is_err());
        }

        #[test]
        fn test_option_decimal_from_cell_null() {
            let opt: Option<rust_decimal::Decimal> =
                Cell::Null.try_into().unwrap();
            assert_eq!(opt, None);
        }

        #[test]
        fn test_option_decimal_from_cell_numeric() {
            let numeric = etl::types::PgNumeric::Value {
                sign: etl::types::Sign::Positive,
                weight: 0,
                scale: 0,
                digits: vec![42],
            };
            let opt: Option<rust_decimal::Decimal> =
                Cell::Numeric(numeric).try_into().unwrap();
            assert!(opt.is_some());
        }

        #[test]
        fn test_option_decimal_from_cell_wrong_type() {
            let result: Result<
                Option<rust_decimal::Decimal>,
                crate::numeric::PgNumericConversionError,
            > = Cell::I32(42).try_into();
            assert!(result.is_err());
        }
    }
}
