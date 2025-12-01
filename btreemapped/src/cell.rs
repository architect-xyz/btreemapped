//! Wrapper type for `etl::types::Cell` that adds primitive conversions.

use crate::numeric::PgNumeric;
use anyhow::anyhow;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
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

impl From<etl::types::Cell> for Cell {
    fn from(cell: etl::types::Cell) -> Self {
        match cell {
            etl::types::Cell::Null => Cell::Null,
            etl::types::Cell::Bool(bool) => Cell::Bool(bool),
            etl::types::Cell::String(string) => Cell::String(string),
            etl::types::Cell::I16(i16) => Cell::I16(i16),
            etl::types::Cell::I32(i32) => Cell::I32(i32),
            etl::types::Cell::U32(u32) => Cell::U32(u32),
            etl::types::Cell::I64(i64) => Cell::I64(i64),
            etl::types::Cell::F32(f32) => Cell::F32(f32),
            etl::types::Cell::F64(f64) => Cell::F64(f64),
            etl::types::Cell::Numeric(numeric) => Cell::Numeric(PgNumeric(numeric)),
            etl::types::Cell::Date(date) => Cell::Date(date),
            etl::types::Cell::Time(time) => Cell::Time(time),
            etl::types::Cell::Timestamp(timestamp) => Cell::Timestamp(timestamp),
            etl::types::Cell::TimestampTz(timestamp_tz) => {
                Cell::TimestampTz(timestamp_tz)
            }
            etl::types::Cell::Uuid(uuid) => Cell::Uuid(uuid),
            etl::types::Cell::Json(json) => Cell::Json(json),
            etl::types::Cell::Bytes(bytes) => Cell::Bytes(bytes),
            etl::types::Cell::Array(array) => Cell::Array(array.into()),
        }
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

impl From<etl::types::ArrayCell> for ArrayCell {
    fn from(array: etl::types::ArrayCell) -> Self {
        match array {
            etl::types::ArrayCell::Bool(bool) => ArrayCell::Bool(bool),
            etl::types::ArrayCell::String(string) => ArrayCell::String(string),
            etl::types::ArrayCell::I16(i16) => ArrayCell::I16(i16),
            etl::types::ArrayCell::I32(i32) => ArrayCell::I32(i32),
            etl::types::ArrayCell::U32(u32) => ArrayCell::U32(u32),
            etl::types::ArrayCell::I64(i64) => ArrayCell::I64(i64),
            etl::types::ArrayCell::F32(f32) => ArrayCell::F32(f32),
            etl::types::ArrayCell::F64(f64) => ArrayCell::F64(f64),
            etl::types::ArrayCell::Numeric(numeric) => {
                // CR alee: we can avoid this allocation with the following
                // magic [1], assuming PgNumeric is still #[repr(transparent)]
                //
                // unsafe fn transmute_vec<T, U>(v: Vec<T>) -> Vec<U> {
                //   unsafe {
                //     let len = v.len();
                //     let capacity = v.capacity();
                //     let ptr = v.as_mut_ptr();
                //     std::mem::forget(v);
                //     Vec::from_raw_parts(ptr as *mut &T, len, capacity)
                //   }
                // }
                //
                // I don't care enough to take this risk yet though.
                //
                // [1] https://www.reddit.com/r/rust/comments/1ez6f2c/any_safe_way_to_transmute_vecoptionfoo_to_vecfoo/
                ArrayCell::Numeric(
                    numeric.into_iter().map(|n| n.map(PgNumeric)).collect(),
                )
            }
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
        }
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
