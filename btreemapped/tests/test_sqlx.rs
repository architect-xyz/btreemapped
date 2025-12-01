#![cfg(feature = "sqlx")]

mod utils;

use anyhow::Result;
use btreemapped::{PgJson, PgNumeric};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::BTreeMap;
use utils::setup_postgres_container;

#[derive(Debug, Clone, sqlx::FromRow, PartialEq)]
struct JsonRecord {
    data1: PgJson<BTreeMap<String, String>>,
    data2: PgJson<BTreeMap<String, Decimal>>,
    data3: PgJson<BTreeMap<String, i32>>,
    data4: PgNumeric,
}

async fn setup_table(pool: &PgPool) -> Result<()> {
    #[rustfmt::skip]
    sqlx::query(r#"
        CREATE TABLE json_records (
            id SERIAL PRIMARY KEY,
            data1 JSONB,
            data2 JSONB,
            data3 JSON,
            data4 NUMERIC
        )"#
    ).execute(pool).await?;

    Ok(())
}

async fn insert_test_data(pool: &PgPool) -> Result<()> {
    #[rustfmt::skip]
    sqlx::query(r#"
        INSERT INTO json_records (data1, data2, data3, data4)
        VALUES (
            '{"str_key": "str_value"}', 
            '{"dec_key": 1.234}',
            '{"num_key": 123}',
            '123.45'::NUMERIC
        )
    "#).execute(pool).await?;

    Ok(())
}

#[tokio::test]
async fn test_sqlx_decode() -> Result<()> {
    let (_container, port) = setup_postgres_container().await?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!("postgres://postgres:postgres@localhost:{port}/testdb"))
        .await?;

    setup_table(&pool).await?;
    insert_test_data(&pool).await?;

    let mut records = sqlx::query_as::<_, JsonRecord>("SELECT * FROM json_records")
        .fetch_all(&pool)
        .await?;

    assert_eq!(1, records.len());
    let record = records.pop().unwrap();

    assert_eq!(*record.data1.get("str_key").unwrap(), "str_value".to_string());
    assert_eq!(*record.data2.get("dec_key").unwrap(), Decimal::from_f64(1.234).unwrap());
    assert_eq!(*record.data3.get("num_key").unwrap(), 123);

    Ok(())
}

#[tokio::test]
async fn test_sqlx_encode() -> Result<()> {
    let (_container, port) = setup_postgres_container().await?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!("postgres://postgres:postgres@localhost:{port}/testdb"))
        .await?;

    setup_table(&pool).await?;

    let records = vec![
        JsonRecord {
            data1: PgJson(BTreeMap::new()),
            data2: PgJson(BTreeMap::from_iter(
                [("abc".to_string(), Decimal::ZERO)].into_iter(),
            )),
            data3: PgJson(BTreeMap::new()),
            data4: dec!(123.45).into(),
        },
        JsonRecord {
            data1: PgJson(BTreeMap::from_iter(
                [
                    ("abc".to_string(), "def".to_string()),
                    ("123".to_string(), "456".to_string()),
                ]
                .into_iter(),
            )),
            data2: PgJson(BTreeMap::new()),
            data3: PgJson(BTreeMap::from_iter(
                [("aaa".to_string(), 123), ("bbb".to_string(), 456)].into_iter(),
            )),
            data4: dec!(678.90).into(),
        },
    ];

    for record in &records {
        sqlx::query(
            "INSERT INTO json_records (data1, data2, data3, data4) VALUES ($1, $2, $3, $4)",
        )
        .bind(&record.data1)
        .bind(&record.data2)
        .bind(&record.data3)
        .bind(&record.data4)
        .execute(&pool)
        .await?;
    }

    let queried_records =
        sqlx::query_as::<_, JsonRecord>("SELECT * FROM json_records ORDER BY id ASC")
            .fetch_all(&pool)
            .await?;

    assert_eq!(records, queried_records);

    Ok(())
}
