use anyhow::Result;
use btreemapped::{BTreeMapSink, BTreeMapped, Json, LIndex1, PgSchema};
use btreemapped_derive::{BTreeMapped, PgSchema};
use pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use postgres_types::Type;
use serde::Serialize;
use std::collections::BTreeMap;
use utils::{create_postgres_client, setup_postgres_container};

mod utils;

#[derive(Debug, Clone, Serialize, BTreeMapped, PgSchema)]
#[btreemap(index = ["id"])]
pub struct JsonRecord {
    #[pg_type(Type::INT8)]
    pub id: i64,
    #[pg_type(Type::TEXT)]
    pub name: Option<String>,
    #[pg_type(Type::JSON)]
    #[try_from_json]
    pub data: Json<BTreeMap<String, i32>>,
}

async fn setup_database(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    // Create table with JSON column
    client
        .execute(
            "CREATE TABLE json_records (
                id BIGINT PRIMARY KEY,
                name TEXT,
                data JSON
            )",
            &[],
        )
        .await?;

    // Create publication
    client.execute("CREATE PUBLICATION json_pub FOR TABLE json_records", &[]).await?;

    Ok(())
}

async fn insert_test_data(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    // Insert test data with JSON
    client
        .execute(
            "INSERT INTO json_records (id, name, data) VALUES ($1, $2, $3)",
            &[&1i64, &"Alice", &serde_json::json!({"score": 100, "level": 5})],
        )
        .await?;

    client
        .execute(
            "INSERT INTO json_records (id, name, data) VALUES ($1, $2, $3)",
            &[&2i64, &"Bob", &serde_json::json!({"score": 200, "level": 10})],
        )
        .await?;

    Ok(())
}

async fn replication_task(
    sink: BTreeMapSink<JsonRecord, 1>,
    host: String,
    port: u16,
) -> Result<()> {
    let pg_source = PostgresSource::new(
        &host,
        port,
        "testdb",
        "postgres",
        Some("postgres".to_string()),
        Some("btreemapped_json_test_slot".to_string()),
        TableNamesFrom::Publication("json_pub".to_string()),
    )
    .await?;

    let batch_config = BatchConfig::new(100, std::time::Duration::from_millis(100));
    let mut pipeline =
        BatchDataPipeline::new(pg_source, sink, PipelineAction::Both, batch_config);
    let pipeline_fut = pipeline.start();
    pipeline_fut.await?;
    Ok(())
}

#[tokio::test]
async fn test_json_replication() -> Result<()> {
    // Setup postgres container
    let (_container, port) = setup_postgres_container().await?;
    let host = "localhost";

    // Setup database and table
    setup_database(host, port).await?;

    // Insert initial data
    insert_test_data(host, port).await?;

    // Create sink and replica
    let sink = BTreeMapSink::<JsonRecord, 1>::new("json_records");
    let replica = sink.replica.clone();

    // Start replication task
    let replication_handle = tokio::spawn({
        let host = host.to_string();
        async move {
            if let Err(e) = replication_task(sink, host, port).await {
                eprintln!("replication task failed: {:?}", e);
            }
        }
    });

    // Wait for replication to sync
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Verify data
    {
        let record1 = replica.get((1i64,));
        assert!(record1.is_some(), "Record with id 1 should exist");
        let record1 = record1.unwrap();
        assert_eq!(record1.id, 1);
        assert_eq!(record1.name.as_deref(), Some("Alice"));
        assert_eq!(record1.data.get("score"), Some(&100));
        assert_eq!(record1.data.get("level"), Some(&5));

        let record2 = replica.get((2i64,));
        assert!(record2.is_some(), "Record with id 2 should exist");
        let record2 = record2.unwrap();
        assert_eq!(record2.id, 2);
        assert_eq!(record2.name.as_deref(), Some("Bob"));
        assert_eq!(record2.data.get("score"), Some(&200));
        assert_eq!(record2.data.get("level"), Some(&10));
    }

    // Test update
    {
        let client = create_postgres_client(host, port).await?;

        client
            .execute(
                "UPDATE json_records SET data = $1 WHERE id = $2",
                &[&serde_json::json!({"score": 150, "level": 7}), &1i64],
            )
            .await?;

        // Wait for update to replicate
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let record1 = replica.get((1i64,)).unwrap();
        assert_eq!(
            record1.data.get("score"),
            Some(&150),
            "Score should be updated to 150"
        );
        assert_eq!(record1.data.get("level"), Some(&7), "Level should be updated to 7");
    }

    // Test delete
    {
        let client = create_postgres_client(host, port).await?;

        client.execute("DELETE FROM json_records WHERE id = $1", &[&2i64]).await?;

        // Wait for delete to replicate
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert!(replica.get((2i64,)).is_none(), "Record with id 2 should be deleted");
        assert!(replica.get((1i64,)).is_some(), "Record with id 1 should still exist");
    }

    // Clean up
    replication_handle.abort();

    Ok(())
}
