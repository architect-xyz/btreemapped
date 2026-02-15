use anyhow::Result;
use btreemapped::{
    replicator::BTreeMapReplicator, BTreeMapped, LIndex1, PgJson, PgSchema,
};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use postgres_types::Type;
use serde::Serialize;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;
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
    pub data: PgJson<BTreeMap<String, i32>>,
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
    client
        .execute("CREATE PUBLICATION json_pub FOR TABLE json_records", &[])
        .await?;

    Ok(())
}

async fn insert_test_data(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    // Insert test data with JSON
    client
        .execute(
            "INSERT INTO json_records (id, name, data) VALUES ($1, $2, $3)",
            &[
                &1i64,
                &"Alice",
                &serde_json::json!({"score": 100, "level": 5}),
            ],
        )
        .await?;

    client
        .execute(
            "INSERT INTO json_records (id, name, data) VALUES ($1, $2, $3)",
            &[
                &2i64,
                &"Bob",
                &serde_json::json!({"score": 200, "level": 10}),
            ],
        )
        .await?;

    Ok(())
}

fn pg_config(host: &str, port: u16) -> PgConnectionConfig {
    PgConnectionConfig {
        host: host.to_string(),
        port,
        name: "testdb".to_string(),
        username: "postgres".to_string(),
        password: Some("postgres".to_string().into()),
        tls: TlsConfig {
            trusted_root_certs: "".to_string(),
            enabled: false,
        },
        keepalive: None,
    }
}

fn pipeline_config(host: &str, port: u16) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "json_pub".to_string(),
        pg_connection: pg_config(host, port),
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 100,
        },
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 3,
        max_table_sync_workers: 4,
        slot_prefix: "test_json_replication".to_string(),
    }
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

    // Create replicator and replica
    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<JsonRecord, 1>("json_records");
    let cancel = CancellationToken::new();

    // Start replication task
    let replication_handle = tokio::spawn({
        let config = pipeline_config(host, port);
        let cancel = cancel.clone();
        async move {
            if let Err(e) = replicator.run(config, Some(cancel)).await {
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
        assert_eq!(
            record1.data.get("level"),
            Some(&7),
            "Level should be updated to 7"
        );
    }

    // Test delete
    {
        let client = create_postgres_client(host, port).await?;

        client
            .execute("DELETE FROM json_records WHERE id = $1", &[&2i64])
            .await?;

        // Wait for delete to replicate
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert!(
            replica.get((2i64,)).is_none(),
            "Record with id 2 should be deleted"
        );
        assert!(
            replica.get((1i64,)).is_some(),
            "Record with id 1 should still exist"
        );
    }

    cancel.cancel();
    let _ = replication_handle.await;

    Ok(())
}
