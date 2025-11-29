use anyhow::Result;
use btreemapped::{replicator::BTreeMapReplicator, BTreeMapped, LIndex1, PgSchema};
use btreemapped_derive::{BTreeMapped, PgSchema};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use postgres_types::Type;
use serde::Serialize;
use utils::{create_postgres_client, setup_postgres_container};

mod utils;

#[derive(Debug, Clone, Serialize, BTreeMapped, PgSchema)]
#[btreemap(index = ["id"])]
pub struct TestRecord {
    #[pg_type(Type::INT8)]
    pub id: i64,
    #[pg_type(Type::TEXT)]
    pub name: Option<String>,
    #[pg_type(Type::INT4)]
    pub value: Option<i32>,
}

async fn setup_database(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    // Create table
    client
        .execute(
            "CREATE TABLE test_records (
                id BIGINT PRIMARY KEY,
                name TEXT,
                value INTEGER
            )",
            &[],
        )
        .await?;

    // Create publication
    client.execute("CREATE PUBLICATION test_pub FOR TABLE test_records", &[]).await?;

    Ok(())
}

async fn insert_test_data(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    // Insert test data
    client
        .execute(
            "INSERT INTO test_records (id, name, value) VALUES ($1, $2, $3)",
            &[&1i64, &"Alice", &100i32],
        )
        .await?;

    client
        .execute(
            "INSERT INTO test_records (id, name, value) VALUES ($1, $2, $3)",
            &[&2i64, &"Bob", &200i32],
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
        tls: TlsConfig { trusted_root_certs: "".to_string(), enabled: false },
        keepalive: None,
    }
}

fn pipeline_config(host: &str, port: u16) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "test_pub".to_string(),
        pg_connection: pg_config(host, port),
        batch: BatchConfig { max_size: 100, max_fill_ms: 100 },
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 3,
        max_table_sync_workers: 4,
    }
}

#[tokio::test]
async fn test_basic_replication() -> Result<()> {
    // Setup postgres container
    let (_container, port) = setup_postgres_container().await?;
    let host = "localhost";

    // Setup database and table
    setup_database(host, port).await?;

    // Insert initial data
    insert_test_data(host, port).await?;

    // Create replicator and replica
    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<TestRecord, 1>("test_records");

    // Start replication task
    let replication_handle = tokio::spawn({
        let config = pipeline_config(host, port);
        async move {
            if let Err(e) = replicator.run(config).await {
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
        assert_eq!(record1.value, Some(100));

        let record2 = replica.get((2i64,));
        assert!(record2.is_some(), "Record with id 2 should exist");
        let record2 = record2.unwrap();
        assert_eq!(record2.id, 2);
        assert_eq!(record2.name.as_deref(), Some("Bob"));
        assert_eq!(record2.value, Some(200));
    }

    // Test update
    {
        let client = create_postgres_client(host, port).await?;

        client
            .execute(
                "UPDATE test_records SET value = $1 WHERE id = $2",
                &[&150i32, &1i64],
            )
            .await?;

        // Wait for update to replicate
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let record1 = replica.get((1i64,)).unwrap();
        assert_eq!(record1.value, Some(150), "Value should be updated to 150");
    }

    // Test delete
    {
        let client = create_postgres_client(host, port).await?;

        client.execute("DELETE FROM test_records WHERE id = $1", &[&2i64]).await?;

        // Wait for delete to replicate
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert!(replica.get((2i64,)).is_none(), "Record with id 2 should be deleted");
        assert!(replica.get((1i64,)).is_some(), "Record with id 1 should still exist");
    }

    // Clean up
    replication_handle.abort();

    Ok(())
}
