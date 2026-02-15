use anyhow::Result;
use btreemapped::{replicator::BTreeMapReplicator, BTreeMapped, LIndex1};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use tokio_util::sync::CancellationToken;
use utils::{create_postgres_client, setup_postgres_container};

mod utils;

#[derive(Debug, Clone, BTreeMapped)]
#[btreemap(index = ["id"])]
pub struct CdcRecord {
    pub id: i64,
    pub name: Option<String>,
}

async fn setup_database(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    client
        .execute(
            "CREATE TABLE cdc_records (
                id BIGINT PRIMARY KEY,
                name TEXT
            )",
            &[],
        )
        .await?;

    client
        .execute("CREATE PUBLICATION cdc_pub FOR TABLE cdc_records", &[])
        .await?;

    Ok(())
}

fn pipeline_config(host: &str, port: u16) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "cdc_pub".to_string(),
        pg_connection: PgConnectionConfig {
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
        },
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 100,
        },
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 3,
        max_table_sync_workers: 4,
        slot_prefix: "test_cdc_events".to_string(),
    }
}

/// Tests CDC Insert and Truncate events, and the synced() notification.
///
/// - Starts replication with an empty table
/// - Uses synced() to wait for initial sync (instead of sleep)
/// - INSERTs rows after sync → exercises Event::Insert CDC path
/// - TRUNCATE TABLE → exercises Event::Truncate CDC path + sink.truncate()
#[tokio::test]
async fn test_cdc_insert_and_truncate() -> Result<()> {
    let (_container, port) = setup_postgres_container().await?;
    let host = "localhost";

    setup_database(host, port).await?;

    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<CdcRecord, 1>("cdc_records");
    let cancel = CancellationToken::new();
    let synced = replicator.synced();

    let replication_handle = tokio::spawn({
        let config = pipeline_config(host, port);
        let cancel = cancel.clone();
        async move {
            if let Err(e) = replicator.run(config, Some(cancel)).await {
                eprintln!("replication task failed: {:?}", e);
            }
        }
    });

    // Wait for initial sync using synced() notification (covers synced() method)
    tokio::select! {
        _ = synced => {}
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
            panic!("timed out waiting for initial sync");
        }
    }

    // INSERT rows via CDC (not initial sync)
    {
        let client = create_postgres_client(host, port).await?;
        client
            .batch_execute(
                "INSERT INTO cdc_records (id, name) VALUES
                    (1, 'Alice'),
                    (2, 'Bob')",
            )
            .await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    assert!(
        replica.get((1i64,)).is_some(),
        "CDC INSERT: record 1 should exist"
    );
    assert!(
        replica.get((2i64,)).is_some(),
        "CDC INSERT: record 2 should exist"
    );
    assert_eq!(replica.get((1i64,)).unwrap().name.as_deref(), Some("Alice"));

    // TRUNCATE via CDC
    {
        let client = create_postgres_client(host, port).await?;
        client.execute("TRUNCATE cdc_records", &[]).await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    assert!(
        replica.get((1i64,)).is_none(),
        "TRUNCATE: record 1 should be gone"
    );
    assert!(
        replica.get((2i64,)).is_none(),
        "TRUNCATE: record 2 should be gone"
    );

    cancel.cancel();
    let _ = replication_handle.await;

    Ok(())
}
