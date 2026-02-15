use anyhow::Result;
use btreemapped::{replicator::BTreeMapReplicator, BTreeMapped, LIndex1};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use tokio_util::sync::CancellationToken;
use utils::{create_postgres_client, setup_postgres_container};

mod utils;

#[derive(Debug, Clone, BTreeMapped)]
#[btreemap(index = ["id"])]
pub struct RecoveryRecord {
    pub id: i64,
    pub data: Option<String>,
}

fn pipeline_config(host: &str, port: u16) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "recovery_pub".to_string(),
        pg_connection: PgConnectionConfig {
            host: host.to_string(),
            port,
            name: "testdb".to_string(),
            username: "postgres".to_string(),
            password: Some("postgres".to_string().into()),
            tls: TlsConfig { trusted_root_certs: "".to_string(), enabled: false },
            keepalive: None,
        },
        batch: BatchConfig { max_size: 100, max_fill_ms: 100 },
        table_error_retry_delay_ms: 500,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        slot_prefix: "test_recovery".to_string(),
    }
}

/// Kill the table sync worker's COPY connection during initial sync to
/// trigger the rollback_table_replication_state → retry code path.
///
/// The pipeline creates two walsender connections:
/// 1. Main pipeline connection — for CDC streaming (apply worker)
/// 2. Table sync worker connection — for initial COPY of table data
///
/// We insert enough data that the COPY takes a few seconds, giving us
/// a wide window to detect and kill the table sync worker's walsender.
/// We poll pg_stat_activity for a second walsender PID, kill it, then
/// poll for the pipeline to recover and complete the sync.
#[tokio::test]
async fn test_rollback_on_connection_kill() -> Result<()> {
    let (_container, port) = setup_postgres_container().await?;
    let host = "localhost";

    let client = create_postgres_client(host, port).await?;

    client
        .execute(
            "CREATE TABLE recovery_records (
                id BIGINT PRIMARY KEY,
                data TEXT
            )",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE PUBLICATION recovery_pub FOR TABLE recovery_records",
            &[],
        )
        .await?;

    // Insert enough data that the table sync COPY takes a few seconds,
    // giving us a wide window to detect the second walsender.
    client
        .batch_execute(
            "INSERT INTO recovery_records (id, data)
             SELECT g, repeat('x', 500)
             FROM generate_series(1, 200000) g",
        )
        .await?;
    drop(client);

    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<RecoveryRecord, 1>("recovery_records");
    let cancel = CancellationToken::new();

    let replication_handle = tokio::spawn({
        let config = pipeline_config(host, port);
        let cancel = cancel.clone();
        async move {
            if let Err(e) = replicator.run(config, Some(cancel)).await {
                eprintln!("replication task failed: {:?}", e);
            }
        }
    });

    // Track the first walsender PID (main pipeline connection).
    // Then wait for a SECOND walsender to appear (table sync worker).
    let monitor = create_postgres_client(host, port).await?;
    let mut main_pid: Option<i32> = None;
    let mut killed = false;

    for _ in 0..400 {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let rows = monitor
            .query(
                "SELECT pid
                 FROM pg_stat_activity
                 WHERE datname = 'testdb'
                   AND pid != pg_backend_pid()
                   AND backend_type = 'walsender'",
                &[],
            )
            .await?;

        let pids: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();

        if main_pid.is_none() && !pids.is_empty() {
            main_pid = Some(pids[0]);
        }

        if let Some(mpid) = main_pid {
            for &pid in &pids {
                if pid != mpid {
                    monitor
                        .execute("SELECT pg_terminate_backend($1)", &[&pid])
                        .await?;
                    killed = true;
                }
            }
        }

        if killed {
            break;
        }
    }

    assert!(killed, "should have found and killed a table sync worker connection");

    // Poll until all rows are synced (retry delay + re-COPY).
    for _ in 0..200 {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        if replica.read().len() == 200_000 {
            break;
        }
    }

    assert_eq!(
        replica.read().len(),
        200_000,
        "all rows should be synced after recovery"
    );

    cancel.cancel();
    let _ = replication_handle.await;

    Ok(())
}
