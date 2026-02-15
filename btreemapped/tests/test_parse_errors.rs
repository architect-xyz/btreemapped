use anyhow::Result;
use btreemapped::{replicator::BTreeMapReplicator, BTreeMapped, LIndex1};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use utils::{create_postgres_client, setup_postgres_container};

mod utils;

// ---------------------------------------------------------------------------
// Test A: parse_row error via non-optional field + NULL
// ---------------------------------------------------------------------------

/// A struct with a non-optional `value` field. When Postgres sends NULL
/// for this column, parse_row will fail (Cell::Null → i32 is an error).
#[derive(Debug, Clone, BTreeMapped)]
#[btreemap(index = ["id"])]
pub struct StrictRecord {
    pub id: i64,
    pub value: i32, // NOT Option — NULL will cause parse_row to fail
}

async fn setup_strict_table(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    // Postgres column is nullable, but the Rust type is non-optional
    client
        .execute(
            "CREATE TABLE strict_records (
                id BIGINT PRIMARY KEY,
                value INTEGER
            )",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE PUBLICATION strict_pub FOR TABLE strict_records",
            &[],
        )
        .await?;

    // Row 1: valid (parse_row succeeds)
    // Row 2: NULL value (parse_row fails → row silently skipped)
    // Row 3: valid
    client
        .batch_execute(
            "INSERT INTO strict_records (id, value) VALUES
                (1, 42),
                (2, NULL),
                (3, 99)",
        )
        .await?;

    Ok(())
}

fn strict_pipeline_config(host: &str, port: u16) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "strict_pub".to_string(),
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
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 3,
        max_table_sync_workers: 4,
        slot_prefix: "test_strict".to_string(),
    }
}

/// Rows with NULL in a non-optional field should be silently skipped
/// during initial sync (exercises parse_row Err path in sink.rs).
#[tokio::test]
async fn test_parse_row_error_skips_invalid_rows() -> Result<()> {
    let (_container, port) = setup_postgres_container().await?;
    let host = "localhost";

    setup_strict_table(host, port).await?;

    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<StrictRecord, 1>("strict_records");
    let cancel = CancellationToken::new();

    let replication_handle = tokio::spawn({
        let config = strict_pipeline_config(host, port);
        let cancel = cancel.clone();
        async move {
            if let Err(e) = replicator.run(config, Some(cancel)).await {
                eprintln!("replication task failed: {:?}", e);
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Valid rows should be present
    let r1 = replica.get((1i64,));
    assert!(r1.is_some(), "Row 1 (valid) should be replicated");
    assert_eq!(r1.unwrap().value, 42);

    let r3 = replica.get((3i64,));
    assert!(r3.is_some(), "Row 3 (valid) should be replicated");
    assert_eq!(r3.unwrap().value, 99);

    // Row with NULL in non-optional field should be skipped
    assert!(replica.get((2i64,)).is_none(), "Row 2 (NULL value) should be skipped");

    cancel.cancel();
    let _ = replication_handle.await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Test B: parse_row_index error via #[parse] key with invalid value
// ---------------------------------------------------------------------------

/// An enum that only accepts specific string values.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum Status {
    Active,
    Inactive,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Active => write!(f, "active"),
            Status::Inactive => write!(f, "inactive"),
        }
    }
}

impl std::str::FromStr for Status {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Status::Active),
            "inactive" => Ok(Status::Inactive),
            _ => anyhow::bail!("unknown status: {s}"),
        }
    }
}

/// A struct whose key uses `#[parse]` with a fallible FromStr.
/// Rows with unrecognized status values will fail both parse_row
/// and parse_row_index.
#[derive(Debug, Clone, BTreeMapped)]
#[btreemap(index = ["status"])]
pub struct StatusRecord {
    #[parse]
    pub status: Status,
    pub value: i64,
}

async fn setup_status_table(host: &str, port: u16) -> Result<()> {
    let client = create_postgres_client(host, port).await?;

    client
        .execute(
            "CREATE TABLE status_records (
                status TEXT PRIMARY KEY,
                value BIGINT NOT NULL
            )",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE PUBLICATION status_pub FOR TABLE status_records",
            &[],
        )
        .await?;

    // 'active' and 'inactive' parse OK; 'unknown' and 'pending' do not
    client
        .batch_execute(
            "INSERT INTO status_records (status, value) VALUES
                ('active', 1),
                ('inactive', 2),
                ('unknown', 3),
                ('pending', 4)",
        )
        .await?;

    Ok(())
}

fn status_pipeline_config(host: &str, port: u16) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "status_pub".to_string(),
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
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 3,
        max_table_sync_workers: 4,
        slot_prefix: "test_status".to_string(),
    }
}

/// Tests both parse_row and parse_row_index error paths:
/// 1. Initial sync: rows with unparseable keys fail parse_row (skipped)
/// 2. CDC DELETE of unparseable row: parse_row_index fails (no-op)
/// 3. CDC INSERT of unparseable row: parse_row fails via CDC path
#[tokio::test]
async fn test_parse_row_index_error_on_delete() -> Result<()> {
    let (_container, port) = setup_postgres_container().await?;
    let host = "localhost";

    setup_status_table(host, port).await?;

    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<StatusRecord, 1>("status_records");
    let cancel = CancellationToken::new();

    let replication_handle = tokio::spawn({
        let config = status_pipeline_config(host, port);
        let cancel = cancel.clone();
        async move {
            if let Err(e) = replicator.run(config, Some(cancel)).await {
                eprintln!("replication task failed: {:?}", e);
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Only parseable rows should be in the replica
    assert!(
        replica.get((Status::Active,)).is_some(),
        "'active' should be replicated"
    );
    assert!(
        replica.get((Status::Inactive,)).is_some(),
        "'inactive' should be replicated"
    );

    // 'unknown' and 'pending' failed parse_row → not in replica
    // (We can't look them up by Status since they don't parse,
    //  but we can check the replica size)
    let count = {
        let r = replica.read();
        r.len()
    };
    assert_eq!(count, 2, "Only 2 parseable rows should be in replica");

    // DELETE an unparseable row → exercises parse_row_index Err path
    // (Postgres sends old_table_row with the primary key 'unknown',
    //  parse_row_index tries to parse 'unknown' as Status → fails)
    {
        let client = create_postgres_client(host, port).await?;
        client
            .execute("DELETE FROM status_records WHERE status = 'unknown'", &[])
            .await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Replica unchanged (the row was never there)
    assert_eq!(replica.read().len(), 2);

    // INSERT another unparseable row via CDC → exercises parse_row Err via CDC
    {
        let client = create_postgres_client(host, port).await?;
        client
            .execute(
                "INSERT INTO status_records (status, value) VALUES ('bogus', 5)",
                &[],
            )
            .await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Still only 2 rows (the bogus INSERT was silently skipped)
    assert_eq!(replica.read().len(), 2);

    cancel.cancel();
    let _ = replication_handle.await;

    Ok(())
}
