use anyhow::Result;
use btreemapped::{replicator::BTreeMapReplicator, BTreeMapped, LIndex1, PgSchema};
use btreemapped::config::{
    BatchConfig, PgConnectionConfig, PipelineConfig, TcpKeepaliveConfig, TlsConfig,
};
use postgres_types::Type;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, BTreeMapped, PgSchema)]
#[btreemap(index = ["id"])]
pub struct Foobar {
    #[pg_type(Type::INT8)]
    pub id: i64,
    #[pg_type(Type::TEXT)]
    pub first_name: Option<String>,
    #[pg_type(Type::INT4)]
    pub age: Option<i32>,
    #[pg_type(Type::BOOL)]
    pub is_foo: Option<bool>,
    #[pg_type(Type::BOOL)]
    pub is_bar: Option<bool>,
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "env_logger")]
    env_logger::init();

    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<Foobar, 1>("foobars");
    let cancel_token = CancellationToken::new();
    let synced = replicator.synced();

    // cancel the replication task after N seconds if CANCEL_AFTER_SECONDS is set
    if let Ok(cancel_after_seconds) = std::env::var("CANCEL_AFTER_SECONDS") {
        let secs = cancel_after_seconds.parse::<u64>().unwrap();
        let cancel_token = cancel_token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            eprintln!("cancelling replication task...");
            cancel_token.cancel();
        });
    }

    // start the replication task
    let replication_task = tokio::spawn(async move {
        if let Err(e) = replicator.run(pipeline_config(), Some(cancel_token)).await {
            #[cfg(feature = "log")]
            log::error!("replication task failed with: {e:?}");
            #[cfg(not(feature = "log"))]
            panic!("replication task failed with: {e:?}");
        }
    });

    // wait for all tables to be synced
    eprintln!("waiting for all tables to be synced...");
    synced.await;
    eprintln!("all tables are synced");

    // periodically print the state of the replica
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        let state = replica.read();
        eprintln!("{state:?}");
        // to trigger this println, insert a row into foobars with id 1337
        if let Some(row_with_id_1337) = state.get(&(1337,).into()) {
            eprintln!("row with id 1337: {row_with_id_1337:?}");
        }
        interval.tick().await;
        if replication_task.is_finished() {
            eprintln!("replication task finished");
            break;
        }
    }

    Ok(())
}

fn pg_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: "localhost".to_string(),
        port: 54320,
        name: "postgres".to_string(), // database name
        username: "postgres".to_string(),
        password: Some("postgres".to_string().into()),
        tls: TlsConfig {
            trusted_root_certs: "".to_string(),
            enabled: false,
        },
        keepalive: TcpKeepaliveConfig::default(),
    }
}

fn pipeline_config() -> PipelineConfig {
    PipelineConfig {
        id: 1,
        // publication name (from CREATE PUBLICATION);
        // this determines which tables are replicated to you
        publication_name: "foobars_pub".to_string(),
        pg_connection: pg_config(),
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 1000,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        max_copy_connections_per_table: 1,
        table_sync_copy: Default::default(),
        invalidated_slot_behavior: Default::default(),
    }
}
