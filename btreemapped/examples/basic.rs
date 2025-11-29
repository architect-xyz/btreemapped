use anyhow::Result;
use btreemapped::{replicator::BTreeMapReplicator, BTreeMapped, LIndex1, PgSchema};
use btreemapped_derive::{BTreeMapped, PgSchema};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use postgres_types::Type;
use serde::Serialize;

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
    let replicator = BTreeMapReplicator::new();
    let replica = replicator.add_replica::<Foobar, 1>("foobars");

    // start the replication task
    tokio::spawn(async move {
        if let Err(e) = replicator.run(pipeline_config()).await {
            #[cfg(feature = "log")]
            log::error!("replication task failed with: {e:?}");
            #[cfg(not(feature = "log"))]
            panic!("replication task failed with: {e:?}");
        }
    });

    // periodically print the state of the replica
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        interval.tick().await;
        let state = replica.read();
        eprintln!("{state:?}");
        // to trigger this println, insert a row into foobars with id 1337
        if let Some(row_with_id_1337) = state.get(&(1337,).into()) {
            eprintln!("row with id 1337: {row_with_id_1337:?}");
        }
    }
    #[allow(unreachable_code)]
    Ok(())
}

fn pg_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: "localhost".to_string(),
        port: 54320,
        name: "postgres".to_string(), // database name
        username: "postgres".to_string(),
        password: Some("postgres".to_string().into()),
        tls: TlsConfig { trusted_root_certs: "".to_string(), enabled: false },
        keepalive: None,
    }
}

fn pipeline_config() -> PipelineConfig {
    PipelineConfig {
        id: 1,
        // publication name (from CREATE PUBLICATION);
        // this determines which tables are replicated to you
        publication_name: "foobars_pub".to_string(),
        pg_connection: pg_config(),
        batch: BatchConfig { max_size: 100, max_fill_ms: 1000 },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    }
}
