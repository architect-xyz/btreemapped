use anyhow::Result;
use btreemapped::{replicator::BTreeMapReplicator, BTreeMapped, LIndex1, PgSchema};
use btreemapped_derive::{BTreeMapped, PgSchema};
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    pipeline::Pipeline,
};
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
    // start replication task
    tokio::spawn(async move {
        if let Err(e) = replication_task(replicator).await {
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

async fn replication_task(replicator: BTreeMapReplicator) -> Result<()> {
    let pg_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 54320,
        name: "postgres".to_string(), // database name
        username: "postgres".to_string(),
        password: Some("postgres".to_string().into()),
        tls: TlsConfig { trusted_root_certs: "".to_string(), enabled: false },
        keepalive: None,
    };
    let pipeline_config = PipelineConfig {
        id: 1,
        // publication name (from CREATE PUBLICATION);
        // this determines which tables are replicated to you
        publication_name: "foobars_pub".to_string(),
        pg_connection: pg_config,
        batch: BatchConfig { max_size: 100, max_fill_ms: 1000 },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };
    // TODO: what is the slot name now?
    // postgres replication slot name; this should be unique
    // for each application instance that intends to replicate
    //
    // reference: https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS
    // Some("btreemapped_example_slot".to_string()),
    let mut pipeline = Pipeline::new(pipeline_config, replicator.clone(), replicator);
    pipeline.start().await?;
    pipeline.wait().await?;
    Ok(())
}
