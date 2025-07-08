use anyhow::Result;
use btreemapped::{BTreeMapSink, BTreeMapped, LIndex1, PgSchema};
use btreemapped_derive::{BTreeMapped, PgSchema};
use pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
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
    let sink = BTreeMapSink::<Foobar, 1>::new("foobars");
    let replica = sink.replica.clone();
    // start replication task
    tokio::spawn(async move {
        if let Err(e) = replication_task(sink).await {
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

async fn replication_task(sink: BTreeMapSink<Foobar, 1>) -> Result<()> {
    let pg_source = PostgresSource::new(
        "localhost",                  // host
        54320,                        // port
        "postgres",                   // database
        "postgres",                   // username
        Some("postgres".to_string()), // password
        // postgres replication slot name; this should be unique
        // for each application instance that intends to replicate
        //
        // reference: https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS
        Some("btreemapped_example_slot".to_string()),
        // publication name (from CREATE PUBLICATION);
        // this determines which tables are replicated to you
        TableNamesFrom::Publication("foobars_pub".to_string()),
    )
    .await?;
    let batch_config = BatchConfig::new(100, std::time::Duration::from_secs(1));
    let mut pipeline =
        BatchDataPipeline::new(pg_source, sink, PipelineAction::Both, batch_config);
    let pipeline_fut = pipeline.start();
    pipeline_fut.await?;
    Ok(())
}
