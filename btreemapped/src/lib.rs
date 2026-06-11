//! How does Postgres logical replication work for tables with b-tree
//! primary key indexes?
//!
//! TODO: fill out prose documentation here

pub mod cell;
pub mod json;
pub mod lvalue;
pub(crate) mod numeric;
pub mod replica;
pub mod replicator;
pub(crate) mod sink;

/// Re-exported configuration types from [`etl`].
///
/// Use `pipeline_id` (the `id` field on [`PipelineConfig`]) to distinguish
/// between multiple replication pipelines.  It determines the replication
/// slot names on the Postgres side (`supabase_etl_apply_{pipeline_id}`,
/// `supabase_etl_table_sync_{pipeline_id}_{table_id}`).
pub mod config {
    pub use etl::config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig,
        PgConnectionConfig, PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig,
        TlsConfig,
    };
}

/// Re-exported migration helpers from [`etl`].
///
/// Since supabase/etl#712, every pipeline runs a set of source-database
/// migrations on startup (installing the `etl` schema and a DDL event
/// trigger for schema-change messages).  Creating an event trigger requires
/// superuser, so if your pipeline connects as a restricted role, run
/// [`migrations::run_source_migrations`] once as a privileged role first;
/// the pipeline will then verify the migrations instead of applying them.
pub mod migrations {
    pub use etl::migrations::run_source_migrations;
}

#[cfg(feature = "derive")]
pub use btreemapped_derive::{BTreeMapped, PgSchema};
pub use json::PgJson;
pub use lvalue::*;
pub use replica::{
    BTreeMapReplica, BTreeMapSyncError, BTreeSnapshot, BTreeUpdate, BTreeWrite,
};
pub use replicator::BTreeMapReplicator;

pub trait BTreeMapped<const N: usize>: Clone + Send + Sync + 'static {
    type LIndex: HasArity<N>
        + std::fmt::Debug
        + Clone
        + Eq
        + Ord
        + std::hash::Hash
        + Send
        + Sync
        + 'static;
    // CR alee: consider dropping this and just using LIndex directly,
    // although is it more obtuse with or without the intermediate?
    type Index: Into<Self::LIndex>
        + for<'a> TryFrom<&'a Self::LIndex>
        + Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static;

    /// Build the index value for this struct.
    fn index(&self) -> Self::Index;

    /// Parse a row from etl into a full struct.  Row values are matched
    /// positionally against `columns`, which must be in the same order as
    /// the row values (e.g. the replicated column schemas of the table).
    fn parse_row<'a>(
        columns: impl Iterator<Item = &'a etl_postgres::types::ColumnSchema>,
        row: etl::types::TableRow,
    ) -> anyhow::Result<Self>;

    /// Parse a row from etl into just the index.
    /// Used for UPDATE/DELETE operations.  Row values are matched
    /// positionally against `columns`; for key-only rows, pass the identity
    /// column schemas of the table.
    fn parse_row_index<'a>(
        columns: impl Iterator<Item = &'a etl_postgres::types::ColumnSchema>,
        row: etl::types::TableRow,
    ) -> anyhow::Result<Self::Index>;
}

pub trait PgSchema {
    fn column_names() -> impl ExactSizeIterator<Item = &'static str>;

    fn column_types() -> impl ExactSizeIterator<Item = postgres_types::Type>;

    fn primary_key_column_names() -> Option<impl ExactSizeIterator<Item = &'static str>>;

    fn columns_to_sql(
        &self,
    ) -> impl ExactSizeIterator<Item = &(dyn postgres_types::ToSql + Sync)>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use btreemapped_derive::PgSchema;
    use chrono::{DateTime, Utc};
    use postgres_types::Type;

    #[allow(dead_code)]
    #[derive(Debug, Clone, PgSchema)]
    #[btreemap(index = ["key"])]
    struct Foo {
        #[pg_type(Type::TEXT)]
        key: String,
        #[pg_type(Type::TIMESTAMPTZ)]
        bar: Option<DateTime<Utc>>,
        #[pg_type(Type::INT4)]
        baz: i32,
        #[pg_type(Type::INT8)]
        qux: i64,
    }
}
