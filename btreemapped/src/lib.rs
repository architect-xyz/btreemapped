//! How does Postgres logical replication work for tables with b-tree
//! primary key indexes?
//!
//! TODO: fill out prose documentation here

pub mod lvalue;
pub mod multi_sink;
pub mod replica;
pub mod sink;

#[cfg(feature = "derive")]
pub use btreemapped_derive::{BTreeMapped, PgSchema};
pub use lvalue::*;
pub use multi_sink::MultiBTreeMapSink;
pub use replica::{BTreeMapReplica, BTreeMapSyncError, BTreeSnapshot, BTreeUpdate};
pub use sink::BTreeMapSink;

pub trait BTreeMapped<const N: usize>: Clone + Send + Sync + 'static {
    type LIndex: HasArity<N>
        + std::fmt::Debug
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

    /// Parse a row from pg_replica into a full struct.
    fn parse_row(
        schema: &pg_replicate::table::TableSchema,
        row: pg_replicate::conversions::table_row::TableRow,
    ) -> anyhow::Result<Self>;

    /// Parse a row from pg_replica into just the index.
    /// Used for UPDATE/DELETE operations.
    fn parse_row_index(
        schema: &pg_replicate::table::TableSchema,
        row: pg_replicate::conversions::table_row::TableRow,
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
