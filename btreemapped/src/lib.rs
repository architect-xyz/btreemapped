//! How does Postgres logical replication work for tables with b-tree
//! primary key indexes?
//!
//! TODO: fill out prose documentation here

pub mod lvalue;
pub mod multi_sink;
pub mod replica;
pub mod sink;

#[cfg(feature = "derive")]
pub use btreemapped_derive::BTreeMapped;
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
