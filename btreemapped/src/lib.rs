//! How does Postgres logical replication work for tables with b-tree
//! primary key indexes?
//!
//! TODO: fill out prose documentation here

pub mod lvalue;
pub mod replica;
pub mod sink;

#[cfg(feature = "derive")]
pub use btreemapped_derive::BTreeMapped;
pub use lvalue::*;
pub use replica::{BTreeMapReplica, BTreeMapSyncError, BTreeSnapshot, BTreeUpdate};

pub trait BTreeMapped<const N: usize>: Clone + 'static {
    type Ref<'a>;
    type LIndex: HasArity<N>
        + std::fmt::Debug
        + Eq
        + Ord
        + std::hash::Hash
        + Send
        + Sync
        + 'static;
    type Index: Into<Self::LIndex>
        + for<'a> TryFrom<&'a Self::LIndex>
        + Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static;
    type Unindexed: Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static;

    fn into_kv(self) -> (Self::Index, Self::Unindexed);

    fn kv_as_ref<'a>(
        index: &'a Self::LIndex,
        unindexed: &'a Self::Unindexed,
    ) -> Option<Self::Ref<'a>>;

    /// Parse a row from pg_replica into a full struct.
    fn parse_row(
        schema: &pg_replicate::table::TableSchema,
        row: pg_replicate::conversions::table_row::TableRow,
    ) -> anyhow::Result<(Self::Index, Self::Unindexed)>;

    /// Parse a row from pg_replica into just the index struct.
    fn parse_row_index(
        schema: &pg_replicate::table::TableSchema,
        row: pg_replicate::conversions::table_row::TableRow,
    ) -> anyhow::Result<Self::Index>;
}
