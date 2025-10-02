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
pub use replica::{
    BTreeMapReplica, BTreeMapSyncError, BTreeSnapshot, BTreeUpdate, BTreeWrite,
};
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

/// Wrapper type for JSON fields that implements ToSql by serializing to serde_json::Value
#[derive(Debug, Clone)]
pub struct Json<T>(pub T);

impl<T> std::ops::Deref for Json<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Json<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> serde::Serialize for Json<T>
where
    T: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<T> postgres_types::ToSql for Json<T>
where
    T: serde::Serialize + std::fmt::Debug + Sync,
{
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let json_value = serde_json::to_value(&self.0)?;
        json_value.to_sql(ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        matches!(*ty, postgres_types::Type::JSON | postgres_types::Type::JSONB)
    }

    postgres_types::to_sql_checked!();
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
