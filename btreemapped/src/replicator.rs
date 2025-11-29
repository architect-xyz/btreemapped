use crate::{BTreeMapSink, BTreeMapped};
use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    state::table::TableReplicationPhase,
    store::{cleanup::CleanupStore, schema::SchemaStore, state::StateStore},
    types::TableName,
};
use etl_postgres::types::{TableId, TableSchema};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

/// Inner state of [`ReplicationState`]
#[derive(Debug)]
struct Inner {
    /// Registered BTreeMapSink metadata
    sink_metadata: HashMap<Arc<str>, Arc<OnceLock<TableMetadata>>>,
    /// Current replication state for each table - this is the authoritative source of truth
    /// for table states. Every table being replicated must have an entry here.
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
    /// Complete history of state transitions for each table, used for debugging and auditing.
    /// This is an append-only log that grows over time and provides visibility into
    /// table state evolution. Entries are chronologically ordered.
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    /// Cached table schema definitions, reference-counted for efficient sharing.
    /// Schemas are expensive to fetch from Postgres, so they're cached here
    /// once retrieved and shared via Arc across the application.
    table_schemas: HashMap<TableId, Arc<TableSchema>>,
    /// Mapping from table IDs to human-readable table names for easier debugging
    /// and logging. These mappings are established during schema discovery.
    table_mappings: HashMap<TableId, String>,
}

#[derive(Debug)]
pub(crate) struct TableMetadata {
    pub table_id: TableId,
    pub table_schema: Arc<TableSchema>,
}

/// In-memory storage for ETL pipeline state and schema information.
///
/// [`BTreeMapReplicator`] implements both [`StateStore`] and [`SchemaStore`] traits,
/// providing a complete storage solution that keeps all data in memory. This is
/// ideal for testing, development, and scenarios where persistence is not required.
///
/// All state information including table replication phases, schema definitions,
/// and table mappings are stored in memory and will be lost on process restart.
#[derive(Debug, Clone)]
pub struct BTreeMapReplicator {
    inner: Arc<Mutex<Inner>>,
}

impl BTreeMapReplicator {
    /// Creates a new empty memory store.
    ///
    /// The store initializes with empty collections for all state and schema data.
    /// As the pipeline runs, it will populate these collections with replication
    /// state and schema information.
    pub fn new() -> Self {
        let inner = Inner {
            sink_metadata: HashMap::new(),
            table_replication_states: HashMap::new(),
            table_state_history: HashMap::new(),
            table_schemas: HashMap::new(),
            table_mappings: HashMap::new(),
        };

        Self { inner: Arc::new(Mutex::new(inner)) }
    }

    pub fn register_sink<T: BTreeMapped<N>, const N: usize>(
        &self,
        sink: &BTreeMapSink<T, N>,
    ) {
        let mut inner = self.inner.lock();
        inner.sink_metadata.insert(sink.table_name.clone(), sink.table_metadata.clone());
    }
}

impl Default for BTreeMapReplicator {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for BTreeMapReplicator {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let inner = self.inner.lock();

        Ok(inner.table_replication_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.lock();

        Ok(inner.table_replication_states.clone())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        let inner = self.inner.lock();

        Ok(inner.table_replication_states.len())
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock();

        // Store the current state in history before updating
        if let Some(current_state) =
            inner.table_replication_states.get(&table_id).cloned()
        {
            inner
                .table_state_history
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(current_state);
        }

        inner.table_replication_states.insert(table_id, state);

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let mut inner = self.inner.lock();

        // Get the previous state from history
        let previous_state = inner
            .table_state_history
            .get_mut(&table_id)
            .and_then(|history| history.pop())
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::StateRollbackError,
                    "No previous state available to roll back to"
                )
            })?;

        // Update the current state to the previous state
        inner.table_replication_states.insert(table_id, previous_state.clone());

        Ok(previous_state)
    }

    async fn get_table_mapping(
        &self,
        source_table_id: &TableId,
    ) -> EtlResult<Option<String>> {
        let inner = self.inner.lock();

        Ok(inner.table_mappings.get(source_table_id).cloned())
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let inner = self.inner.lock();

        Ok(inner.table_mappings.clone())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        let inner = self.inner.lock();

        Ok(inner.table_mappings.len())
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock();
        inner.table_mappings.insert(source_table_id, destination_table_id);

        Ok(())
    }
}

impl SchemaStore for BTreeMapReplicator {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock();

        Ok(inner.table_schemas.get(table_id).cloned())
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock();

        Ok(inner.table_schemas.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.lock();

        Ok(inner.table_schemas.len())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let mut inner = self.inner.lock();

        let table_schema = Arc::new(table_schema);
        let sink_table_name = normalized_table_name(&table_schema.name);

        if let Some(meta) = inner.sink_metadata.get(sink_table_name.as_str()) {
            if let Err(_) = meta.set(TableMetadata {
                table_id: table_schema.id,
                table_schema: table_schema.clone(),
            }) {
                Err::<_, EtlError>(
                    (ErrorKind::DestinationError, SCHEMA_CHANGE_NOT_ALLOWED_ERROR).into(),
                )?;
            }
        }

        inner.table_schemas.insert(table_schema.id, table_schema);

        Ok(())
    }
}

impl CleanupStore for BTreeMapReplicator {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock();

        inner.table_replication_states.remove(&table_id);
        inner.table_state_history.remove(&table_id);
        inner.table_schemas.remove(&table_id);
        inner.table_mappings.remove(&table_id);

        Ok(())
    }
}

/// Assuming the default schema is "public", normalize a full
/// {schema}.{table} name by omitting the default schema.
pub(crate) fn normalized_table_name(table_name: &TableName) -> String {
    if table_name.schema == "public" {
        table_name.name.to_string()
    } else {
        table_name.to_string()
    }
}

static SCHEMA_CHANGE_NOT_ALLOWED_ERROR: &'static str =
    "btreemapped destination does not tolerate online schema changes or table renames";
