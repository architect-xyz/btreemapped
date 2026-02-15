use crate::{
    sink::{ErasedBTreeMapReplica, ErasedBTreeMapSink},
    BTreeMapReplica, BTreeMapped,
};
use etl::{
    config::PipelineConfig,
    destination::Destination,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    pipeline::Pipeline,
    state::table::TableReplicationPhase,
    store::{cleanup::CleanupStore, schema::SchemaStore, state::StateStore},
    types::{Event, TableName, TableRow},
};
use etl_postgres::types::{TableId, TableSchema};
use futures::{pin_mut, select_biased, FutureExt};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{futures::OwnedNotified, Notify};
use tokio_util::sync::CancellationToken;

/// Inner state of [`BTreeMapReplicator`]
struct Inner {
    sinks: HashMap<TableId, Box<dyn ErasedBTreeMapSink>>,
    pending_sinks: HashMap<String, Box<dyn ErasedBTreeMapReplica>>,
    committed_lsn: u64,
    txn_lsn: Option<u64>,
    fully_synced: bool,
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

/// In-memory storage for ETL pipeline state and schema information.
///
/// [`BTreeMapReplicator`] implements both [`StateStore`] and [`SchemaStore`] traits,
/// providing a complete storage solution that keeps all data in memory. This is
/// ideal for testing, development, and scenarios where persistence is not required.
///
/// All state information including table replication phases, schema definitions,
/// and table mappings are stored in memory and will be lost on process restart.
#[derive(Clone)]
pub struct BTreeMapReplicator {
    inner: Arc<Mutex<Inner>>,
    synced: Arc<Notify>,
}

impl BTreeMapReplicator {
    /// Creates a new empty memory store.
    ///
    /// The store initializes with empty collections for all state and schema data.
    /// As the pipeline runs, it will populate these collections with replication
    /// state and schema information.
    pub fn new() -> Self {
        let inner = Inner {
            sinks: HashMap::new(),
            pending_sinks: HashMap::new(),
            committed_lsn: 0,
            txn_lsn: None,
            fully_synced: false,
            table_replication_states: HashMap::new(),
            table_state_history: HashMap::new(),
            table_schemas: HashMap::new(),
            table_mappings: HashMap::new(),
        };

        Self { inner: Arc::new(Mutex::new(inner)), synced: Arc::new(Notify::new()) }
    }

    pub fn add_replica<T: BTreeMapped<N>, const N: usize>(
        &self,
        table_name: &str,
    ) -> BTreeMapReplica<T, N> {
        let mut inner = self.inner.lock();
        let replica = BTreeMapReplica::new();
        inner.pending_sinks.insert(table_name.to_string(), Box::new(replica.clone()));
        replica
    }

    pub fn synced(&self) -> OwnedNotified {
        self.synced.clone().notified_owned()
    }

    /// Run the replication pipeline.  Optionally provide a cancellation token
    /// to stop the pipeline from another context.  Additionally, provide a
    /// `Notify` to be notified when all tables have reached `SyncDone` state.
    pub async fn run(
        self,
        pipeline_config: PipelineConfig,
        cancellation_token: Option<CancellationToken>,
    ) -> anyhow::Result<()> {
        let mut pipeline = Pipeline::new(pipeline_config, self.clone(), self);
        let shutdown_tx = pipeline.shutdown_tx();
        pipeline.start().await?;
        let pipeline_fut = pipeline.wait().fuse();
        pin_mut!(pipeline_fut);

        let cancellation_token =
            cancellation_token.unwrap_or_else(|| CancellationToken::new());
        let cancellation = cancellation_token.cancelled().fuse();
        pin_mut!(cancellation);

        select_biased! {
            _ = cancellation => {
                if let Err(_) = shutdown_tx.shutdown() {
                    return Err(anyhow::anyhow!("failed to shutdown pipeline"));
                }
            }
            res = &mut pipeline_fut => {
                res?;
                return Ok(());
            }
        }

        pipeline_fut.await?;

        Ok(())
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

        if !inner.fully_synced {
            if inner
                .table_replication_states
                .values()
                .all(|state| matches!(state, TableReplicationPhase::SyncDone { .. }))
            {
                inner.fully_synced = true;
                self.synced.notify_waiters();
            }
        }

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

        #[cfg(feature = "log")]
        log::trace!("store_table_schema: {:?}", table_schema);

        if inner.sinks.contains_key(&table_schema.id) {
            #[cfg(feature = "log")]
            log::error!("table schema already registered: {:?}", table_schema);
            Err::<_, EtlError>(
                (ErrorKind::DestinationError, SCHEMA_CHANGE_NOT_ALLOWED_ERROR).into(),
            )?;
        } else if let Some(replica) = inner.pending_sinks.remove(&sink_table_name) {
            #[cfg(feature = "log")]
            log::debug!(
                "table {} sink assigned, table_id = {}",
                sink_table_name,
                table_schema.id
            );
            let sink = replica.to_sink(table_schema.clone());
            inner.sinks.insert(table_schema.id, sink);
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

impl Destination for BTreeMapReplicator {
    fn name() -> &'static str {
        "btreemapped"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock();
        if let Some(sink) = inner.sinks.get_mut(&table_id) {
            sink.truncate();
        }
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        #[cfg(feature = "log")]
        log::trace!("write_table_rows to table {table_id}: {} rows", rows.len());
        let should_yield = {
            let mut inner = self.inner.lock();
            if let Some(sink) = inner.sinks.get_mut(&table_id) {
                sink.write_table_rows(rows)?;
                true
            } else {
                false
            }
        };
        if should_yield {
            // let consumers catch up
            // CR alee: possibly we could make this more efficient by tuning when we
            // yield to the size of our bcast channel? not sure if that's true...
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        for event in events {
            #[cfg(feature = "log")]
            log::trace!("write_events: {:?}", event);
            let mut should_yield = false;
            match event {
                Event::Begin(begin) => {
                    let mut inner = self.inner.lock();
                    inner.txn_lsn = Some(begin.commit_lsn.into());
                }
                Event::Commit(commit) => {
                    let mut inner = self.inner.lock();
                    let commit_lsn: u64 = commit.commit_lsn.into();
                    let txn_lsn = if let Some(lsn) = inner.txn_lsn {
                        lsn
                    } else {
                        return Err(etl_error!(
                            ErrorKind::DestinationError,
                            "commit without preceding begin"
                        ));
                    };
                    if txn_lsn != commit_lsn {
                        return Err(etl_error!(
                            ErrorKind::DestinationError,
                            "commit with incorrect LSN"
                        ));
                    }
                    // INVARIANT: txn_lsn == commit_lsn
                    for sink in inner.sinks.values_mut() {
                        sink.commit();
                    }
                    inner.committed_lsn = commit_lsn;
                    should_yield = true;
                }
                Event::Insert(insert) => {
                    let mut inner = self.inner.lock();
                    if let Some(sink) = inner.sinks.get_mut(&insert.table_id) {
                        sink.write_event(Event::Insert(insert))?;
                    }
                }
                Event::Update(update) => {
                    let mut inner = self.inner.lock();
                    if let Some(sink) = inner.sinks.get_mut(&update.table_id) {
                        sink.write_event(Event::Update(update))?;
                    }
                }
                Event::Delete(delete) => {
                    let mut inner = self.inner.lock();
                    if let Some(sink) = inner.sinks.get_mut(&delete.table_id) {
                        sink.write_event(Event::Delete(delete))?;
                    }
                }
                Event::Relation(..) => {}
                Event::Truncate(truncate) => {
                    let mut inner = self.inner.lock();
                    for rel_id in truncate.rel_ids {
                        if let Some(sink) = inner.sinks.get_mut(&rel_id.into()) {
                            sink.truncate();
                        }
                    }
                }
                Event::Unsupported => {}
            }
            if should_yield {
                // let consumers catch up
                // CR alee: possibly we could make this more efficient by tuning when we
                // yield to the size of our bcast channel? not sure if that's true...
                tokio::task::yield_now().await;
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LIndex1;
    use etl::state::table::TableReplicationPhase;
    use etl::types::PgLsn;
    use etl_postgres::types::{TableId, TableSchema};

    #[derive(Debug, Clone, BTreeMapped)]
    #[btreemap(index = ["key"])]
    struct TestRow {
        key: String,
    }

    fn make_table_id(oid: u32) -> TableId {
        TableId::new(oid)
    }

    fn make_table_schema(id: u32, schema: &str, name: &str) -> TableSchema {
        TableSchema {
            id: make_table_id(id),
            name: TableName { schema: schema.to_string(), name: name.to_string() },
            column_schemas: vec![],
        }
    }

    fn make_table_schema_with_columns(
        id: u32,
        schema: &str,
        name: &str,
    ) -> TableSchema {
        use etl::types::Type;
        use etl_postgres::types::ColumnSchema;

        TableSchema {
            id: make_table_id(id),
            name: TableName { schema: schema.to_string(), name: name.to_string() },
            column_schemas: vec![ColumnSchema {
                name: "key".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: false,
                primary: true,
            }],
        }
    }

    #[test]
    fn test_normalized_table_name_public_schema() {
        let tn =
            TableName { schema: "public".to_string(), name: "users".to_string() };
        assert_eq!(normalized_table_name(&tn), "users");
    }

    #[test]
    fn test_normalized_table_name_custom_schema() {
        let tn =
            TableName { schema: "custom".to_string(), name: "users".to_string() };
        let result = normalized_table_name(&tn);
        assert!(result.contains("custom"));
        assert!(result.contains("users"));
    }

    #[test]
    fn test_replicator_default() {
        let r1 = BTreeMapReplicator::new();
        let r2 = BTreeMapReplicator::default();
        drop(r1);
        drop(r2);
    }

    #[test]
    fn test_add_replica() {
        let replicator = BTreeMapReplicator::new();
        let _replica = replicator.add_replica::<TestRow, 1>("test_table");
        let inner = replicator.inner.lock();
        assert!(inner.pending_sinks.contains_key("test_table"));
    }

    #[tokio::test]
    async fn test_state_store_table_replication_state() {
        let store = BTreeMapReplicator::new();
        let table_id = make_table_id(1);

        // Initially no state
        let state = store.get_table_replication_state(table_id).await.unwrap();
        assert!(state.is_none());

        // Set state
        store
            .update_table_replication_state(table_id, TableReplicationPhase::Init)
            .await
            .unwrap();
        let state = store.get_table_replication_state(table_id).await.unwrap();
        assert!(matches!(state, Some(TableReplicationPhase::Init)));
    }

    #[tokio::test]
    async fn test_state_store_get_all_states() {
        let store = BTreeMapReplicator::new();
        let t1 = make_table_id(1);
        let t2 = make_table_id(2);

        store
            .update_table_replication_state(t1, TableReplicationPhase::Init)
            .await
            .unwrap();
        store
            .update_table_replication_state(t2, TableReplicationPhase::DataSync)
            .await
            .unwrap();

        let states = store.get_table_replication_states().await.unwrap();
        assert_eq!(states.len(), 2);
    }

    #[tokio::test]
    async fn test_state_store_load_count() {
        let store = BTreeMapReplicator::new();
        assert_eq!(store.load_table_replication_states().await.unwrap(), 0);

        store
            .update_table_replication_state(
                make_table_id(1),
                TableReplicationPhase::Init,
            )
            .await
            .unwrap();
        assert_eq!(store.load_table_replication_states().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_state_store_update_history_and_rollback() {
        let store = BTreeMapReplicator::new();
        let tid = make_table_id(1);

        store
            .update_table_replication_state(tid, TableReplicationPhase::Init)
            .await
            .unwrap();
        store
            .update_table_replication_state(tid, TableReplicationPhase::DataSync)
            .await
            .unwrap();

        // Rollback should go back to Init
        let prev = store.rollback_table_replication_state(tid).await.unwrap();
        assert!(matches!(prev, TableReplicationPhase::Init));

        // Current state should now be Init
        let state = store.get_table_replication_state(tid).await.unwrap();
        assert!(matches!(state, Some(TableReplicationPhase::Init)));
    }

    #[tokio::test]
    async fn test_state_store_rollback_no_history() {
        let store = BTreeMapReplicator::new();
        let tid = make_table_id(1);

        // No state set at all
        let result = store.rollback_table_replication_state(tid).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_state_store_table_mappings() {
        let store = BTreeMapReplicator::new();
        let tid = make_table_id(1);

        // No mapping initially
        assert!(store.get_table_mapping(&tid).await.unwrap().is_none());
        assert_eq!(store.load_table_mappings().await.unwrap(), 0);

        // Store a mapping
        store.store_table_mapping(tid, "dest_table".to_string()).await.unwrap();

        assert_eq!(
            store.get_table_mapping(&tid).await.unwrap(),
            Some("dest_table".to_string())
        );
        assert_eq!(store.load_table_mappings().await.unwrap(), 1);

        let mappings = store.get_table_mappings().await.unwrap();
        assert_eq!(mappings.len(), 1);
    }

    #[tokio::test]
    async fn test_schema_store() {
        let store = BTreeMapReplicator::new();
        let schema = make_table_schema(1, "public", "test_table");
        let tid = make_table_id(1);

        // No schema initially
        assert!(store.get_table_schema(&tid).await.unwrap().is_none());
        assert_eq!(store.load_table_schemas().await.unwrap(), 0);

        // Store schema
        store.store_table_schema(schema).await.unwrap();

        assert!(store.get_table_schema(&tid).await.unwrap().is_some());
        assert_eq!(store.load_table_schemas().await.unwrap(), 1);

        let schemas = store.get_table_schemas().await.unwrap();
        assert_eq!(schemas.len(), 1);
    }

    #[tokio::test]
    async fn test_schema_store_duplicate_rejected() {
        let store = BTreeMapReplicator::new();
        // Register a pending sink so that the first store_table_schema creates a sink
        let _replica = store.add_replica::<TestRow, 1>("test_table");
        let schema1 = make_table_schema(1, "public", "test_table");
        let schema2 = make_table_schema(1, "public", "test_table");

        store.store_table_schema(schema1).await.unwrap();
        // Storing the same table_id again should fail because a sink exists
        let result = store.store_table_schema(schema2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_store_assigns_sink() {
        let store = BTreeMapReplicator::new();
        let _replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema(1, "public", "test_table");

        store.store_table_schema(schema).await.unwrap();

        // Pending sink should be removed, and a real sink should exist
        let inner = store.inner.lock();
        assert!(!inner.pending_sinks.contains_key("test_table"));
        assert!(inner.sinks.contains_key(&make_table_id(1)));
    }

    #[tokio::test]
    async fn test_cleanup_store() {
        let store = BTreeMapReplicator::new();
        let tid = make_table_id(1);

        store
            .update_table_replication_state(tid, TableReplicationPhase::Init)
            .await
            .unwrap();
        store.store_table_mapping(tid, "dest".to_string()).await.unwrap();
        store.store_table_schema(make_table_schema(1, "public", "t")).await.unwrap();

        // Cleanup
        store.cleanup_table_state(tid).await.unwrap();

        assert!(store.get_table_replication_state(tid).await.unwrap().is_none());
        assert!(store.get_table_mapping(&tid).await.unwrap().is_none());
        assert!(store.get_table_schema(&tid).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_destination_name() {
        assert_eq!(BTreeMapReplicator::name(), "btreemapped");
    }

    #[tokio::test]
    async fn test_fully_synced_notification() {
        let store = BTreeMapReplicator::new();
        let tid = make_table_id(1);

        store
            .update_table_replication_state(tid, TableReplicationPhase::Init)
            .await
            .unwrap();

        // Not synced yet
        {
            let inner = store.inner.lock();
            assert!(!inner.fully_synced);
        }

        // Set to SyncDone
        store
            .update_table_replication_state(
                tid,
                TableReplicationPhase::SyncDone { lsn: PgLsn::from(0u64) },
            )
            .await
            .unwrap();

        // Now fully synced
        {
            let inner = store.inner.lock();
            assert!(inner.fully_synced);
        }
    }

    #[tokio::test]
    async fn test_destination_truncate() {
        let store = BTreeMapReplicator::new();
        let _replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        // Truncate should succeed even with empty table
        store.truncate_table(tid).await.unwrap();
    }

    #[tokio::test]
    async fn test_destination_truncate_no_sink() {
        let store = BTreeMapReplicator::new();
        // Truncate with no sink should be a no-op
        store.truncate_table(make_table_id(999)).await.unwrap();
    }

    #[tokio::test]
    async fn test_destination_write_table_rows_no_sink() {
        let store = BTreeMapReplicator::new();
        // Write rows with no sink should be a no-op
        store
            .write_table_rows(make_table_id(999), vec![])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_destination_write_events_begin_commit() {
        use etl::types::{BeginEvent, CommitEvent, Event};

        let store = BTreeMapReplicator::new();
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        store.write_events(events).await.unwrap();
    }

    #[tokio::test]
    async fn test_destination_write_events_commit_without_begin() {
        use etl::types::{CommitEvent, Event};

        let store = BTreeMapReplicator::new();
        let events = vec![Event::Commit(CommitEvent {
            start_lsn: PgLsn::from(100u64),
            commit_lsn: PgLsn::from(200u64),
            flags: 0,
            end_lsn: 300u64,
            timestamp: 0,
        })];
        let result = store.write_events(events).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_destination_write_events_commit_lsn_mismatch() {
        use etl::types::{BeginEvent, CommitEvent, Event};

        let store = BTreeMapReplicator::new();
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(999u64), // mismatch
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        let result = store.write_events(events).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_destination_write_events_unsupported_and_relation() {
        use etl::types::Event;

        let store = BTreeMapReplicator::new();
        // These should be no-ops
        let events = vec![Event::Unsupported, Event::Relation(etl::types::RelationEvent {
            start_lsn: PgLsn::from(0u64),
            commit_lsn: PgLsn::from(0u64),
            table_schema: make_table_schema(1, "public", "t"),
        })];
        store.write_events(events).await.unwrap();
    }

    #[test]
    fn test_synced() {
        let store = BTreeMapReplicator::new();
        // Just verify synced() returns a valid notified future
        let _notified = store.synced();
    }

    #[tokio::test]
    async fn test_destination_write_events_insert_update_delete_no_sink() {
        use etl::types::{
            BeginEvent, CommitEvent, DeleteEvent, Event, InsertEvent,
            UpdateEvent,
        };

        let store = BTreeMapReplicator::new();
        let tid = make_table_id(1);
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                table_row: etl::types::TableRow { values: vec![] },
            }),
            Event::Update(UpdateEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                table_row: etl::types::TableRow { values: vec![] },
                old_table_row: None,
            }),
            Event::Delete(DeleteEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                old_table_row: None,
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        store.write_events(events).await.unwrap();
    }

    fn make_row(key: &str) -> etl::types::TableRow {
        etl::types::TableRow {
            values: vec![etl::types::Cell::String(key.to_string())],
        }
    }

    #[tokio::test]
    async fn test_destination_write_events_with_sink() {
        use etl::types::{
            BeginEvent, CommitEvent, DeleteEvent, Event, InsertEvent,
            UpdateEvent,
        };

        let store = BTreeMapReplicator::new();
        let replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                table_row: make_row("insert_key"),
            }),
            Event::Update(UpdateEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                table_row: make_row("updated_key"),
                old_table_row: Some((true, make_row("insert_key"))),
            }),
            Event::Delete(DeleteEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                old_table_row: Some((true, make_row("updated_key"))),
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        store.write_events(events).await.unwrap();

        // After commit, the delete should have been applied:
        // insert_key was inserted, then updated to updated_key, then deleted
        assert!(replica.get(("insert_key".to_string(),)).is_none());
        assert!(replica.get(("updated_key".to_string(),)).is_none());
    }

    #[tokio::test]
    async fn test_destination_write_events_with_sink_insert_only() {
        use etl::types::{BeginEvent, CommitEvent, Event, InsertEvent};

        let store = BTreeMapReplicator::new();
        let replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                table_row: make_row("hello"),
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        store.write_events(events).await.unwrap();

        // After commit, the insert should be visible
        let row = replica.get(("hello".to_string(),));
        assert!(row.is_some());
        assert_eq!(row.unwrap().key, "hello");
    }

    #[tokio::test]
    async fn test_destination_write_table_rows_with_sink() {
        let store = BTreeMapReplicator::new();
        let replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        store
            .write_table_rows(tid, vec![make_row("row1"), make_row("row2")])
            .await
            .unwrap();

        assert!(replica.get(("row1".to_string(),)).is_some());
        assert!(replica.get(("row2".to_string(),)).is_some());
    }

    #[tokio::test]
    async fn test_destination_truncate_with_sink() {
        let store = BTreeMapReplicator::new();
        let replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        store
            .write_table_rows(tid, vec![make_row("row1")])
            .await
            .unwrap();
        assert!(replica.get(("row1".to_string(),)).is_some());

        store.truncate_table(tid).await.unwrap();
        assert!(replica.get(("row1".to_string(),)).is_none());
    }

    #[tokio::test]
    async fn test_destination_write_events_empty_commit_with_sink() {
        use etl::types::{BeginEvent, CommitEvent, Event};

        let store = BTreeMapReplicator::new();
        let _replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        // Begin and Commit with no data events: exercises the empty txn_clog
        // path in sink.commit()
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        store.write_events(events).await.unwrap();
    }

    #[tokio::test]
    async fn test_destination_write_events_truncate_with_sink() {
        use etl::types::{Event, TruncateEvent};

        let store = BTreeMapReplicator::new();
        let replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        // First add some data
        store.write_table_rows(tid, vec![make_row("x")]).await.unwrap();
        assert!(replica.get(("x".to_string(),)).is_some());

        // Now truncate via event
        let events = vec![Event::Truncate(TruncateEvent {
            start_lsn: PgLsn::from(0u64),
            commit_lsn: PgLsn::from(0u64),
            options: 0,
            rel_ids: vec![1], // table_id oid
        })];
        store.write_events(events).await.unwrap();
        assert!(replica.get(("x".to_string(),)).is_none());
    }

    #[tokio::test]
    async fn test_destination_write_events_update_without_old_row() {
        use etl::types::{BeginEvent, CommitEvent, Event, InsertEvent, UpdateEvent};

        let store = BTreeMapReplicator::new();
        let replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                table_row: make_row("key1"),
            }),
            // Update without old_table_row (no key change)
            Event::Update(UpdateEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                table_row: make_row("key1"),
                old_table_row: None,
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        store.write_events(events).await.unwrap();
        assert!(replica.get(("key1".to_string(),)).is_some());
    }

    #[tokio::test]
    async fn test_destination_write_events_delete_without_old_row() {
        use etl::types::{BeginEvent, CommitEvent, DeleteEvent, Event};

        let store = BTreeMapReplicator::new();
        let _replica = store.add_replica::<TestRow, 1>("test_table");
        let schema = make_table_schema_with_columns(1, "public", "test_table");
        store.store_table_schema(schema).await.unwrap();

        let tid = make_table_id(1);
        // Delete without old_table_row should be a no-op
        let events = vec![
            Event::Begin(BeginEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                timestamp: 0,
                xid: 1,
            }),
            Event::Delete(DeleteEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                table_id: tid,
                old_table_row: None,
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(200u64),
                flags: 0,
                end_lsn: 300u64,
                timestamp: 0,
            }),
        ];
        store.write_events(events).await.unwrap();
    }

    #[tokio::test]
    async fn test_destination_write_events_truncate() {
        use etl::types::{Event, TruncateEvent};

        let store = BTreeMapReplicator::new();
        let events = vec![Event::Truncate(TruncateEvent {
            start_lsn: PgLsn::from(0u64),
            commit_lsn: PgLsn::from(0u64),
            options: 0,
            rel_ids: vec![999],
        })];
        // Should be fine even without sinks
        store.write_events(events).await.unwrap();
    }
}
