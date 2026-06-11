use crate::{
    sink::{ErasedBTreeMapReplica, ErasedBTreeMapSink},
    BTreeMapReplica, BTreeMapped,
};
use etl::{
    config::PipelineConfig,
    destination::{
        Destination, DropTableForCopyResult, WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    pipeline::Pipeline,
    replication::WorkerType,
    state::{
        AppliedDestinationTableMetadata, DestinationTableMetadata,
        DestinationTableSchemaStatus, TableState,
    },
    store::{
        lifecycle::{TableStateLifecycleStore, TableStateOperation},
        schema::SchemaStore,
        state::{StateStore, TableStates},
        MemoryStore,
    },
    types::{
        Event, PgLsn, ReplicatedTableSchema, SnapshotId, TableId, TableName, TableRow,
        TableSchema,
    },
};
use futures::{pin_mut, select_biased, FutureExt};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{futures::OwnedNotified, Notify};
use tokio_util::sync::CancellationToken;

/// Inner state of [`BTreeMapReplicator`]
struct Inner {
    sinks: HashMap<TableId, Box<dyn ErasedBTreeMapSink>>,
    pending_sinks: HashMap<String, Box<dyn ErasedBTreeMapReplica>>,
    committed_lsn: u64,
    txn_lsn: Option<u64>,
    fully_synced: bool,
}

/// In-memory storage for ETL pipeline state and schema information.
///
/// [`BTreeMapReplicator`] implements the store traits required by
/// [`etl::store::PipelineStore`] by delegating to [`MemoryStore`], providing a
/// complete storage solution that keeps all data in memory. Replication state
/// is intentionally ephemeral: restarting the process does a fresh snapshot of
/// all replicated tables.
#[derive(Clone)]
pub struct BTreeMapReplicator {
    store: MemoryStore,
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
        };

        Self {
            store: MemoryStore::new(),
            inner: Arc::new(Mutex::new(inner)),
            synced: Arc::new(Notify::new()),
        }
    }

    pub fn add_replica<T: BTreeMapped<N>, const N: usize>(
        &self,
        table_name: &str,
    ) -> BTreeMapReplica<T, N> {
        let mut inner = self.inner.lock();
        let replica = BTreeMapReplica::new();
        inner
            .pending_sinks
            .insert(table_name.to_string(), Box::new(replica.clone()));
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

        let cancellation_token = cancellation_token.unwrap_or_default();
        let cancellation = cancellation_token.cancelled().fuse();
        pin_mut!(cancellation);

        select_biased! {
            _ = cancellation => {
                if shutdown_tx.shutdown().is_err() {
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

    /// Notify `synced` waiters once every table has finished its initial
    /// copy and catch-up (reached `SyncDone` or `Ready`).
    fn check_fully_synced(&self, table_states: &TableStates) {
        let all_synced = !table_states.is_empty()
            && table_states.values().all(|state| {
                matches!(state, TableState::SyncDone { .. } | TableState::Ready)
            });
        if all_synced {
            let mut inner = self.inner.lock();
            if !inner.fully_synced {
                inner.fully_synced = true;
                self.synced.notify_waiters();
            }
        }
    }

    /// Stores or advances destination table metadata for a table.
    ///
    /// The pipeline uses this metadata to know that a destination "object"
    /// (here: the in-memory replica) exists for the table, so it can ask us
    /// to drop it via [`Destination::drop_table_for_copy`] before restarting
    /// a table copy.
    async fn sync_destination_table_metadata(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = schema.id();
        let existing = self.store.get_destination_table_metadata(table_id).await?;
        let metadata = match existing {
            Some(metadata)
                if metadata.snapshot_id == schema.inner().snapshot_id
                    && metadata.replication_mask == *schema.replication_mask() =>
            {
                return Ok(());
            }
            Some(metadata) => metadata
                .with_schema_change(
                    schema.inner().snapshot_id,
                    schema.replication_mask().clone(),
                    DestinationTableSchemaStatus::Applied,
                )
                .to_applied(),
            None => DestinationTableMetadata::new_applied(
                normalized_table_name(schema.name()),
                schema.inner().snapshot_id,
                schema.replication_mask().clone(),
            ),
        };
        self.store
            .store_destination_table_metadata(table_id, metadata)
            .await?;

        Ok(())
    }
}

impl Default for BTreeMapReplicator {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for BTreeMapReplicator {
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        self.store.get_table_state(table_id).await
    }

    async fn get_table_states(&self) -> EtlResult<TableStates> {
        self.store.get_table_states().await
    }

    async fn load_table_states(&self) -> EtlResult<usize> {
        self.store.load_table_states().await
    }

    async fn update_table_states(
        &self,
        updates: Vec<(TableId, TableState)>,
    ) -> EtlResult<()> {
        self.store.update_table_states(updates).await?;

        let table_states = self.store.get_table_states().await?;
        self.check_fully_synced(&table_states);

        Ok(())
    }

    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        self.store.rollback_table_state(table_id).await
    }

    async fn get_replication_progress(
        &self,
        worker_type: WorkerType,
    ) -> EtlResult<Option<PgLsn>> {
        self.store.get_replication_progress(worker_type).await
    }

    async fn upsert_replication_progress(
        &self,
        worker_type: WorkerType,
        flush_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        self.store
            .upsert_replication_progress(worker_type, flush_lsn)
            .await
    }

    async fn delete_replication_progress(
        &self,
        worker_type: WorkerType,
    ) -> EtlResult<()> {
        self.store.delete_replication_progress(worker_type).await
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        self.store.get_destination_table_metadata(table_id).await
    }

    async fn get_applied_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<AppliedDestinationTableMetadata>> {
        self.store
            .get_applied_destination_table_metadata(table_id)
            .await
    }

    async fn load_destination_tables_metadata(&self) -> EtlResult<usize> {
        self.store.load_destination_tables_metadata().await
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        self.store
            .store_destination_table_metadata(table_id, metadata)
            .await
    }
}

impl SchemaStore for BTreeMapReplicator {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        self.store.get_table_schema(table_id, snapshot_id).await
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        self.store.get_table_schemas().await
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        self.store.load_table_schemas().await
    }

    async fn store_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> EtlResult<Arc<TableSchema>> {
        let table_schema = self.store.store_table_schema(table_schema).await?;

        #[cfg(feature = "log")]
        log::trace!("store_table_schema: {:?}", table_schema);

        // Assign a pending sink the first time we learn the table's id.
        // Schemas are versioned upstream and stored repeatedly (table copy,
        // relation messages, DDL); subsequent versions for an already-known
        // table are just recorded.
        let mut inner = self.inner.lock();
        if !inner.sinks.contains_key(&table_schema.id) {
            let sink_table_name = normalized_table_name(&table_schema.name);
            if let Some(replica) = inner.pending_sinks.remove(&sink_table_name) {
                #[cfg(feature = "log")]
                log::debug!(
                    "table {} sink assigned, table_id = {}",
                    sink_table_name,
                    table_schema.id
                );
                let sink = replica.to_sink();
                inner.sinks.insert(table_schema.id, sink);
            }
        }

        Ok(table_schema)
    }

    async fn prune_table_schemas(
        &self,
        table_schema_retentions: HashMap<
            TableId,
            etl::store::schema::TableSchemaRetention,
        >,
    ) -> EtlResult<u64> {
        self.store
            .prune_table_schemas(table_schema_retentions)
            .await
    }
}

impl TableStateLifecycleStore for BTreeMapReplicator {
    async fn apply_table_state_operation(
        &self,
        operation: TableStateOperation,
    ) -> EtlResult<usize> {
        let affected = self.store.apply_table_state_operation(operation).await?;

        if matches!(operation, TableStateOperation::ResetForResync) {
            // A resync redoes the initial copy of every table; arm the
            // `synced` notification again so waiters can observe completion
            // of the fresh pass.
            let mut inner = self.inner.lock();
            inner.fully_synced = false;
        }

        Ok(affected)
    }
}

impl Destination for BTreeMapReplicator {
    fn name() -> &'static str {
        "btreemapped"
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        {
            let mut inner = self.inner.lock();
            if let Some(sink) = inner.sinks.get_mut(&replicated_table_schema.id()) {
                sink.truncate();
            }
        }
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();
        #[cfg(feature = "log")]
        log::trace!(
            "write_table_rows to table {table_id}: {} rows",
            table_rows.len()
        );
        self.sync_destination_table_metadata(replicated_table_schema)
            .await?;
        let should_yield = {
            let mut inner = self.inner.lock();
            if let Some(sink) = inner.sinks.get_mut(&table_id) {
                sink.write_table_rows(replicated_table_schema, table_rows)?;
                true
            } else {
                false
            }
        };
        async_result.send(Ok(()));
        if should_yield {
            // let consumers catch up
            // CR alee: possibly we could make this more efficient by tuning when we
            // yield to the size of our bcast channel? not sure if that's true...
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        // Keep destination table metadata current for every schema version
        // seen in this batch, so the pipeline knows the replica exists (and
        // can prune obsolete schema versions).
        let mut seen_snapshots = HashSet::new();
        for event in &events {
            let schemas: &[ReplicatedTableSchema] = match event {
                Event::Insert(insert) => {
                    std::slice::from_ref(&insert.replicated_table_schema)
                }
                Event::Update(update) => {
                    std::slice::from_ref(&update.replicated_table_schema)
                }
                Event::Delete(delete) => {
                    std::slice::from_ref(&delete.replicated_table_schema)
                }
                Event::Relation(relation) => {
                    std::slice::from_ref(&relation.replicated_table_schema)
                }
                Event::Truncate(truncate) => &truncate.truncated_tables,
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => &[],
            };
            for schema in schemas {
                if seen_snapshots.insert((schema.id(), schema.inner().snapshot_id)) {
                    self.sync_destination_table_metadata(schema).await?;
                }
            }
        }

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
                    if let Some(sink) =
                        inner.sinks.get_mut(&insert.replicated_table_schema.id())
                    {
                        sink.write_event(Event::Insert(insert))?;
                    }
                }
                Event::Update(update) => {
                    let mut inner = self.inner.lock();
                    if let Some(sink) =
                        inner.sinks.get_mut(&update.replicated_table_schema.id())
                    {
                        sink.write_event(Event::Update(update))?;
                    }
                }
                Event::Delete(delete) => {
                    let mut inner = self.inner.lock();
                    if let Some(sink) =
                        inner.sinks.get_mut(&delete.replicated_table_schema.id())
                    {
                        sink.write_event(Event::Delete(delete))?;
                    }
                }
                Event::Relation(..) => {}
                Event::Truncate(truncate) => {
                    let mut inner = self.inner.lock();
                    for schema in truncate.truncated_tables {
                        if let Some(sink) = inner.sinks.get_mut(&schema.id()) {
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
        async_result.send(Ok(()));
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
