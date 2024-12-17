// TODO: think about naming and structure wrt the single table sink
// really, this is the primary sink, don't center the world around
// the sink table case

use crate::{sink::ErasedBTreeMapSink, BTreeMapReplica, BTreeMapSink, BTreeMapped};
use async_trait::async_trait;
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{Sink, SinkError},
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
    tokio_postgres::types::PgLsn,
};
use std::collections::{HashMap, HashSet};

pub struct MultiBTreeMapSink {
    sinks: HashMap<TableId, Box<dyn ErasedBTreeMapSink>>,
    pending_sinks: HashMap<String, Box<dyn ErasedBTreeMapSink>>,
    table_ids: HashMap<String, TableId>,
    committed_tables: HashSet<TableId>,
    committed_lsn: PgLsn,
    txn_lsn: Option<PgLsn>,
}

impl MultiBTreeMapSink {
    pub fn new() -> Self {
        Self {
            sinks: HashMap::new(),
            pending_sinks: HashMap::new(),
            table_ids: HashMap::new(),
            committed_tables: HashSet::new(),
            committed_lsn: PgLsn::from(0),
            txn_lsn: None,
        }
    }

    pub fn add_sink<T: BTreeMapped<N>, const N: usize>(
        &mut self,
        table_name: &str,
    ) -> BTreeMapReplica<T, N>
    where
        // CR alee: I don't think this bound should be necessary
        // to make BTreeMapReplica itself Clone...how do we convince
        // the compiler?
        T::LIndex: Clone,
    {
        let sink = BTreeMapSink::<T, N>::new(table_name);
        let replica = sink.replica.clone();
        self.pending_sinks.insert(table_name.to_string(), Box::new(sink));
        replica
    }
}

#[async_trait]
impl Sink for MultiBTreeMapSink {
    async fn get_resumption_state(
        &mut self,
    ) -> Result<PipelineResumptionState, SinkError> {
        Ok(PipelineResumptionState {
            copied_tables: self.committed_tables.clone(),
            last_lsn: self.committed_lsn,
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError> {
        #[cfg(feature = "log")]
        log::trace!("write_table_schemas: {:?}", table_schemas);
        for table_schema in table_schemas {
            let (table_id, schema) = table_schema;
            if let Some(sink) = self.sinks.get_mut(&table_id) {
                sink.set_table_id_and_schema(table_id, schema);
            } else if let Some(mut sink) =
                self.pending_sinks.remove(&schema.table_name.name)
            {
                let table_name = schema.table_name.name.clone();
                #[cfg(feature = "log")]
                log::debug!(
                    "table {} sink assigned, table_id = {}",
                    table_name,
                    table_id
                );
                sink.set_table_id_and_schema(table_id, schema);
                self.sinks.insert(table_id, sink);
                self.table_ids.insert(table_name, table_id);
            }
        }
        Ok(())
    }

    async fn write_table_row(
        &mut self,
        row: TableRow,
        table_id: TableId,
    ) -> Result<(), SinkError> {
        if let Some(sink) = self.sinks.get_mut(&table_id) {
            sink.write_table_row(row, table_id).await?;
        }
        Ok(())
    }

    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, SinkError> {
        #[cfg(feature = "log")]
        log::trace!("write_cdc_event: {:?}", event);
        match event {
            CdcEvent::Begin(begin) => {
                let final_lsn_u64 = begin.final_lsn();
                self.txn_lsn = Some(final_lsn_u64.into());
            }
            CdcEvent::Commit(commit) => {
                let commit_lsn: PgLsn = commit.commit_lsn().into();
                if let Some(final_lsn) = self.txn_lsn {
                    if commit_lsn == final_lsn {
                        for sink in self.sinks.values_mut() {
                            sink.commit(commit_lsn);
                        }
                        self.committed_lsn = commit_lsn;
                    } else {
                        Err(SinkError::IncorrectCommitLsn(commit_lsn, final_lsn))?
                    }
                } else {
                    Err(SinkError::CommitWithoutBegin)?
                }
            }
            CdcEvent::Insert((tid, _)) => {
                if let Some(sink) = self.sinks.get_mut(&tid) {
                    sink.write_cdc_event(event).await?;
                } else {
                    #[cfg(feature = "log")]
                    log::trace!(
                        "insert for unknown table_id: {}, known tables are: {:?}",
                        tid,
                        self.table_ids
                    );
                }
            }
            CdcEvent::Update { table_id, .. } => {
                if let Some(sink) = self.sinks.get_mut(&table_id) {
                    sink.write_cdc_event(event).await?;
                } else {
                    #[cfg(feature = "log")]
                    log::trace!(
                        "update for unknown table_id: {}, known tables are: {:?}",
                        table_id,
                        self.table_ids
                    );
                }
            }
            CdcEvent::Delete((tid, _)) => {
                if let Some(sink) = self.sinks.get_mut(&tid) {
                    sink.write_cdc_event(event).await?;
                } else {
                    #[cfg(feature = "log")]
                    log::trace!(
                        "delete for unknown table_id: {}, known tables are: {:?}",
                        tid,
                        self.table_ids
                    );
                }
            }
            CdcEvent::Type(_) => {}
            CdcEvent::Relation(_) => {}
            CdcEvent::KeepAliveRequested { reply: _ } => {}
        }
        #[cfg(feature = "log")]
        log::trace!("committed_lsn: {}", self.committed_lsn);
        Ok(self.committed_lsn)
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError> {
        if let Some(sink) = self.sinks.get_mut(&table_id) {
            sink.table_copied(table_id).await?;
        }
        self.committed_tables.insert(table_id);
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError> {
        if let Some(sink) = self.sinks.get_mut(&table_id) {
            sink.truncate_table(table_id).await?;
        }
        Ok(())
    }
}
