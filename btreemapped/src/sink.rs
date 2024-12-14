//! pg_replicate sink implementation for BTreeMapped -> BTreeMapReplica

use crate::{BTreeMapReplica, BTreeMapped, BTreeUpdate};
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
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct BTreeMapSink<T: BTreeMapped<N>, const N: usize> {
    pub replica: BTreeMapReplica<T, N>,
    committed_tables: HashSet<TableId>,
    committed_lsn: PgLsn,
    txn_lsn: Option<PgLsn>,
    txn_clog: Vec<Result<T, T::Index>>,
    table_id: Option<TableId>,
    table_name: String,
    table_schema: Option<TableSchema>,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapSink<T, N> {
    pub fn new(table_name: &str) -> Self {
        let seqid = rand::random::<u64>();
        let replica = BTreeMapReplica::new(seqid);
        Self {
            replica,
            committed_tables: HashSet::new(),
            committed_lsn: PgLsn::from(0),
            txn_lsn: None,
            txn_clog: vec![],
            table_id: None,
            table_name: table_name.to_string(),
            table_schema: None,
        }
    }
}

fn parse_row<T: BTreeMapped<N>, const N: usize>(
    schema: &TableSchema,
    row: TableRow,
) -> Result<T, SinkError> {
    match T::parse_row(schema, row) {
        Ok(t) => Ok(t),
        Err(_e) => {
            #[cfg(feature = "log")]
            log::error!("parse_row returned error: {_e:?}");
            Err(SinkError::GenericSinkError)
        }
    }
}

fn parse_row_index<T: BTreeMapped<N>, const N: usize>(
    schema: &TableSchema,
    row: TableRow,
) -> Result<T::Index, SinkError> {
    match T::parse_row_index(schema, row) {
        Ok(index) => Ok(index),
        Err(_e) => {
            #[cfg(feature = "log")]
            log::error!("parse_row returned error: {_e:?}");
            Err(SinkError::GenericSinkError)
        }
    }
}

// MultiBTreeMapSink need to be able to manipulate the inner state
// of the individual sinks; also erase the types so we can put them
// in Box<dyn>s.
pub(crate) trait ErasedBTreeMapSink: Sink + Send {
    fn set_table_id_and_schema(&mut self, table_id: TableId, schema: TableSchema);
    fn commit(&mut self, commit_lsn: PgLsn);
}

impl<T: BTreeMapped<N>, const N: usize> ErasedBTreeMapSink for BTreeMapSink<T, N> {
    fn set_table_id_and_schema(&mut self, table_id: TableId, schema: TableSchema) {
        self.table_id = Some(table_id);
        self.table_schema = Some(schema);
    }

    fn commit(&mut self, commit_lsn: PgLsn) {
        let mut updates = vec![];
        if let Some((seqid, seqno)) = if self.txn_clog.is_empty() {
            self.committed_lsn = commit_lsn;
            None
        } else {
            let mut replica = self.replica.write();
            for chg in self.txn_clog.drain(..) {
                match chg {
                    Ok(t) => {
                        let i = t.index();
                        replica.insert(i.clone().into(), t.clone());
                        updates.push((i, Some(t)));
                    }
                    Err(i) => {
                        replica.remove(&i.clone().into());
                        updates.push((i, None));
                    }
                }
            }
            replica.seqno += 1;
            Some((replica.seqid, replica.seqno))
        } {
            self.committed_lsn = commit_lsn;
            let _ = self.replica.changed.send_replace((seqid, seqno));
            if let Err(_) = self.replica.updates.send(Arc::new(BTreeUpdate {
                seqid,
                seqno,
                snapshot: None,
                updates,
            })) {
                // nobody listening, fine
            }
        }
    }
}

// CR alee: implement BatchSink
#[async_trait]
impl<T: BTreeMapped<N>, const N: usize> Sink for BTreeMapSink<T, N> {
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
        for (id, schema) in table_schemas {
            #[cfg(feature = "log")]
            log::trace!("write_table_schemas: {:?}", schema);
            if schema.table_name.name == self.table_name {
                self.table_id = Some(id);
                self.table_schema = Some(schema);
            }
        }
        Ok(())
    }

    async fn write_table_row(
        &mut self,
        row: TableRow,
        table_id: TableId,
    ) -> Result<(), SinkError> {
        #[cfg(feature = "log")]
        log::trace!("write_table_row to table {table_id}: {:?}", row);
        if self.table_id.is_some_and(|id| id == table_id) {
            let schema = self.table_schema.as_ref().unwrap();
            let t = parse_row::<T, N>(schema, row)?;
            let index = t.index();
            let (seqid, seqno) = {
                let mut replica = self.replica.write();
                replica.insert(index.clone().into(), t.clone());
                replica.seqno += 1;
                (replica.seqid, replica.seqno)
            };
            let _ = self.replica.changed.send_replace((seqid, seqno));
            if let Err(_) = self.replica.updates.send(Arc::new(BTreeUpdate {
                seqid,
                seqno,
                snapshot: None,
                updates: vec![(index, Some(t))],
            })) {
                // nobody listening, fine
            }
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
                        self.commit(commit_lsn);
                    } else {
                        Err(SinkError::IncorrectCommitLsn(commit_lsn, final_lsn))?
                    }
                } else {
                    Err(SinkError::CommitWithoutBegin)?
                }
            }
            CdcEvent::Insert((tid, row)) => {
                if self.table_id.is_some_and(|id| id == tid) {
                    let schema = self.table_schema.as_ref().unwrap();
                    let t = parse_row::<T, N>(schema, row)?;
                    self.txn_clog.push(Ok(t));
                }
            }
            CdcEvent::Update { table_id, old_row: _, key_row, row } => {
                if self.table_id.is_some_and(|id| id == table_id) {
                    let schema = self.table_schema.as_ref().unwrap();
                    if let Some(key) = key_row {
                        let i = parse_row_index::<T, N>(schema, key)?;
                        self.txn_clog.push(Err(i));
                    }
                    let t = parse_row::<T, N>(schema, row)?;
                    self.txn_clog.push(Ok(t));
                }
            }
            CdcEvent::Delete((tid, row)) => {
                if self.table_id.is_some_and(|id| id == tid) {
                    let schema = self.table_schema.as_ref().unwrap();
                    let i = parse_row_index::<T, N>(schema, row)?;
                    self.txn_clog.push(Err(i));
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
        self.committed_tables.insert(table_id);
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError> {
        if self.table_id.is_some_and(|id| id == table_id) {
            let (seqid, seqno) = {
                let mut replica = self.replica.write();
                replica.clear();
                replica.seqno += 1;
                (replica.seqid, replica.seqno)
            };
            let _ = self.replica.changed.send_replace((seqid, seqno));
            if let Err(_) = self.replica.updates.send(Arc::new(BTreeUpdate {
                seqid,
                seqno,
                snapshot: Some(vec![]),
                updates: vec![],
            })) {
                // nobody listening, fine
            }
        }
        Ok(())
    }
}
