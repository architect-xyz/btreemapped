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
    txn_clog: Vec<Result<(T::Index, T::Unindexed), T::Index>>,
    table_id: Option<TableId>,
    table_name: String,
    table_schema: Option<TableSchema>,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapSink<T, N> {
    pub fn new(seqid: u64, table_name: &str) -> Self {
        let mut replica = BTreeMapReplica::new();
        replica.set_seqid(seqid);
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
        if self.table_id.is_some_and(|id| id == table_id) {
            let schema = self.table_schema.as_ref().unwrap();
            if let (Some(index), Some(unindexed)) = T::parse_row(schema, row) {
                let (seqid, seqno) = {
                    let mut replica = self.replica.write().unwrap();
                    replica.insert(index.clone().into(), unindexed.clone());
                    replica.seqno += 1;
                    (replica.seqid, replica.seqno)
                };
                self.replica.changed.notify_waiters();
                if let Err(_) = self.replica.updates.send(Arc::new(BTreeUpdate {
                    seqid,
                    seqno,
                    snapshot: None,
                    updates: vec![(index, Some(unindexed))],
                })) {
                    // nobody listening, fine
                }
            } else {
                Err(SinkError::GenericSinkError)?;
            }
        }
        Ok(())
    }

    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, SinkError> {
        match event {
            CdcEvent::Begin(begin) => {
                let final_lsn_u64 = begin.final_lsn();
                self.txn_lsn = Some(final_lsn_u64.into());
            }
            CdcEvent::Commit(commit) => {
                let commit_lsn: PgLsn = commit.commit_lsn().into();
                if let Some(final_lsn) = self.txn_lsn {
                    if commit_lsn == final_lsn {
                        let mut updates = vec![];
                        let (seqid, seqno) = {
                            let mut replica = self.replica.write().unwrap();
                            for chg in self.txn_clog.drain(..) {
                                match chg {
                                    Ok((i, u)) => {
                                        replica.insert(i.clone().into(), u.clone());
                                        updates.push((i, Some(u)));
                                    }
                                    Err(i) => {
                                        replica.remove(&i.clone().into());
                                        updates.push((i, None));
                                    }
                                }
                            }
                            replica.seqno += 1;
                            (replica.seqid, replica.seqno)
                        };
                        self.committed_lsn = commit_lsn;
                        self.replica.changed.notify_waiters();
                        if let Err(_) = self.replica.updates.send(Arc::new(BTreeUpdate {
                            seqid,
                            seqno,
                            snapshot: None,
                            updates,
                        })) {
                            // nobody listening, fine
                        }
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
                    if let (Some(i), Some(u)) = T::parse_row(schema, row) {
                        self.txn_clog.push(Ok((i, u)));
                    }
                }
            }
            CdcEvent::Update { table_id, old_row: _, key_row, row } => {
                if self.table_id.is_some_and(|id| id == table_id) {
                    let schema = self.table_schema.as_ref().unwrap();
                    if let Some(key) = key_row {
                        if let (Some(key), _) = T::parse_row(schema, key) {
                            self.txn_clog.push(Err(key));
                        }
                    }
                    if let (Some(i), Some(u)) = T::parse_row(schema, row) {
                        self.txn_clog.push(Ok((i, u)));
                    }
                }
            }
            CdcEvent::Delete((tid, row)) => {
                if self.table_id.is_some_and(|id| id == tid) {
                    let schema = self.table_schema.as_ref().unwrap();
                    if let (Some(i), _) = T::parse_row(schema, row) {
                        self.txn_clog.push(Err(i));
                    }
                }
            }
            CdcEvent::Relation(_) => {}
            CdcEvent::KeepAliveRequested { reply: _ } => {}
        }
        Ok(self.committed_lsn)
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError> {
        self.committed_tables.insert(table_id);
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), SinkError> {
        if self.table_id.is_some_and(|id| id == table_id) {
            let (seqid, seqno) = {
                let mut replica = self.replica.write().unwrap();
                replica.clear();
                replica.seqno += 1;
                (replica.seqid, replica.seqno)
            };
            self.replica.changed.notify_waiters();
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
