use crate::{
    replication_state::TableMetadata, BTreeMapReplica, BTreeMapped, BTreeUpdate,
};
use etl::{
    destination::Destination,
    error::{ErrorKind, EtlError, EtlResult},
    types::{Event, TableId, TableRow, TableSchema},
};
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

pub struct BTreeMapSink<T: BTreeMapped<N>, const N: usize> {
    pub replica: BTreeMapReplica<T, N>,
    // sink_state: SinkState,
    committed_lsn: AtomicU64,
    // NB: LSN 0/0 shouldn't be encountered in practice, and
    // is used synonymously with `None` in our logic.
    txn_lsn: AtomicU64,
    txn_clog: Mutex<Vec<Result<T, T::Index>>>,
    table_name: String,
    table_metadata: Arc<OnceLock<TableMetadata>>,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapSink<T, N> {
    pub fn new(table_name: &str) -> Self {
        let seqid = rand::random::<u64>();
        let replica = BTreeMapReplica::new(seqid);
        // let sink_state = SinkState::new();
        Self {
            replica,
            // sink_state,
            committed_lsn: AtomicU64::new(0),
            txn_lsn: AtomicU64::new(0),
            txn_clog: Mutex::new(vec![]),
            table_name: table_name.to_string(),
            table_metadata: Arc::new(OnceLock::new()),
        }
    }
}

fn parse_row<T: BTreeMapped<N>, const N: usize>(
    schema: &TableSchema,
    row: TableRow,
) -> Option<T> {
    match T::parse_row(schema, row) {
        Ok(t) => Some(t),
        // TODO: we can probably return an error and propagate it now
        Err(_e) => {
            #[cfg(feature = "log")]
            log::error!("parse_row returned error: {_e:?}");
            None
        }
    }
}

fn parse_row_index<T: BTreeMapped<N>, const N: usize>(
    schema: &TableSchema,
    row: TableRow,
) -> Option<T::Index> {
    match T::parse_row_index(schema, row) {
        Ok(index) => Some(index),
        Err(_e) => {
            #[cfg(feature = "log")]
            log::error!("parse_row returned error: {_e:?}");
            None
        }
    }
}

// MultiBTreeMapSink need to be able to manipulate the inner state
// of the individual sinks; also erase the types so we can put them
// in Box<dyn>s.
pub(crate) trait ErasedBTreeMapSink: Destination + Send {
    fn commit(&self, commit_lsn: u64);
}

impl<T: BTreeMapped<N>, const N: usize> ErasedBTreeMapSink for BTreeMapSink<T, N> {
    fn commit(&self, commit_lsn: u64) {
        let mut txn_clog = self.txn_clog.lock();
        let mut updates = vec![];
        if let Some((seqid, seqno)) = if txn_clog.is_empty() {
            self.committed_lsn.store(commit_lsn, Ordering::SeqCst);
            None
        } else {
            let mut replica = self.replica.write();
            for chg in txn_clog.drain(..) {
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
            self.committed_lsn.store(commit_lsn, Ordering::SeqCst);
            let _ = self.replica.sequence.send_replace((seqid, seqno));
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

impl<T: BTreeMapped<N>, const N: usize> Destination for BTreeMapSink<T, N> {
    fn name() -> &'static str {
        "btreemap"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        todo!()
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        #[cfg(feature = "log")]
        log::trace!("write_table_rows to table {table_id}: {} rows", rows.len());
        if let Some(meta) = self.table_metadata.get() {
            if meta.table_id == table_id {
                let mut updates = vec![];
                let (seqid, seqno) = {
                    let mut replica = self.replica.write();
                    for row in rows {
                        if let Some(t) = parse_row::<T, N>(&meta.table_schema, row) {
                            let index = t.index();
                            replica.insert(index.clone().into(), t.clone());
                            updates.push((index, Some(t)));
                        }
                    }
                    replica.seqno += 1;
                    (replica.seqid, replica.seqno)
                };
                let _ = self.replica.sequence.send_replace((seqid, seqno));
                if let Err(_) = self.replica.updates.send(Arc::new(BTreeUpdate {
                    seqid,
                    seqno,
                    snapshot: None,
                    updates,
                })) {
                    // nobody listening, fine
                }
                // let consumers catch up
                // CR alee: possibly we could make this more efficient by tuning when we
                // yield to the size of our bcast channel? not sure if that's true...
                tokio::task::yield_now().await;
            }
        }
        EtlResult::Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        for event in events {
            #[cfg(feature = "log")]
            log::trace!("write_events: {:?}", event);
            match event {
                Event::Begin(begin) => {
                    self.txn_lsn.store(begin.commit_lsn.into(), Ordering::SeqCst);
                }
                Event::Commit(commit) => {
                    let txn_lsn = self.txn_lsn.load(Ordering::SeqCst);
                    let commit_lsn: u64 = commit.commit_lsn.into();
                    if txn_lsn == 0 {
                        Err::<_, EtlError>(
                            (
                                ErrorKind::InvalidState,
                                "commit event without preceding begin",
                            )
                                .into(),
                        )?;
                    } else if txn_lsn != commit_lsn {
                        Err::<_, EtlError>(
                            (ErrorKind::InvalidState, "commit event with incorrect LSN")
                                .into(),
                        )?;
                    } else {
                        // INVARIANT: txn_lsn == commit_lsn
                        self.commit(commit_lsn);
                        // let consumers catch up
                        tokio::task::yield_now().await;
                    }
                }
                Event::Insert(insert) => {
                    if let Some(meta) = self.table_metadata.get() {
                        if meta.table_id == insert.table_id {
                            if let Some(t) =
                                parse_row::<T, N>(&meta.table_schema, insert.table_row)
                            {
                                let mut txn_clog = self.txn_clog.lock();
                                txn_clog.push(Ok(t));
                            }
                        }
                    }
                }
                Event::Update(update) => {
                    if let Some(meta) = self.table_metadata.get() {
                        if meta.table_id == update.table_id {
                            let mut index = Ok(None);
                            if let Some((_, key)) = update.old_table_row {
                                if let Some(i) =
                                    parse_row_index::<T, N>(&meta.table_schema, key)
                                {
                                    index = Ok(Some(i));
                                } else {
                                    index = Err(());
                                }
                            }
                            if let Ok(index) = index {
                                if let Some(t) = parse_row::<T, N>(
                                    &meta.table_schema,
                                    update.table_row,
                                ) {
                                    let mut txn_clog = self.txn_clog.lock();
                                    if let Some(i) = index {
                                        txn_clog.push(Err(i));
                                    }
                                    txn_clog.push(Ok(t));
                                }
                            }
                        }
                    }
                }
                Event::Delete(delete) => {
                    if let Some(meta) = self.table_metadata.get() {
                        if meta.table_id == delete.table_id {
                            // CR alee: under what conditions is old_table_row not Some(..)?
                            if let Some((_, key)) = delete.old_table_row {
                                if let Some(i) =
                                    parse_row_index::<T, N>(&meta.table_schema, key)
                                {
                                    let mut txn_clog = self.txn_clog.lock();
                                    txn_clog.push(Err(i));
                                }
                            }
                        }
                    }
                }
                Event::Relation(..) => {}
                Event::Truncate(_truncate) => {
                    // TODO
                }
                Event::Unsupported => {}
            }
        }
        EtlResult::Ok(())
    }
}
