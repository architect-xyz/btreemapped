use crate::{BTreeMapReplica, BTreeMapped, BTreeUpdate};
use etl::{
    error::EtlResult,
    types::{Event, TableRow, TableSchema},
};
use std::sync::Arc;

pub struct BTreeMapSink<T: BTreeMapped<N>, const N: usize> {
    pub replica: BTreeMapReplica<T, N>,
    txn_clog: Vec<Result<T, T::Index>>,
    table_schema: Arc<TableSchema>,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapSink<T, N> {
    pub fn new(replica: BTreeMapReplica<T, N>, table_schema: Arc<TableSchema>) -> Self {
        Self {
            replica,
            txn_clog: vec![],
            table_schema,
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

pub(crate) trait ErasedBTreeMapReplica: Send {
    fn to_sink(&self, table_schema: Arc<TableSchema>) -> Box<dyn ErasedBTreeMapSink>;
}

impl<T: BTreeMapped<N>, const N: usize> ErasedBTreeMapReplica for BTreeMapReplica<T, N> {
    fn to_sink(&self, table_schema: Arc<TableSchema>) -> Box<dyn ErasedBTreeMapSink> {
        Box::new(BTreeMapSink::new(self.clone(), table_schema))
    }
}

// MultiBTreeMapSink need to be able to manipulate the inner state
// of the individual sinks; also erase the types so we can put them
// in Box<dyn>s.
pub(crate) trait ErasedBTreeMapSink: Send {
    fn commit(&mut self);

    fn truncate(&self);

    fn write_table_rows(&self, rows: Vec<TableRow>) -> EtlResult<()>;

    fn write_event(&mut self, events: Event) -> EtlResult<()>;
}

impl<T: BTreeMapped<N>, const N: usize> ErasedBTreeMapSink for BTreeMapSink<T, N> {
    fn commit(&mut self) {
        let mut updates = vec![];
        if let Some((seqid, seqno)) = if self.txn_clog.is_empty() {
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
            let _ = self.replica.sequence.send_replace((seqid, seqno));
            // nobody listening, fine
            let _ = self.replica.updates.send(Arc::new(BTreeUpdate {
                seqid,
                seqno,
                snapshot: None,
                updates,
            }));
        }
    }

    fn truncate(&self) {
        let mut replica = self.replica.write();
        replica.clear();
        replica.seqno += 1;
        let (seqid, seqno) = (replica.seqid, replica.seqno);
        let _ = self.replica.sequence.send_replace((seqid, seqno));
        // nobody listening, fine
        let _ = self.replica.updates.send(Arc::new(BTreeUpdate {
            seqid,
            seqno,
            snapshot: Some(vec![]),
            updates: vec![],
        }));
    }

    fn write_table_rows(&self, rows: Vec<TableRow>) -> EtlResult<()> {
        #[cfg(feature = "log")]
        log::trace!("write_table_rows: {} rows", rows.len());
        let mut updates = vec![];
        let (seqid, seqno) = {
            let mut replica = self.replica.write();
            for row in rows {
                if let Some(t) = parse_row::<T, N>(&self.table_schema, row) {
                    let index = t.index();
                    replica.insert(index.clone().into(), t.clone());
                    updates.push((index, Some(t)));
                }
            }
            replica.seqno += 1;
            (replica.seqid, replica.seqno)
        };
        let _ = self.replica.sequence.send_replace((seqid, seqno));
        // nobody listening, fine
        let _ = self.replica.updates.send(Arc::new(BTreeUpdate {
            seqid,
            seqno,
            snapshot: None,
            updates,
        }));
        EtlResult::Ok(())
    }

    fn write_event(&mut self, event: Event) -> EtlResult<()> {
        match event {
            Event::Begin(..) => {}
            Event::Commit(..) => {}
            Event::Insert(insert) => {
                if let Some(t) = parse_row::<T, N>(&self.table_schema, insert.table_row) {
                    self.txn_clog.push(Ok(t));
                }
            }
            Event::Update(update) => {
                let mut index = Ok(None);
                if let Some((_, key)) = update.old_table_row {
                    if let Some(i) = parse_row_index::<T, N>(&self.table_schema, key) {
                        index = Ok(Some(i));
                    } else {
                        index = Err(());
                    }
                }
                if let Ok(index) = index {
                    if let Some(t) =
                        parse_row::<T, N>(&self.table_schema, update.table_row)
                    {
                        if let Some(i) = index {
                            self.txn_clog.push(Err(i));
                        }
                        self.txn_clog.push(Ok(t));
                    }
                }
            }
            Event::Delete(delete) => {
                // CR alee: under what conditions is old_table_row not Some(..)?
                if let Some((_, key)) = delete.old_table_row {
                    if let Some(i) = parse_row_index::<T, N>(&self.table_schema, key) {
                        self.txn_clog.push(Err(i));
                    }
                }
            }
            Event::Relation(..) => {}
            Event::Truncate(..) => {}
            Event::Unsupported => {}
        }
        EtlResult::Ok(())
    }
}
