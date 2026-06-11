use crate::{BTreeMapReplica, BTreeMapped, BTreeUpdate};
use etl::{
    error::EtlResult,
    types::{
        ColumnSchema, Event, OldTableRow, ReplicatedTableSchema, TableRow,
        UpdatedTableRow,
    },
};
use std::sync::Arc;

pub struct BTreeMapSink<T: BTreeMapped<N>, const N: usize> {
    pub replica: BTreeMapReplica<T, N>,
    txn_clog: Vec<Result<T, T::Index>>,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapSink<T, N> {
    pub fn new(replica: BTreeMapReplica<T, N>) -> Self {
        Self {
            replica,
            txn_clog: vec![],
        }
    }
}

fn parse_row<'a, T: BTreeMapped<N>, const N: usize>(
    columns: impl Iterator<Item = &'a ColumnSchema>,
    row: TableRow,
) -> Option<T> {
    match T::parse_row(columns, row) {
        Ok(t) => Some(t),
        // TODO: we can probably return an error and propagate it now
        Err(_e) => {
            #[cfg(feature = "log")]
            log::error!("parse_row returned error: {_e:?}");
            None
        }
    }
}

fn parse_row_index<'a, T: BTreeMapped<N>, const N: usize>(
    columns: impl Iterator<Item = &'a ColumnSchema>,
    row: TableRow,
) -> Option<T::Index> {
    match T::parse_row_index(columns, row) {
        Ok(index) => Some(index),
        Err(_e) => {
            #[cfg(feature = "log")]
            log::error!("parse_row returned error: {_e:?}");
            None
        }
    }
}

/// Parse the index from an old-row image, using the appropriate column
/// schemas for the image shape: full rows carry all replicated columns,
/// key rows carry only the (dense) identity columns.
fn parse_old_row_index<T: BTreeMapped<N>, const N: usize>(
    schema: &ReplicatedTableSchema,
    old_row: OldTableRow,
) -> Option<T::Index> {
    match old_row {
        OldTableRow::Full(row) => parse_row_index::<T, N>(schema.column_schemas(), row),
        OldTableRow::Key(row) => {
            parse_row_index::<T, N>(schema.identity_column_schemas(), row)
        }
    }
}

pub(crate) trait ErasedBTreeMapReplica: Send {
    fn to_sink(&self) -> Box<dyn ErasedBTreeMapSink>;
}

impl<T: BTreeMapped<N>, const N: usize> ErasedBTreeMapReplica for BTreeMapReplica<T, N> {
    fn to_sink(&self) -> Box<dyn ErasedBTreeMapSink> {
        Box::new(BTreeMapSink::new(self.clone()))
    }
}

// MultiBTreeMapSink need to be able to manipulate the inner state
// of the individual sinks; also erase the types so we can put them
// in Box<dyn>s.
pub(crate) trait ErasedBTreeMapSink: Send {
    fn commit(&mut self);

    fn truncate(&self);

    fn write_table_rows(
        &self,
        schema: &ReplicatedTableSchema,
        rows: Vec<TableRow>,
    ) -> EtlResult<()>;

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

    fn write_table_rows(
        &self,
        schema: &ReplicatedTableSchema,
        rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        #[cfg(feature = "log")]
        log::trace!("write_table_rows: {} rows", rows.len());
        let mut updates = vec![];
        let (seqid, seqno) = {
            let mut replica = self.replica.write();
            for row in rows {
                if let Some(t) = parse_row::<T, N>(schema.column_schemas(), row) {
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
                let schema = &insert.replicated_table_schema;
                if let Some(t) =
                    parse_row::<T, N>(schema.column_schemas(), insert.table_row)
                {
                    self.txn_clog.push(Ok(t));
                }
            }
            Event::Update(update) => {
                let schema = &update.replicated_table_schema;
                let mut index = Ok(None);
                if let Some(old_row) = update.old_table_row {
                    if let Some(i) = parse_old_row_index::<T, N>(schema, old_row) {
                        index = Ok(Some(i));
                    } else {
                        index = Err(());
                    }
                }
                if let Ok(index) = index {
                    match update.updated_table_row {
                        UpdatedTableRow::Full(row) => {
                            if let Some(t) =
                                parse_row::<T, N>(schema.column_schemas(), row)
                            {
                                if let Some(i) = index {
                                    self.txn_clog.push(Err(i));
                                }
                                self.txn_clog.push(Ok(t));
                            }
                        }
                        UpdatedTableRow::Partial(_partial) => {
                            // Postgres emitted UnchangedToast columns that etl
                            // could not reconstruct from the old row image; we
                            // can't merge partial rows into typed structs, so
                            // skip the update (same policy as a parse failure).
                            #[cfg(feature = "log")]
                            log::error!(
                                "skipping partial update for table {}: \
                                 unchanged TOAST columns could not be reconstructed",
                                schema.name()
                            );
                        }
                    }
                }
            }
            Event::Delete(delete) => {
                let schema = &delete.replicated_table_schema;
                // CR alee: under what conditions is old_table_row not Some(..)?
                if let Some(old_row) = delete.old_table_row {
                    if let Some(i) = parse_old_row_index::<T, N>(schema, old_row) {
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
