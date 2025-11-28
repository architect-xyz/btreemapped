use crate::{sink_state::SinkState, BTreeMapReplica, BTreeMapped, BTreeUpdate};
use etl::{
    destination::Destination,
    error::EtlResult,
    types::{Event, TableId, TableRow, TableSchema},
};
use std::{collections::HashMap, sync::Arc};

pub struct BTreeMapSink<T: BTreeMapped<N>, const N: usize> {
    pub replica: BTreeMapReplica<T, N>,
    sink_state: SinkState,
    table_name: String,
    table_id: Option<TableId>,
    table_schema: Option<TableSchema>,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapSink<T, N> {
    pub fn new(table_name: &str) -> Self {
        let seqid = rand::random::<u64>();
        let replica = BTreeMapReplica::new(seqid);
        let sink_state = SinkState::new();
        Self {
            replica,
            sink_state,
            table_name: table_name.to_string(),
            table_id: None,
            table_schema: None,
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
        todo!()
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        todo!()
    }
}
