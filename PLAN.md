# Migration Plan: pg_replicate → etl

## Overview

This document outlines the plan to migrate btreemapped from using `pg_replicate` to `etl` as its primary dependency for Postgres logical replication. The `etl` crate is a more up-to-date fork of `pg_replicate` maintained by Supabase.

## Key Architectural Differences

### Trait Model

| pg_replicate | etl |
|--------------|-----|
| `BatchSink` trait | `Destination` trait |
| `get_resumption_state()` | No equivalent (state handled externally via stores) |
| `write_table_schemas()` | No equivalent (schema via RelationEvent) |
| `write_table_rows()` | `write_table_rows(table_id, rows)` |
| `write_cdc_events()` | `write_events(events)` |
| `table_copied()` | No equivalent |
| `truncate_table()` | `truncate_table(table_id)` |

### Event Model

**pg_replicate CdcEvent:**
```rust
enum CdcEvent {
    Begin(BeginBody),
    Commit(CommitBody),
    Insert((TableId, TableRow)),
    Update { table_id, old_row, key_row, row },
    Delete((TableId, TableRow)),
    Type(TypeBody),
    Relation(RelationBody),
    KeepAliveRequested { reply },
}
```

**etl Event:**
```rust
enum Event {
    Begin(BeginEvent),      // Contains start_lsn, commit_lsn, timestamp, xid
    Commit(CommitEvent),    // Contains start_lsn, commit_lsn, flags, end_lsn, timestamp
    Insert(InsertEvent),    // Contains start_lsn, commit_lsn, table_id, table_row
    Update(UpdateEvent),    // Contains start_lsn, commit_lsn, table_id, table_row, old_table_row
    Delete(DeleteEvent),    // Contains start_lsn, commit_lsn, table_id, old_table_row
    Relation(RelationEvent), // Contains start_lsn, commit_lsn, table_schema
    Truncate(TruncateEvent),
    Unsupported,
}
```

### Type Locations

| Type | pg_replicate | etl |
|------|--------------|-----|
| `TableId` | `pg_replicate::table::TableId` | `etl_postgres::types::TableId` |
| `TableSchema` | `pg_replicate::table::TableSchema` | `etl_postgres::types::TableSchema` |
| `TableName` | `pg_replicate::table::TableName` | `etl_postgres::types::TableName` |
| `TableRow` | `pg_replicate::conversions::table_row::TableRow` | `etl::types::TableRow` |
| `Cell` | `pg_replicate::conversions::Cell` | `etl::types::Cell` |
| `PgLsn` | `pg_replicate::tokio_postgres::types::PgLsn` | `tokio_postgres::types::PgLsn` |

### Key Structural Changes in TableSchema

**pg_replicate:**
- `table_id: TableId`
- `table_name: TableName`
- `column_schemas: Vec<ColumnSchema>`

**etl:**
- `id: TableId`
- `name: TableName`
- `column_schemas: Vec<ColumnSchema>`

---

## Migration Steps

### Step 1: Update Cargo.toml Dependencies

**File:** `Cargo.toml`

Replace:
```toml
pg_replicate = { git = "https://github.com/architect-xyz/pg_replicate.git" }
```

With:
```toml
etl = { path = "../etl", default-features = false }
etl-postgres = { path = "../etl/etl-postgres", default-features = false, features = ["replication"] }
tokio-postgres = { git = "https://github.com/MaterializeInc/rust-postgres", default-features = false, rev = "c4b473b478b3adfbf8667d2fbe895d8423f1290b" }
```

### Step 2: Update btreemapped/Cargo.toml

Update the feature flags and dependencies in the sub-crate's Cargo.toml to reference the new workspace dependencies.

### Step 3: Update btreemapped_derive Macro

**File:** `btreemapped_derive/src/derive_btreemapped.rs`

The derive macro generates code that references pg_replicate types directly. Update:

1. Change `pg_replicate::table::TableSchema` → `etl_postgres::types::TableSchema`
2. Change `pg_replicate::conversions::table_row::TableRow` → `etl::types::TableRow`
3. Update field access: `schema.column_schemas[_n]` stays same, but `col.name` stays same
4. Cell conversion: etl's Cell has TryInto implementations similar to pg_replicate

**Generated code changes:**
```rust
// Before
fn parse_row(
    schema: &pg_replicate::table::TableSchema,
    row: pg_replicate::conversions::table_row::TableRow,
) -> anyhow::Result<Self>

// After
fn parse_row(
    schema: &etl_postgres::types::TableSchema,
    row: etl::types::TableRow,
) -> anyhow::Result<Self>
```

### Step 4: Update BTreeMapped Trait

**File:** `btreemapped/src/lib.rs`

Update trait definition:
```rust
// Before
fn parse_row(
    schema: &pg_replicate::table::TableSchema,
    row: pg_replicate::conversions::table_row::TableRow,
) -> anyhow::Result<Self>;

fn parse_row_index(
    schema: &pg_replicate::table::TableSchema,
    row: pg_replicate::conversions::table_row::TableRow,
) -> anyhow::Result<Self::Index>;

// After
fn parse_row(
    schema: &etl_postgres::types::TableSchema,
    row: etl::types::TableRow,
) -> anyhow::Result<Self>;

fn parse_row_index(
    schema: &etl_postgres::types::TableSchema,
    row: etl::types::TableRow,
) -> anyhow::Result<Self::Index>;
```

### Step 5: Refactor BTreeMapSink

**File:** `btreemapped/src/sink.rs`

This is the most significant change. Convert from `BatchSink` to `Destination` trait.

**Key differences to handle:**

1. **Transaction state management**: The `Destination` trait doesn't track resumption state internally. We need to handle Begin/Commit events differently.

2. **Schema handling**: No `write_table_schemas` method. Schema comes via `RelationEvent` in the event stream.

3. **Method signatures change**:
   - `write_table_rows(rows, table_id)` → `write_table_rows(table_id, rows)`
   - `write_cdc_events(events) -> PgLsn` → `write_events(events) -> EtlResult<()>`

**New structure:**

```rust
use etl::destination::Destination;
use etl::error::EtlResult;
use etl::types::{Event, TableRow};
use etl_postgres::types::{TableId, TableSchema};

impl<T: BTreeMapped<N>, const N: usize> Destination for BTreeMapSink<T, N> {
    fn name() -> &'static str {
        "btreemap"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        // existing truncate logic
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // existing write_table_rows logic with updated types
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        // Convert from write_cdc_events logic
        // Handle Begin, Commit, Insert, Update, Delete, Relation events
    }
}
```

**Event handling changes:**

```rust
// Before (pg_replicate)
CdcEvent::Insert((tid, row)) => { ... }
CdcEvent::Update { table_id, old_row, key_row, row } => { ... }
CdcEvent::Delete((tid, row)) => { ... }

// After (etl)
Event::Insert(insert_event) => {
    let tid = insert_event.table_id;
    let row = insert_event.table_row;
    ...
}
Event::Update(update_event) => {
    let table_id = update_event.table_id;
    let row = update_event.table_row;
    let old_row = update_event.old_table_row; // Option<(bool, TableRow)>
    ...
}
Event::Delete(delete_event) => {
    let tid = delete_event.table_id;
    let old_row = delete_event.old_table_row;
    ...
}
Event::Relation(relation_event) => {
    // Handle schema updates dynamically
    let schema = relation_event.table_schema;
    ...
}
```

### Step 6: Refactor MultiBTreeMapSink

**File:** `btreemapped/src/multi_sink.rs`

Similar changes as BTreeMapSink, but managing multiple table sinks.

Key change: Schema assignment now happens via RelationEvent during `write_events` instead of `write_table_schemas`.

### Step 7: Update normalized_table_name Helper

**File:** `btreemapped/src/sink.rs`

```rust
// Before
pub(crate) fn normalized_table_name(table_name: &TableName) -> String {
    if table_name.schema == "public" {
        table_name.name.to_string()
    } else {
        table_name.to_string()
    }
}

// After (same logic, different import)
use etl_postgres::types::TableName;
// Function body remains the same
```

### Step 8: Handle State Management Differences

The etl crate uses separate `SchemaStore` and `StateStore` traits for persistence. Since btreemapped primarily provides an in-memory replica, we have two options:

**Option A: Internal State Only (Recommended)**
Keep internal state management within BTreeMapSink/MultiBTreeMapSink without implementing external stores. This matches the current in-memory-only design.

**Option B: Implement Store Traits**
Implement `SchemaStore` and `StateStore` for scenarios requiring persistence across restarts.

For this migration, we'll proceed with Option A.

### Step 9: Update Tests

**Files:** `btreemapped/tests/*.rs`

Update test imports and any test fixtures that reference pg_replicate types directly.

### Step 10: Update Examples

**File:** `btreemapped/examples/basic.rs`

Update example code to use etl's Pipeline API:

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};

// Create pipeline with BTreeMapSink as destination
let store = MemoryStore::new();
let sink = BTreeMapSink::<MyType, 1>::new("my_table");
let config = PipelineConfig { ... };
let mut pipeline = Pipeline::new(config, store, sink);
pipeline.start().await?;
```

---

## Implementation Order

1. **Phase 1: Dependencies & Types**
   - [ ] Update workspace Cargo.toml
   - [ ] Update btreemapped/Cargo.toml
   - [ ] Update btreemapped_derive/Cargo.toml

2. **Phase 2: Core Trait Updates**
   - [ ] Update BTreeMapped trait in lib.rs
   - [ ] Update derive macro to generate etl-compatible code

3. **Phase 3: Sink Refactor**
   - [ ] Refactor BTreeMapSink to implement Destination
   - [ ] Refactor MultiBTreeMapSink to implement Destination
   - [ ] Update ErasedBTreeMapSink trait

4. **Phase 4: Testing & Validation**
   - [ ] Update test files
   - [ ] Update examples
   - [ ] Run full test suite

---

## Risk Assessment

### Low Risk
- Type renames (TableId, TableSchema, etc.) - straightforward search/replace
- Import path changes - mechanical updates

### Medium Risk
- Cell type compatibility - etl's Cell enum is similar but may have subtle differences in variant names or conversion implementations
- Transaction boundary handling - etl's event-based model packages Begin/Commit differently

### Higher Risk
- Loss of resumption state tracking - the Destination trait doesn't have `get_resumption_state()`. If btreemapped consumers rely on this, additional work needed.
- Schema discovery timing - pg_replicate had explicit `write_table_schemas`, etl uses RelationEvent during streaming. Need to handle schema-before-data ordering.

---

## Rollback Plan

If migration encounters blocking issues:
1. Keep pg_replicate as a feature-flagged dependency
2. Implement both BatchSink and Destination traits conditionally
3. Allow users to choose which replication backend to use via feature flags

---

## Success Criteria

- [ ] All existing tests pass with etl backend
- [ ] `cargo build` succeeds without pg_replicate dependency
- [ ] Example code runs successfully against test Postgres instance
- [ ] No regression in replication functionality (inserts, updates, deletes, truncates)
