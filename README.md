# btreemapped

## Introduction

**btreemapped** replicates a database table into an in-memory `BTreeMap<PrimaryKey, Row>`, kept in sync via logical replication.  This in-memory table replica supports the same access patterns as a primary key index: retrieving a row by its key, or iterate over rows in key order over a range.

## Usage

### Defining a table

A struct with `#[derive(BTreeMapped)]` maps a database table. The
`index` attribute specifies which fields form the BTreeMap key.

```rust
// Single-column key
#[derive(Debug, Clone, BTreeMapped)]
#[btreemap(index = ["id"])]
struct User {
    id: i64,
    name: Option<String>,
    age: Option<i32>,
}

// Composite key
#[derive(Debug, Clone, BTreeMapped)]
#[btreemap(index = ["owner", "license_plate"])]
struct Car {
    owner: String,
    license_plate: i64,
    color: String,
}
```

### Point lookups

```rust
let replica: BTreeMapReplica<User, 1> = BTreeMapReplica::new();

// Zero-allocation lookup for &str keys
let user = replica.get("alice");

// Composite key lookup — &str auto-converts
let car_replica: BTreeMapReplica<Car, 2> = BTreeMapReplica::new();
let car = car_replica.get(("Alice", 123));
```

### Range iteration

Range queries iterate rows in key order. For composite keys, fix a
prefix and range over the next dimension—the same operation as a
prefix scan on a database composite index.

```rust
// All users with id between 100 and 200
replica.for_range1(100..=200, |user| {
    println!("{}: {:?}", user.id, user.name);
});

// All cars owned by "Bob" — &str auto-converts, zero-alloc
car_replica.for_range1("Bob"..="Bob", |car| {
    println!("{} {}", car.owner, car.license_plate);
});

// Charlie's cars with license_plate in 1000..1003
car_replica.for_range2("Charlie", 1000..1003, |car| {
    println!("{} {}", car.owner, car.license_plate);
});
// prints: Charlie 1000, Charlie 1001, Charlie 1002
```

### Connecting to Postgres

```rust
let replicator = BTreeMapReplicator::new();
let replica = replicator.add_replica::<User, 1>("users");

// Start replication in the background
tokio::spawn(async move {
    replicator.run(pipeline_config(), None).await
});

// Wait for initial sync to complete
replicator.synced().await;

// replica is now live — queries reflect the current table state
let user = replica.get("alice");
```

## Caveats

### Table shape restriction

**btreemapped** requires that every row be uniquely identifiable by some set of columns. These columns become the `BTreeMap` tuple key, and if two rows share the same key, the second silently overwrites the first. Tables with no primary key, no unique constraint, or duplicate rows cannot be faithfully represented. This is inherent to the choice of `BTreeMap` as the backing structure.

### Postgres replication

The replication layer is currently built on [Supabase's `etl` library](https://github.com/supabase/etl), which binds it to Postgres logical replication for now. This is incidental to the design--in principle we could support any database that emits row-level change events.

## Why not SQLite in-memory or DuckDB

Once you want secondary indexes, filtered queries, joins, or aggregations, the complexity required exceeds what a single BTreeMap provides. At that point, an embedded database engine like SQLite (row-oriented, OTLP) or DuckDB (columnar, OLAP) is the right tool.

**btreemapped** deliberately stops short of this. It is a Rust data structure, not a database or a query engine. The tradeoff is a deliberately narrow query surface: one table, on index, point lookups and prefix range scans.

## Appendix

to explain:

- postgres wal_sender_timeout and its interaction with tcp idle keepalive
- pitfalls of logical replication lagging, monitoring and recovery
- https://wolfman.dev/posts/pg-logical-heartbeats/

todo:

- String -> Cow<str> conversions
- Erase arity from the type somehow?
- Pretty sure arity >= 2 iteration needs an additional bounds check
- Use fully qualified name for LIndex* in btreemapped_derive, so users don't have to import it explicitly
- https://www.morling.dev/blog/mastering-postgres-replication-slots/
- Study arithmetic checksum properties to see what's best for checking replication success. Although, pg_current_wal_lsn()isn't transactional, so it's not clear how you would get a stable reference. There might be some way to "dye" the databaseor leave a breadcrumb. Or, try this crazy shit: https://stackoverflow.com/questions/69459481/retrieve-lsn-of-postgres-database-during-transaction 
- Move the Cell conversion to this crate, so we don't need to upstream it to etl
- Upstream TCP keepalive config to etl
- Test schema changes, particularly alter-table-alter-column-set-default, and alter-table-add-column-set-default
- BTreeMap is just one choice of data structure. It might be worth separating the choice of data structure vs the relational guarantees; this goes hand-in-hand with the mapping functions
- I bet Arc<str> or Box<str> solves a lot of String/str coerce problems! E.g. Looking up tuple, could you look up a BTreeMap<(Arc<str>, Arc<str>), V> by an (&str, &str)?

## Running the examples

```bash
# start a postgres
docker compose up postgres

# run the replication example
cargo run --example basic

# connect to the database and insert/delete rows
# observe the output of the example
```

## Check WAL growth and replication status

```
SELECT 
    slot_name,
    confirmed_flush_lsn, 
    pg_current_wal_lsn(), 
    (pg_current_wal_lsn() - confirmed_flush_lsn) AS lsn_distance
FROM 
    pg_replication_slots;
```
