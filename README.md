# btreemapped

to explain:

- postgres wal_sender_timeout and its interaction with tcp idle keepalive
- pitfalls of logical replication lagging, monitoring and recovery
- https://wolfman.dev/posts/pg-logical-heartbeats/
- BTreeWrite and the concept of postgres-as-kv-store

todo:

- String -> Cow<str> conversions
- Erase arity from the type somehow?
- Pretty sure arity >= 2 iteration needs an additional bounds check
- Use fully qualified name for LIndex* in btreemapped_derive, so users don't have to import it explicitly
- https://www.morling.dev/blog/mastering-postgres-replication-slots/
- Study arithmetic checksum properties to see what's best for checking replication success. Although, pg_current_wal_lsn()isn't transactional, so it's not clear how you would get a stable reference. There might be some way to "dye" the databaseor leave a breadcrumb. Or, try this crazy shit: https://stackoverflow.com/questions/69459481/retrieve-lsn-of-postgres-database-during-transaction 
- Move the Cell conversion to this crate, so we don't need to upstream it to etl
- Upstream TCP keepalive config to etl
- Exposing the connection task to the caller should be an option; it will make certain call patterns easier (e.g. task cancellation and parallel scheduling)...I think? Yes, b/c then you don't have to wait the cancellation token, the connection task will drop with the scope

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
