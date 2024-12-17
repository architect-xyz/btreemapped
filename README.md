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