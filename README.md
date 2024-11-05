# btreemapped

to explain:

- postgres wal_sender_timeout and its interaction with tcp idle keepalive
- pitfalls of logical replication lagging, monitoring and recovery
- https://wolfman.dev/posts/pg-logical-heartbeats/

todo:

- String -> Cow<str> conversions
- Erase arity from the type somehow?
- Refactor a bit to reflect that a sink could cover multiple tables/replicas