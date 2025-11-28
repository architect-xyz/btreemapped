# Update pg_replicate dependency to etl

Instead of pg_replicate, use ../etl as the new primary dependency. etl is the same codebase as pg_replicate (they share common ancestry), but more up-to-date and has seen a few major refactors. The online documentation for etl is located at https://supabase.github.io/etl/tutorials/first-pipeline/.

The way BTreeMapSink works is similar in some respects to etl::destinations::MemoryDestination. You should study it for inspiration and guidance. Come up with a PLAN.md to guide this refactor.

## Continuation notes

1. Need to link SinkState and table schema info to the sink
2. Add a 1.sql to the docker compose psql setup for the example test
3. Run the example test manually