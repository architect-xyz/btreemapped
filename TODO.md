# Update pg_replicate dependency to etl

Instead of pg_replicate, use ../etl as the new primary dependency. etl is the same codebase as pg_replicate (they share common ancestry), but more up-to-date and has seen a few major refactors. The online documentation for etl is located at https://supabase.github.io/etl/tutorials/first-pipeline/. 

I had to make some changes to pg_replicate to make it work with btreemapped. Those changes are in ../pg_replicate/FORK_CHANGES.diff. Study this diff, then study etl, and understand what changes need to be made to etl as well to match. Describe the changes (grouped by broad theme) and write your plan in PLAN.md for inspection.
