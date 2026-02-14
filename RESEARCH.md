# Prior Art: BTreeMap with Composite/Tuple Keys

## What is this pattern?

A BTreeMap where keys are tuples like `(String, i64)`, sorted in lexicographic order, supporting prefix range queries — e.g., iterate all entries where the first element = "Alice" and the second element is in range `100..200`.

This is a **composite/compound/concatenated index** — the exact same concept as a multi-column B-tree index in every major relational database.

## Formal Terminology

| btreemapped concept | Standard name |
|---|---|
| BTreeMap with tuple keys | **Composite/compound/concatenated index** |
| `LValue::NegInfinity`/`Infinity` | **Sentinel values** (MapDB: `null`/`Tuple.HI`) |
| Fix prefix, range over suffix | **Prefix scan** / **prefix range query** |
| Tuple lexicographic ordering | **Lexicographic order** on Cartesian product |
| "Can only query leftmost columns" | **Leftmost prefix rule** |
| "Range stops at first non-equality" | **ESR rule** (Equality, Sort, Range — MongoDB's term) |

## Closest Direct Prior Art: MapDB (Java)

[MapDB's BTreeMap composite keys](https://mapdb.org/book/btreemap/composite-keys/) use `Tuple2`, `Tuple3`, etc. with the **exact same sentinel pattern**:

- `null` represents negative infinity
- `Tuple.HI` represents positive infinity
- `prefixSubMap(new Tuple2("Smith", null))` returns all entries where first = "Smith"
- Their docs note sentinels "are not serializable and cannot be stored in Map" — same invariant as our `LValue::Exact`-only keys

Our `LValue<T>` enum is a cleaner version: type-safe, generic, with derived `Ord` giving correct sentinel ordering for free via Rust's discriminant-based enum ordering.

## Other Systems Doing the Same Thing

**FoundationDB Tuple Layer** — Preserves lexicographic ordering of tuples when encoding as byte strings. `Subspace` and `Directory` abstractions define key prefixes; `range()` returns all keys with a given prefix tuple. Production-grade distributed version of this pattern.

**RocksDB / LevelDB** — [Prefix Seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek) is built for this pattern. Keys like `<user-id>.<timestamp>.<activity-type>` use prefix seek to iterate all entries for a given user-id. Prefix extractors optimize bloom filters.

**Google Bigtable / HBase** — Composite row keys like `user#timestamp#activity`. Range queries via [prefix scans](https://docs.cloud.google.com/bigtable/docs/samples/bigtable-reads-prefix).

**CouchDB** — Views use [array keys with `startkey`/`endkey`](https://docs.couchdb.org/en/stable/ddocs/views/collation.html). The sentinel `{}` (empty object) serves as positive infinity: `startkey=["foo"]&endkey=["foo",{}]`.

**Amazon DynamoDB** — [Composite primary key](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html) = partition key (equality) + sort key (range). The leftmost prefix rule enforced at the API level.

**PostgreSQL** — [Composite B-tree indexes](https://www.postgresql.org/docs/current/indexes-multicolumn.html) with the leftmost prefix rule. Supports [row value comparisons](https://www.postgresql.org/docs/current/functions-comparisons.html) like `WHERE (a, b) > (1, 2)` mapping to lexicographic range scans. Postgres 18 introduces Skip Scan to partially relax the leftmost-prefix constraint.

**MySQL / SQLite** — Same composite B-tree indexes, same leftmost prefix rule. MySQL [stops using the index after the first range condition](https://planetscale.com/learn/courses/mysql-for-developers/indexes/composite-indexes). SQLite: ["there cannot be gaps in the columns of the index that are used"](https://www.sqlite.org/optoverview.html).

## Approaches to "Partial Range on Tuple Dimensions"

The core problem: how do you express a range bound on a multi-component key when you only want to constrain some dimensions?

| Approach | Used by | Mechanism |
|---|---|---|
| **Sentinel enum values** | btreemapped, MapDB | Wrap each component in an enum with neg-infinity/exact/pos-infinity. Derive Ord so sentinels sort correctly. |
| **MIN/MAX constants** | Many ad-hoc impls | Use `i64::MIN`/`i64::MAX` etc. Fails for unbounded types like strings. |
| **Prefix + successor key** | FoundationDB, RocksDB, Bigtable | Compute lexicographic successor of prefix to form half-open range `[prefix, successor)`. For strings, append `\0`. |
| **Special query operators** | CouchDB (`{}`), DynamoDB (`begins_with`), SQL (`LIKE 'prefix%'`) | Query API provides prefix/range operators that handle bounds internally. |
| **`Bound::Unbounded`** | Rust std | For non-composite keys only. Does not compose into individual tuple dimensions — `LValue` fills this gap. |

## Key Fields of Study

1. **Database indexing theory** — composite indexes, the leftmost prefix rule, ESR guideline
2. **Order theory** — lexicographic order on Cartesian products of totally ordered sets
3. **Computational geometry** — multi-dimensional range searching (range trees, KD-trees, R-trees) — though this pattern is the simpler 1D-with-composite-key variant, not true multi-dimensional indexing
4. **Space-filling curves** (Z-order/Morton curves, Hilbert curves) — an alternative that maps N-dimensional keys to 1D without the leftmost-prefix constraint, at the cost of complexity and false positives

## Important Distinction

This design is **not** multi-dimensional indexing (like R-trees or KD-trees). It is a standard 1D B-tree exploiting the fact that lexicographic order on tuples naturally groups entries sharing a prefix. This is simpler and faster when queries follow the leftmost-prefix pattern — which is exactly what database composite indexes do.
