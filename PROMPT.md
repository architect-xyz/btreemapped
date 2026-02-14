# Ergonomic String Key Lookups in btreemapped

## Original Problem

Users had to write verbose, allocating lookups like:

```rust
replica.get(&("MyKey".to_string(),))
replica.get((Cow::Borrowed("Alice"), 123))
```

The goal: enable ergonomic, zero-allocation lookups like:

```rust
replica.get("MyKey")           // singleton String index
replica.get(("Alice", 123))    // composite index
```

Two constraints:
1. **Ergonomic**: no `.to_string()`, no `Cow::Borrowed(...)`, no tuple wrapper for singletons
2. **Zero-allocation for &str**: like `HashMap<String, V>.get("hello")`, shouldn't allocate a String just to look up

## Why It's Hard

The BTreeMap keys are `LIndex1<String>` (a newtype wrapping `LValue<String>`), not plain `String`. So `BTreeMap::get`'s `Borrow`-based zero-alloc lookup doesn't work out of the box.

Rust's trait coherence rules make it impossible to have both a blanket singleton `From<I0> for LIndex1<I0>` AND a flexible tuple `From<(Q0,)> for LIndex1<I0> where Q0: Into<I0>` — they overlap when `I0 = (Q0,)`. Similarly, a blanket `IntoLIndex` impl through `Into` conflicts with specific `&str` impls.

## Solution: `Borrow<str>` + `GetKey<L>` trait

### 1. `Borrow<str>` on LIndex1 (zero-alloc path)

All BTreeMap keys are always `LValue::Exact`; `NegInfinity`/`Infinity` only appear in range bound construction, never as stored keys. This means the `Borrow` Ord-equivalence contract holds for all stored keys:

```rust
impl Borrow<str> for LIndex1<String> { ... }
impl Borrow<str> for LIndex1<Cow<'static, str>> { ... }
```

This enables `BTreeMap::get("hello")` to compare `&str` directly against stored keys — zero allocation.

### 2. `GetKey<L>` trait (dispatch between strategies)

A custom trait used by `get`/`contains_key` that dispatches between:

- **Zero-alloc** (`&str` on singleton String/Cow indexes): uses `Borrow<str>` via `map.get(self)`
- **Allocating** (tuples at any arity): each element converts via `Into`, so `("Alice", 123)` works where `&str: Into<String>` (from std)
- **Reference** (`&LIndex`): pass an existing LIndex directly, zero-alloc

No coherence issues because:
- `&str` is never a tuple → no overlap with tuple impls
- Specific `&str` impls target different `L` types than the `&L` blanket
- Singleton impls (String, i64, etc.) are never tuples → no overlap

### Files changed

- `btreemapped/src/lvalue.rs`: Added `Borrow` impls, `GetKey` trait + impls
- `btreemapped/src/replica.rs`: Changed `get`/`contains_key` from `Into<T::LIndex>` to `GetKey<T::LIndex>`

### What works now

```rust
replica.get("MyKey")                        // zero-alloc singleton
replica.get(("MyKey",))                     // tuple form (allocates)
replica.get(("Alice", 123))                 // composite (&str auto-converts)
replica.get("MyKey".to_string())            // still works
replica.get((Cow::Borrowed("Alice"), 123))  // still works
replica.get(&some_lindex)                   // pass existing LIndex ref
```
