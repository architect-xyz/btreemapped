//! Helper struct for working with range bounds on BTreeMaps with tuple keys.
//!
//! BTreeMaps are not multi-dimensional structures, but we still need a way
//! to express the bounds on each tuple element respectively.  The user can
//! iterate over the BTreeMap as usual and use `LValue<T>`s as comparators.

use std::{
    borrow::{Borrow, Cow},
    collections::BTreeMap,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LValue<T> {
    /// Less than any/all T
    NegInfinity,
    /// Exactly T
    Exact(T),
    /// Greater than any/all T
    Infinity,
}

// Manual impl so that `Exact(t)` hashes exactly like `t`; this keeps Hash
// consistent with the `Borrow<str>` impls on LIndex1 below (the Borrow
// contract requires hash equivalence between a key and its borrowed form).
impl<T: std::hash::Hash> std::hash::Hash for LValue<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            LValue::NegInfinity => state.write_u8(0),
            LValue::Exact(t) => t.hash(state),
            LValue::Infinity => state.write_u8(1),
        }
    }
}

impl<T> LValue<T> {
    pub fn exact(&self) -> Option<&T> {
        match self {
            LValue::Exact(t) => Some(t),
            _ => None,
        }
    }
}

impl<T: std::fmt::Display> std::fmt::Display for LValue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LValue::NegInfinity => write!(f, "-∞"),
            LValue::Exact(t) => t.fmt(f),
            LValue::Infinity => write!(f, "∞"),
        }
    }
}

/// Marker traits for LIndex arity
pub trait HasArity<const N: usize> {}

// NB alee: tuples are always foreign, so we need to wrap them in a struct
// in order to define trait implementations for them.  We call this struct,
// which wraps a tuple of LValues, an LIndex.
macro_rules! lindex {
    ($name:ident, N = $n:literal, $($I:ident),+) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name<$($I),+>($(pub LValue<$I>,)+);

        paste::paste! {
            impl<$($I),+> HasArity<$n> for $name<$($I),+> {}

            impl<$($I),+> From<($($I,)+)> for $name<$($I),+> {
                fn from(($([<$I:lower>],)+): ($($I,)+)) -> Self {
                    $name($(LValue::Exact([<$I:lower>]),)+)
                }
            }

            impl<'a, $($I),+> TryFrom<&'a $name<$($I),+>> for ($($I,)+)
            where
                $($I: Clone),+
            {
                type Error = anyhow::Error;

                fn try_from(value: &'a $name<$($I),+>) -> Result<Self, Self::Error> {
                    let $name($([<$I:lower>]),+) = value;
                    Ok(($(
                        [<$I:lower>]
                            .exact()
                            .ok_or_else(|| anyhow::anyhow!("incomplete LIndex"))?
                            .clone(),
                    )+))
                }
            }
        }
    };
}

lindex!(LIndex1, N = 1, I0);

// special case for single-element tuples
impl<I0> From<I0> for LIndex1<I0> {
    fn from(i0: I0) -> Self {
        LIndex1(LValue::Exact(i0))
    }
}

lindex!(LIndex2, N = 2, I0, I1);
lindex!(LIndex3, N = 3, I0, I1, I2);
lindex!(LIndex4, N = 4, I0, I1, I2, I3);
lindex!(LIndex5, N = 5, I0, I1, I2, I3, I4);

// --- Borrow impls for zero-allocation string lookups ---
//
// Every key stored in a replica's BTreeMap is `LValue::Exact`;
// `NegInfinity`/`Infinity` only appear in range-bound construction and
// are never inserted as keys.  For Exact keys the Borrow contract
// (Eq/Ord/Hash equivalence with the borrowed form) holds: Ord on Exact
// values delegates to `T`, and LValue's manual Hash impl hashes
// `Exact(t)` exactly like `t`.
//
// Constructing a sentinel LIndex and storing it as a map key violates
// that invariant, and a subsequent `&str` lookup on the map will panic.

impl Borrow<str> for LIndex1<String> {
    fn borrow(&self) -> &str {
        self.0.exact().expect("BTreeMap key must be LValue::Exact")
    }
}

impl Borrow<str> for LIndex1<Cow<'static, str>> {
    fn borrow(&self) -> &str {
        self.0.exact().expect("BTreeMap key must be LValue::Exact")
    }
}

/// A key argument for looking up an entry in a `BTreeMap` keyed by the
/// index type `L` (one of the `LIndexN` types).
///
/// This is the bound on [`BTreeMapReplica::get`](crate::BTreeMapReplica::get)
/// and [`contains_key`](crate::BTreeMapReplica::contains_key), and is what
/// allows lookups to be written without intermediate allocations or tuple
/// construction:
///
/// - `&str` looks up arity-1 `String` and `Cow<'static, str>` indexes
///   without allocating, e.g. `replica.get("hello")`
/// - any arity-1 index value can be passed bare, without wrapping it in
///   a 1-tuple, e.g. `replica.get(42)`
/// - tuples of arity >= 2 convert each element via `Into`, so
///   `("Alice", 123)` works for a `(String, i64)` index (the `&str`
///   element allocates); 1-tuples must match the index type exactly
/// - an `LIndexN` can be passed directly, by value or by reference
pub trait Lookup<L> {
    fn lookup<V>(self, map: &BTreeMap<L, V>) -> Option<&V>;
}

// Pass an LIndex by value
impl<L: Ord> Lookup<L> for L {
    fn lookup<V>(self, map: &BTreeMap<L, V>) -> Option<&V> {
        map.get(&self)
    }
}

// Pass a reference to an existing LIndex
impl<L: Ord> Lookup<L> for &L {
    fn lookup<V>(self, map: &BTreeMap<L, V>) -> Option<&V> {
        map.get(self)
    }
}

// Arity-1 indexes can be looked up by bare value, no tuple required
impl<I0: Ord> Lookup<LIndex1<I0>> for I0 {
    fn lookup<V>(self, map: &BTreeMap<LIndex1<I0>, V>) -> Option<&V> {
        map.get(&LIndex1(LValue::Exact(self)))
    }
}

// Zero-allocation &str lookups on arity-1 string indexes via Borrow<str>

impl Lookup<LIndex1<String>> for &str {
    fn lookup<V>(self, map: &BTreeMap<LIndex1<String>, V>) -> Option<&V> {
        map.get(self)
    }
}

impl Lookup<LIndex1<String>> for &String {
    fn lookup<V>(self, map: &BTreeMap<LIndex1<String>, V>) -> Option<&V> {
        map.get(self.as_str())
    }
}

impl Lookup<LIndex1<Cow<'static, str>>> for &str {
    fn lookup<'a, V>(
        self,
        map: &'a BTreeMap<LIndex1<Cow<'static, str>>, V>,
    ) -> Option<&'a V> {
        map.get(self)
    }
}

impl<'c> Lookup<LIndex1<Cow<'static, str>>> for &Cow<'c, str> {
    fn lookup<'a, V>(
        self,
        map: &'a BTreeMap<LIndex1<Cow<'static, str>>, V>,
    ) -> Option<&'a V> {
        map.get(self.as_ref())
    }
}

// 1-tuples must match the index type exactly; coherence with the bare
// value impl above forbids per-element Into flexibility here
impl<I0: Ord> Lookup<LIndex1<I0>> for (I0,) {
    fn lookup<V>(self, map: &BTreeMap<LIndex1<I0>, V>) -> Option<&V> {
        map.get(&LIndex1(LValue::Exact(self.0)))
    }
}

// Tuples of arity >= 2 convert each element via Into, so e.g.
// ("Alice", 123) can look up a (String, i64) index
macro_rules! impl_tuple_lookup {
    ($name:ident, $($Q:ident => $I:ident),+) => {
        paste::paste! {
            impl<$($Q, $I),+> Lookup<$name<$($I),+>> for ($($Q,)+)
            where
                $($Q: Into<$I>, $I: Ord,)+
            {
                fn lookup<V>(
                    self,
                    map: &BTreeMap<$name<$($I),+>, V>,
                ) -> Option<&V> {
                    let ($([<$Q:lower>],)+) = self;
                    map.get(&$name($(LValue::Exact([<$Q:lower>].into()),)+))
                }
            }
        }
    };
}

impl_tuple_lookup!(LIndex2, Q0 => I0, Q1 => I1);
impl_tuple_lookup!(LIndex3, Q0 => I0, Q1 => I1, Q2 => I2);
impl_tuple_lookup!(LIndex4, Q0 => I0, Q1 => I1, Q2 => I2, Q3 => I3);
impl_tuple_lookup!(LIndex5, Q0 => I0, Q1 => I1, Q2 => I2, Q3 => I3, Q4 => I4);

/// A range argument for the `for_rangeN` methods on `BTreeMapReplica`,
/// generalizing `RangeBounds<I>` so that range endpoints convert to the
/// index element type via `Into`.  This allows e.g.
/// `for_range1("abc"..="ghi")` on a `String` index without building
/// owned `String` bounds at the call site.
///
/// Implemented for the std range types and `(Bound<Q>, Bound<Q>)` where
/// `Q: Into<I>`, and for `..` (`RangeFull`).  Unlike `RangeBounds`,
/// endpoints are converted to owned values once per call, not per
/// comparison.
pub trait IntoBounds<I> {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>);
}

impl<I> IntoBounds<I> for std::ops::RangeFull {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>) {
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
    }
}

impl<Q: Into<I>, I> IntoBounds<I> for std::ops::Range<Q> {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>) {
        (
            std::ops::Bound::Included(self.start.into()),
            std::ops::Bound::Excluded(self.end.into()),
        )
    }
}

impl<Q: Into<I>, I> IntoBounds<I> for std::ops::RangeInclusive<Q> {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>) {
        let (start, end) = self.into_inner();
        (
            std::ops::Bound::Included(start.into()),
            std::ops::Bound::Included(end.into()),
        )
    }
}

impl<Q: Into<I>, I> IntoBounds<I> for std::ops::RangeFrom<Q> {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>) {
        (
            std::ops::Bound::Included(self.start.into()),
            std::ops::Bound::Unbounded,
        )
    }
}

impl<Q: Into<I>, I> IntoBounds<I> for std::ops::RangeTo<Q> {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>) {
        (
            std::ops::Bound::Unbounded,
            std::ops::Bound::Excluded(self.end.into()),
        )
    }
}

impl<Q: Into<I>, I> IntoBounds<I> for std::ops::RangeToInclusive<Q> {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>) {
        (
            std::ops::Bound::Unbounded,
            std::ops::Bound::Included(self.end.into()),
        )
    }
}

impl<Q: Into<I>, I> IntoBounds<I> for (std::ops::Bound<Q>, std::ops::Bound<Q>) {
    fn into_bounds(self) -> (std::ops::Bound<I>, std::ops::Bound<I>) {
        (self.0.map(Into::into), self.1.map(Into::into))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lvalue_exact() {
        assert_eq!(LValue::Exact(42).exact(), Some(&42));
        assert_eq!(LValue::<i32>::NegInfinity.exact(), None);
        assert_eq!(LValue::<i32>::Infinity.exact(), None);
    }

    #[test]
    fn test_lvalue_display() {
        assert_eq!(format!("{}", LValue::<i32>::NegInfinity), "-∞");
        assert_eq!(format!("{}", LValue::Exact(42)), "42");
        assert_eq!(format!("{}", LValue::Exact("hello")), "hello");
        assert_eq!(format!("{}", LValue::<i32>::Infinity), "∞");
    }

    #[test]
    fn test_lvalue_ordering() {
        assert!(LValue::<i32>::NegInfinity < LValue::Exact(i32::MIN));
        assert!(LValue::Exact(i32::MAX) < LValue::<i32>::Infinity);
        assert!(LValue::Exact(1) < LValue::Exact(2));
        assert!(LValue::<i32>::NegInfinity < LValue::<i32>::Infinity);
    }

    #[test]
    fn test_lindex1_from_value() {
        let idx = LIndex1::from(42i32);
        assert_eq!(idx.0, LValue::Exact(42));
    }

    #[test]
    fn test_lindex1_from_tuple() {
        let idx = LIndex1::from((42i32,));
        assert_eq!(idx.0, LValue::Exact(42));
    }

    #[test]
    fn test_lindex1_try_from_ref() {
        let idx = LIndex1::from(42i32);
        let tuple: (i32,) = (&idx).try_into().unwrap();
        assert_eq!(tuple, (42,));
    }

    #[test]
    fn test_lindex1_try_from_ref_incomplete() {
        let idx = LIndex1::<i32>(LValue::NegInfinity);
        let result: Result<(i32,), _> = (&idx).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_lindex2_from_tuple() {
        let idx = LIndex2::from(("hello".to_string(), 42i64));
        assert_eq!(idx.0, LValue::Exact("hello".to_string()));
        assert_eq!(idx.1, LValue::Exact(42i64));
    }

    #[test]
    fn test_lindex2_try_from_ref() {
        let idx = LIndex2::from(("key".to_string(), 99i64));
        let tuple: (String, i64) = (&idx).try_into().unwrap();
        assert_eq!(tuple, ("key".to_string(), 99));
    }

    #[test]
    fn test_lindex2_try_from_ref_incomplete() {
        let idx =
            LIndex2::<String, i64>(LValue::Exact("key".to_string()), LValue::Infinity);
        let result: Result<(String, i64), _> = (&idx).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_lindex3_from_tuple() {
        let idx = LIndex3::from((1i32, 2i32, 3i32));
        assert_eq!(idx.0, LValue::Exact(1));
        assert_eq!(idx.1, LValue::Exact(2));
        assert_eq!(idx.2, LValue::Exact(3));
    }

    #[test]
    fn test_lindex3_try_from_ref() {
        let idx = LIndex3::from((1i32, 2i32, 3i32));
        let tuple: (i32, i32, i32) = (&idx).try_into().unwrap();
        assert_eq!(tuple, (1, 2, 3));
    }

    #[test]
    fn test_lindex4_roundtrip() {
        let idx = LIndex4::from((1i32, 2i32, 3i32, 4i32));
        let tuple: (i32, i32, i32, i32) = (&idx).try_into().unwrap();
        assert_eq!(tuple, (1, 2, 3, 4));
    }

    #[test]
    fn test_lindex5_roundtrip() {
        let idx = LIndex5::from((1i32, 2i32, 3i32, 4i32, 5i32));
        let tuple: (i32, i32, i32, i32, i32) = (&idx).try_into().unwrap();
        assert_eq!(tuple, (1, 2, 3, 4, 5));
    }

    #[test]
    fn test_lindex_debug() {
        let idx = LIndex1::from(42i32);
        let debug = format!("{:?}", idx);
        assert!(debug.contains("42"));
    }

    #[test]
    fn test_lindex_clone_eq_hash() {
        let idx1 = LIndex1::from(42i32);
        let idx2 = idx1.clone();
        assert_eq!(idx1, idx2);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(idx1.clone());
        assert!(set.contains(&idx2));
    }

    #[test]
    fn test_lookup_str_zero_alloc() {
        let mut map: BTreeMap<LIndex1<String>, i32> = BTreeMap::new();
        map.insert(LIndex1::from("hello".to_string()), 1);
        map.insert(LIndex1::from("world".to_string()), 2);
        // &str via Borrow<str>, no String constructed
        assert_eq!("hello".lookup(&map), Some(&1));
        assert_eq!("nope".lookup(&map), None);
        // &String
        let key = "world".to_string();
        assert_eq!((&key).lookup(&map), Some(&2));
        // bare owned value and exact 1-tuple
        assert_eq!("hello".to_string().lookup(&map), Some(&1));
        assert_eq!(("hello".to_string(),).lookup(&map), Some(&1));
        // LIndex by value and by reference
        let idx = LIndex1::from("world".to_string());
        assert_eq!((&idx).lookup(&map), Some(&2));
        assert_eq!(idx.lookup(&map), Some(&2));
    }

    #[test]
    fn test_lookup_cow_str() {
        let mut map: BTreeMap<LIndex1<Cow<'static, str>>, i32> = BTreeMap::new();
        map.insert(LIndex1::from(Cow::Borrowed("hello")), 1);
        assert_eq!("hello".lookup(&map), Some(&1));
        let local = "hello".to_string();
        let cow: Cow<'_, str> = Cow::Borrowed(local.as_str());
        assert_eq!((&cow).lookup(&map), Some(&1));
        assert_eq!(
            Cow::<'static, str>::Borrowed("hello").lookup(&map),
            Some(&1)
        );
    }

    #[test]
    fn test_lookup_tuples_convert_via_into() {
        let mut map: BTreeMap<LIndex2<String, i64>, i32> = BTreeMap::new();
        map.insert(LIndex2::from(("alice".to_string(), 7i64)), 1);
        assert_eq!(("alice", 7i64).lookup(&map), Some(&1));
        assert_eq!(("alice", 7u8).lookup(&map), Some(&1));
        assert_eq!(("alice".to_string(), 7i64).lookup(&map), Some(&1));
        assert_eq!(("bob", 7i64).lookup(&map), None);
    }

    #[test]
    fn test_into_bounds() {
        use std::ops::Bound;

        let (s, e) = IntoBounds::<String>::into_bounds("a".."b");
        assert_eq!(s, Bound::Included("a".to_string()));
        assert_eq!(e, Bound::Excluded("b".to_string()));
        let (s, e) = IntoBounds::<String>::into_bounds("a"..="b");
        assert_eq!(s, Bound::Included("a".to_string()));
        assert_eq!(e, Bound::Included("b".to_string()));
        let (s, e) = IntoBounds::<String>::into_bounds("a"..);
        assert_eq!(s, Bound::Included("a".to_string()));
        assert_eq!(e, Bound::Unbounded);
        let (s, e) = IntoBounds::<String>::into_bounds(.."b");
        assert_eq!(s, Bound::Unbounded);
        assert_eq!(e, Bound::Excluded("b".to_string()));
        let (s, e) = IntoBounds::<String>::into_bounds(..="b");
        assert_eq!(s, Bound::Unbounded);
        assert_eq!(e, Bound::Included("b".to_string()));
        let (s, e) = IntoBounds::<String>::into_bounds(..);
        assert_eq!(s, Bound::Unbounded);
        assert_eq!(e, Bound::Unbounded);
        let (s, e) =
            IntoBounds::<String>::into_bounds((Bound::Excluded("a"), Bound::Unbounded));
        assert_eq!(s, Bound::Excluded("a".to_string()));
        assert_eq!(e, Bound::Unbounded);
        // owned endpoints and integer widening also work
        let (s, e) = IntoBounds::<String>::into_bounds("a".to_string().."b".to_string());
        assert_eq!(s, Bound::Included("a".to_string()));
        assert_eq!(e, Bound::Excluded("b".to_string()));
        let (s, e) = IntoBounds::<i64>::into_bounds(1u8..=2u8);
        assert_eq!(s, Bound::Included(1i64));
        assert_eq!(e, Bound::Included(2i64));
    }

    #[test]
    fn test_borrow_hash_consistency() {
        // the Borrow contract requires hash(key) == hash(key.borrow()),
        // which LValue's manual Hash impl provides for Exact values
        use std::collections::HashMap;
        let mut map: HashMap<LIndex1<String>, i32> = HashMap::new();
        map.insert(LIndex1::from("hello".to_string()), 1);
        assert_eq!(map.get("hello"), Some(&1));
        assert_eq!(map.get("nope"), None);
    }
}
