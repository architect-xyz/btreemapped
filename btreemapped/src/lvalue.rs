//! Helper struct for working with range bounds on BTreeMaps with tuple keys.
//!
//! BTreeMaps are not multi-dimensional structures, but we still need a way
//! to express the bounds on each tuple element respectively.  The user can
//! iterate over the BTreeMap as usual and use `LValue<T>`s as comparators.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LValue<T> {
    /// Less than any/all T
    NegInfinity,
    /// Exactly T
    Exact(T),
    /// Greater than any/all T
    Infinity,
}

impl<T> LValue<T> {
    pub fn exact(&self) -> Option<&T> {
        match self {
            LValue::Exact(t) => Some(t),
            _ => None,
        }
    }
}

impl<'a, T: std::fmt::Display> std::fmt::Display for LValue<T> {
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

// --- Borrow impls for zero-allocation BTreeMap lookups ---
//
// All BTreeMap keys are always LValue::Exact; NegInfinity/Infinity
// are only used for range bound construction and never stored as keys.
// This means the Borrow Ord-equivalence contract holds: for any two
// stored keys a, b: a.cmp(&b) == a.borrow().cmp(b.borrow()).

impl std::borrow::Borrow<str> for LIndex1<String> {
    fn borrow(&self) -> &str {
        self.0
            .exact()
            .expect("BTreeMap key must be LValue::Exact")
    }
}

impl std::borrow::Borrow<str> for LIndex1<std::borrow::Cow<'static, str>> {
    fn borrow(&self) -> &str {
        self.0
            .exact()
            .expect("BTreeMap key must be LValue::Exact")
    }
}

// --- GetKey: flexible lookup trait for BTreeMapReplica::get ---
//
// For &str on singleton String/Cow indexes, uses Borrow<str> for
// zero-allocation lookups. For tuples, converts each element via
// Into (may allocate for &str → String).

pub trait GetKey<L> {
    fn get_in_map<'a, V>(self, map: &'a std::collections::BTreeMap<L, V>) -> Option<&'a V>;
}

// Zero-alloc: &str on LIndex1<String> via Borrow<str>
impl<'k> GetKey<LIndex1<String>> for &'k str {
    fn get_in_map<'a, V>(
        self,
        map: &'a std::collections::BTreeMap<LIndex1<String>, V>,
    ) -> Option<&'a V> {
        map.get(self)
    }
}

// Zero-alloc: &str on LIndex1<Cow<'static, str>> via Borrow<str>
impl<'k> GetKey<LIndex1<std::borrow::Cow<'static, str>>> for &'k str {
    fn get_in_map<'a, V>(
        self,
        map: &'a std::collections::BTreeMap<LIndex1<std::borrow::Cow<'static, str>>, V>,
    ) -> Option<&'a V> {
        map.get(self)
    }
}

// Zero-alloc: pass a reference to an existing LIndex directly
impl<'k, L: Ord> GetKey<L> for &'k L {
    fn get_in_map<'a, V>(
        self,
        map: &'a std::collections::BTreeMap<L, V>,
    ) -> Option<&'a V> {
        map.get(self)
    }
}

// Singleton impls for common key types (no string allocation)
macro_rules! impl_singleton_getkey {
    ($($t:ty),*) => {
        $(
            impl GetKey<LIndex1<$t>> for $t {
                fn get_in_map<'a, V>(
                    self,
                    map: &'a std::collections::BTreeMap<LIndex1<$t>, V>,
                ) -> Option<&'a V> {
                    map.get(&LIndex1(LValue::Exact(self)))
                }
            }
        )*
    };
}

impl_singleton_getkey!(String, i8, i16, i32, i64, u8, u16, u32, u64, bool);

impl GetKey<LIndex1<std::borrow::Cow<'static, str>>> for std::borrow::Cow<'static, str> {
    fn get_in_map<'a, V>(
        self,
        map: &'a std::collections::BTreeMap<LIndex1<std::borrow::Cow<'static, str>>, V>,
    ) -> Option<&'a V> {
        map.get(&LIndex1(LValue::Exact(self)))
    }
}

// Tuple impls for all arities (each element converts via Into)
macro_rules! impl_tuple_getkey {
    ($name:ident, $($Q:ident => $I:ident),+) => {
        paste::paste! {
            impl<$($Q, $I),+> GetKey<$name<$($I),+>> for ($($Q,)+)
            where
                $($Q: Into<$I>, $I: Ord,)+
            {
                fn get_in_map<'a, V>(
                    self,
                    map: &'a std::collections::BTreeMap<$name<$($I),+>, V>,
                ) -> Option<&'a V> {
                    let ($([<$Q:lower>],)+) = self;
                    let key = $name($(LValue::Exact([<$Q:lower>].into()),)+);
                    map.get(&key)
                }
            }
        }
    };
}

impl_tuple_getkey!(LIndex1, Q0 => I0);
impl_tuple_getkey!(LIndex2, Q0 => I0, Q1 => I1);
impl_tuple_getkey!(LIndex3, Q0 => I0, Q1 => I1, Q2 => I2);
impl_tuple_getkey!(LIndex4, Q0 => I0, Q1 => I1, Q2 => I2, Q3 => I3);
impl_tuple_getkey!(LIndex5, Q0 => I0, Q1 => I1, Q2 => I2, Q3 => I3, Q4 => I4);
