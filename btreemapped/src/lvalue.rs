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
        let idx = LIndex2::<String, i64>(
            LValue::Exact("key".to_string()),
            LValue::Infinity,
        );
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
}
