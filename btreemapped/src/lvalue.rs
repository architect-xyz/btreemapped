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
