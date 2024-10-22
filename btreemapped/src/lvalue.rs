//! Helper struct for working with range bounds on BTreeMaps with tuple keys.
//!
//! BTreeMaps are not multi-dimensional structures, but we still need a way
//! to express the bounds on each tuple element respectively.  The user can
//! iterate over the BTreeMap as usual and use `LValue<T>`s as comparators.

use anyhow::anyhow;
use std::borrow::{Borrow, Cow};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LValue<'a, T: ToOwned + ?Sized> {
    /// Less than any/all T
    NegInfinity,
    /// Exactly T
    Exact(Cow<'a, T>),
    /// Greater than any/all T
    Infinity,
}

impl<'a, T: ToOwned + ?Sized> LValue<'a, T> {
    pub fn exact(&self) -> Option<&T> {
        match self {
            LValue::Exact(t) => Some(t),
            _ => None,
        }
    }

    pub fn into_exact(self) -> Option<T::Owned> {
        match self {
            LValue::Exact(t) => Some(t.into_owned()),
            _ => None,
        }
    }
}

impl<'a, T> std::fmt::Debug for LValue<'a, T>
where
    T: std::fmt::Debug + ToOwned,
    <T as ToOwned>::Owned: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LValue::NegInfinity => write!(f, "NegInfinity"),
            LValue::Exact(t) => write!(f, "Exact({t:?})"),
            LValue::Infinity => write!(f, "Infinity"),
        }
    }
}

impl<'a, T: ToOwned + std::fmt::Display> std::fmt::Display for LValue<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LValue::NegInfinity => write!(f, "-∞"),
            LValue::Exact(t) => t.fmt(f),
            LValue::Infinity => write!(f, "∞"),
        }
    }
}

// NB alee: tuples are always foreign, so we need to wrap them in a struct
// in order to define trait implementations for them.  We call this struct,
// which wraps a tuple of LValues, an LIndex.
macro_rules! lindex {
    ($name:ident, $($I:ident),+) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name<'a, $($I: ToOwned + ?Sized),+>($(pub LValue<'a, $I>,)+);

        impl<'a, $($I: ToOwned + ?Sized),+> std::fmt::Debug for $name<'a, $($I),+> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
            }
        }
    }
}

lindex!(LIndex2, I0, I1);
lindex!(LIndex3, I0, I1, I2);
lindex!(LIndex4, I0, I1, I2, I3);
lindex!(LIndex5, I0, I1, I2, I3, I4);

// TODO: put these in the macro

impl<I0, I1, J0, J1> From<(J0, J1)> for LIndex2<'static, I0, I1>
where
    I0: ToOwned + ?Sized,
    I1: ToOwned + ?Sized,
    <I0 as ToOwned>::Owned: From<J0>,
    <I1 as ToOwned>::Owned: From<J1>,
{
    fn from(value: (J0, J1)) -> Self {
        LIndex2(
            LValue::Exact(Cow::Owned(value.0.into())),
            LValue::Exact(Cow::Owned(value.1.into())),
        )
    }
}

impl<'a, I0, I1, J0, J1> TryFrom<&'a LIndex2<'static, I0, I1>> for (J0, J1)
where
    I0: ToOwned<Owned = J0> + Clone,
    I1: ToOwned<Owned = J1> + Clone,
{
    type Error = anyhow::Error;

    fn try_from(value: &'a LIndex2<'static, I0, I1>) -> Result<Self, Self::Error> {
        let i0 = value
            .0
            .exact()
            .ok_or_else(|| anyhow!("incomplete LIndex2"))?;
        let i1 = value
            .1
            .exact()
            .ok_or_else(|| anyhow!("incomplete LIndex2"))?;
        Ok((i0.to_owned(), i1.to_owned()))
    }
}

// impl<I0: Clone, I1: Clone> From<(I0, I1)> for LIndex2<'static, I0, I1> {
//     fn from(value: (I0, I1)) -> Self {
//         (
//             LValue::Exact(Cow::Owned(value.0)),
//             LValue::Exact(Cow::Owned(value.1)),
//         )
//     }
// }
