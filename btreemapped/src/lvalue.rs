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

// NB alee: tuples are always foreign, so we need to wrap them in a struct
// in order to define trait implementations for them.  We call this struct,
// which wraps a tuple of LValues, an LIndex.
macro_rules! lindex {
    ($name:ident, $($I:ident),+) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name<$($I),+>($(pub LValue<$I>,)+);
    }
}

lindex!(LIndex2, I0, I1);
lindex!(LIndex3, I0, I1, I2);
lindex!(LIndex4, I0, I1, I2, I3);
lindex!(LIndex5, I0, I1, I2, I3, I4);

// TODO: put these in the macro

impl<I0, I1> From<(I0, I1)> for LIndex2<I0, I1> {
    fn from(value: (I0, I1)) -> Self {
        LIndex2(LValue::Exact(value.0), LValue::Exact(value.1))
    }
}

impl<'a, I0, I1> TryFrom<&'a LIndex2<I0, I1>> for (I0, I1)
where
    I0: Clone,
    I1: Clone,
{
    type Error = anyhow::Error;

    fn try_from(value: &'a LIndex2<I0, I1>) -> Result<Self, Self::Error> {
        // TODO: no unwrap
        Ok((
            value.0.exact().unwrap().clone(),
            value.1.exact().unwrap().clone(),
        ))
    }
}
