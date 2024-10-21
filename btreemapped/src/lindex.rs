//! LIndex lets you specify range bounds on BTreeMaps with tuple keys.
//! It's designed to match the semantics of postgres B-tree indexes.
//!
//! For a BTreeMap with tuple keys, it's not generally possible to
//! express something like (1, 1, 3..5) as a range query.  Instead,
//! consider writing a BTreeMap with LIndex keys, and use the range
//! query:
//!
//! ```rust,ignore
//! (Exact(1)..Exact(1), Exact(2)..Exact(2), Exact(3)..Exact(5)).
//! ```
//!
//! and subsequently using an includes check.
//!
//! > ...the index is most efficient when there are constraints on the
//! > leading (leftmost) columns. The exact rule is that equality
//! > constraints on leading columns, plus any inequality constraints
//! > on the first column that does not have an equality constraint,
//! > will be used to limit the portion of the index that is scanned.
//!
//! https://www.postgresql.org/docs/current/indexes-multicolumn.html

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl<T: std::fmt::Display> std::fmt::Display for LValue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LValue::NegInfinity => write!(f, "-∞"),
            LValue::Exact(t) => t.fmt(f),
            LValue::Infinity => write!(f, "∞"),
        }
    }
}

macro_rules! lindex {
    ($n:expr, [$($T:tt)*]) => {
        paste::paste! {
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct [<LIndex $n>]<$($T),*>(pub ($(LValue<$T>,)*));

            impl<$($T),*> From<($($T,)*)> for [<LIndex $n>]<$($T),*> {
                fn from(($([<$T:lower>],)*) : ($($T,)*)) -> Self {
                    [<LIndex $n>](($(LValue::Exact([<$T:lower>])),*))
                }
            }

            impl<'a, $($T),*> TryFrom<&'a [<LIndex $n>]<$($T),*>> for ($($T,)*)
            where
                $($T: Clone),*
            {
                type Error = anyhow::Error;

                fn try_from(index: &'a [<LIndex $n>]<$($T),*>) -> Result<Self, Self::Error>
                {
                    let ($([<$T:lower>],)*) = &index.0;
                    $(
                        let [<$T:lower>] = match [<$T:lower>] {
                            LValue::Exact(t) => t.clone(),
                            _ => anyhow::bail!(concat!("incomplete LIndex", $n)),
                        };
                    )*
                    Ok(($([<$T:lower>],)*))
                }
            }
        }
    }
}

lindex!(2, [A B]);
lindex!(3, [A B C]);
lindex!(4, [A B C D]);

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, ops::Bound};

    #[test]
    fn test_lindex2_range_semantics() {
        let mut map_of_map: BTreeMap<i32, BTreeMap<i32, &'static str>> = BTreeMap::new();
        map_of_map.insert(
            1,
            [
                (1, "mary"),
                (2, "had"),
                (3, "a"),
                (4, "little"),
                (5, "lamb"),
            ]
            .into(),
        );
        map_of_map.insert(
            2,
            [(3, "the"), (4, "quick"), (5, "brown"), (6, "fox")].into(),
        );
        let mut flat_map: BTreeMap<LIndex2<i32, i32>, &'static str> = BTreeMap::new();
        flat_map.insert(LIndex2::from((1, 1)), "mary");
        flat_map.insert(LIndex2::from((1, 2)), "had");
        flat_map.insert(LIndex2::from((1, 3)), "a");
        flat_map.insert(LIndex2::from((1, 4)), "little");
        flat_map.insert(LIndex2::from((1, 5)), "lamb");
        flat_map.insert(LIndex2::from((2, 3)), "the");
        flat_map.insert(LIndex2::from((2, 4)), "quick");
        flat_map.insert(LIndex2::from((2, 5)), "brown");
        flat_map.insert(LIndex2::from((2, 6)), "fox");
        // Iterate over the two maps in an equivalent way, visiting
        // the same elements in the same order via range queries.
        let mut map_of_map_iteration = vec![];
        let mut flat_map_iteration = vec![];
        for (_k1, map) in map_of_map.range(..=2) {
            for (_k2, v) in map.range(4..) {
                map_of_map_iteration.push(v);
            }
        }
        let b1 = Bound::Included(LIndex2((LValue::NegInfinity, LValue::Exact(4))));
        let b2 = Bound::Included(LIndex2((LValue::Exact(2), LValue::Infinity)));
        for (LIndex2((k1, k2)), v) in flat_map.range((b1, b2)) {
            if k1 >= &LValue::NegInfinity
                && k1 <= &LValue::Exact(2)
                && k2 >= &LValue::Exact(4)
                && k2 <= &LValue::Infinity
            {
                println!("  {} {}", k1, k2);
                flat_map_iteration.push(v);
            } else {
                println!("X {} {}", k1, k2);
            }
        }
        assert_eq!(map_of_map_iteration, flat_map_iteration);
    }
}
