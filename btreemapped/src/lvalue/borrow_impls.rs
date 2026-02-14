//! Borrow impls for zero-allocation BTreeMap lookups.
//!
//! All BTreeMap keys are always `LValue::Exact`; `Bottom`/`Top`
//! are only used for range bound construction and never stored as keys.
//! This means the `Borrow` Ord-equivalence contract holds: for any two
//! stored keys a, b: `a.cmp(&b) == a.borrow().cmp(b.borrow())`.

use super::LIndex1;
use std::borrow::{Borrow, Cow};

// Blanket: LIndex1<T> borrows as T for any T (identity borrow).
// Covers all primitives, Option<T>, and any future index element types.
impl<T> Borrow<T> for LIndex1<T> {
    fn borrow(&self) -> &T {
        self.0.exact().expect("BTreeMap key must be LValue::Exact")
    }
}

// Special cases: LIndex1<String> and LIndex1<Cow<str>> also borrow as
// str, enabling zero-allocation BTreeMap lookups with &str keys.

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
