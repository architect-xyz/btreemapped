//! Concrete replica of a database table in memory as a BTreeMap.

use crate::{lvalue::*, BTreeMapped};
use btreemapped_derive::impl_for_range;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;
use tokio::sync::{broadcast, Notify};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeSnapshot<T: BTreeMapped<N>, const N: usize> {
    pub seqid: u64,
    pub seqno: u64,
    // NB: BTreeMap<K, V> isn't representable in JSON for complex K,
    // so we deliver Vec<(K, V)> instead.
    pub snapshot: Vec<(T::Index, T::Unindexed)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeUpdate<T: BTreeMapped<N>, const N: usize> {
    pub seqid: u64,
    pub seqno: u64,
    // if snapshot is provided, always use it, then apply updates
    // as normal; server may supply a snapshot at any time
    pub snapshot: Option<Vec<(T::Index, T::Unindexed)>>,
    // semantics of updates are (K, None) for delete, (K, Some(V)) for insert/update
    pub updates: Vec<(T::Index, Option<T::Unindexed>)>,
}

#[derive(Debug, Clone)]
pub struct Sequenced<T> {
    pub seqid: u64,
    pub seqno: u64,
    pub t: T,
}

impl<T> std::ops::Deref for Sequenced<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.t
    }
}

impl<T> std::ops::DerefMut for Sequenced<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.t
    }
}

// NB alee: a bit unfortunate that we have to specify the INDEX_ARITY here
#[derive(Debug, Clone)]
pub struct BTreeMapReplica<T: BTreeMapped<N>, const N: usize> {
    pub replica: Arc<RwLock<Sequenced<BTreeMap<T::LIndex, T::Unindexed>>>>,
    pub changed: Arc<Notify>,
    pub updates: broadcast::Sender<Arc<BTreeUpdate<T, N>>>,
}

#[derive(Debug, Error)]
pub enum BTreeMapSyncError {
    #[error("seqid mismatch")]
    SeqidMismatch,
    #[error("seqno skip")]
    SeqnoSkip,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapReplica<T, N> {
    pub fn new() -> Self {
        let (updates, _) = broadcast::channel(100);
        Self {
            replica: Arc::new(RwLock::new(Sequenced {
                seqid: 0,
                seqno: 0,
                t: BTreeMap::new(),
            })),
            changed: Arc::new(Notify::new()),
            updates,
        }
    }

    #[cfg(test)]
    fn insert(&mut self, t: T) {
        let mut replica = self.replica.write();
        let (i, u) = t.into_kv();
        replica.insert(i.into(), u);
    }

    pub(crate) fn set_seqid(&mut self, seqid: u64) {
        let mut replica = self.replica.write();
        replica.seqid = seqid;
    }

    pub fn snapshot(&self) -> BTreeSnapshot<T, N> {
        let replica = self.replica.read();
        let mut snapshot = vec![];
        for (k, v) in replica.iter() {
            let i: T::Index = k.try_into().ok().unwrap();
            let u = v.clone();
            snapshot.push((i, u));
        }
        BTreeSnapshot { seqid: replica.seqid, seqno: replica.seqno, snapshot }
    }

    pub fn apply_snapshot(&mut self, snap: BTreeSnapshot<T, N>) -> (u64, u64) {
        let mut replica = self.replica.write();
        replica.clear();
        for (i, u) in snap.snapshot.iter() {
            replica.insert(i.clone().into(), u.clone());
        }
        replica.seqid = snap.seqid;
        replica.seqno = snap.seqno;
        self.changed.notify_waiters();
        (replica.seqid, replica.seqno)
    }

    pub fn apply_update(
        &mut self,
        up: BTreeUpdate<T, N>,
    ) -> Result<(u64, u64), BTreeMapSyncError> {
        let mut replica = self.replica.write();
        if let Some(snapshot) = up.snapshot.as_ref() {
            replica.clear();
            for (i, u) in snapshot.iter() {
                replica.insert(i.clone().into(), u.clone());
            }
            replica.seqid = up.seqid;
            replica.seqno = up.seqno;
        } else {
            if up.seqid != replica.seqid {
                return Err(BTreeMapSyncError::SeqidMismatch);
            }
            if up.seqno != replica.seqno + 1 {
                return Err(BTreeMapSyncError::SeqnoSkip);
            }
        }
        for (i, u) in up.updates.iter() {
            match u {
                Some(u) => {
                    replica.insert(i.clone().into(), u.clone());
                }
                None => {
                    replica.remove(&i.clone().into());
                }
            }
        }
        replica.seqno = up.seqno;
        Ok((replica.seqid, replica.seqno))
    }

    // CR alee: I bet if we used a normal looking BTreeMap, like BTreeMap<T::LIndex, T>
    // instead and just accept an extra index clone...then we would use parking_lot's
    // mutex maps and get a normal person interface instead of w/e this mess is.
    //
    // And it would use the same amount of space anyways as the Postgres original.

    /// Visit the item referenced by the given index, call `f` if it exists.
    /// Returns true if the item exists, false otherwise.
    pub fn where_<F>(&self, i: T::Index, f: F) -> bool
    where
        F: FnOnce(T::Ref<'_>),
    {
        let index: T::LIndex = i.into();
        let replica = self.replica.read();
        let t = replica.get(&index).and_then(|u| T::kv_as_ref(&index, u));
        if let Some(t) = t {
            f(t);
            true
        } else {
            false
        }
    }

    pub fn get_map<F, U>(&self, i: T::Index, f: F) -> Option<U>
    where
        F: Fn(T::Ref<'_>) -> U,
    {
        let mut u = None;
        self.where_(i, |t| u = Some(f(t)));
        u
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(T::Ref<'_>),
    {
        let replica = self.replica.read();
        for (k, v) in replica.iter() {
            if let Some(t) = T::kv_as_ref(k, v) {
                f(t);
            }
        }
    }
}

impl<T: BTreeMapped<N>, const N: usize> std::ops::Deref for BTreeMapReplica<T, N> {
    type Target = Arc<RwLock<Sequenced<BTreeMap<T::LIndex, T::Unindexed>>>>;

    fn deref(&self) -> &Self::Target {
        &self.replica
    }
}

impl_for_range!(LIndex1<I0>);
impl_for_range!(LIndex2<I0, I1>);
impl_for_range!(LIndex3<I0, I1, I2>);
impl_for_range!(LIndex4<I0, I1, I2, I3>);

// CR alee: consider the trick in https://stackoverflow.com/questions/45786717/how-to-get-value-from-hashmap-with-two-keys-via-references-to-both-keys

#[cfg(test)]
mod tests {
    use super::*;
    use btreemapped_derive::BTreeMapped;
    use chrono::{DateTime, Utc};
    use std::borrow::Cow;

    #[derive(Debug, Clone, BTreeMapped)]
    #[btreemap(index = ["key"])]
    struct Foo {
        key: String,
        bar: Option<DateTime<Utc>>,
    }

    impl Foo {
        fn new(key: &str, bar: Option<DateTime<Utc>>) -> Self {
            Self { key: key.to_string(), bar }
        }
    }

    #[test]
    fn test_btreemap_replica1() {
        let mut replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        replica.insert(Foo::new("abc", None));
        replica.insert(Foo::new("def", Some("2024-03-01T00:30:44Z".parse().unwrap())));
        replica.insert(Foo::new("ghi", None));
        let mut io = vec![];
        replica.for_range1("abc".to_string()..="ghi".to_string(), |foo| {
            io.push(format!("{} {:?}", foo.key, foo.bar));
        });
        assert_eq!(io, vec!["abc None", "def Some(2024-03-01T00:30:44Z)", "ghi None"]);
    }

    #[derive(Debug, Clone, BTreeMapped)]
    #[btreemap(index = ["owner", "license_plate"])]
    struct Car {
        owner: Cow<'static, str>,
        license_plate: i64,
        key: String,
    }

    impl Car {
        fn new(owner: &str, license_plate: i64, key: &str) -> Self {
            Self {
                owner: Cow::Owned(owner.to_string()),
                license_plate,
                key: key.to_string(),
            }
        }
    }

    #[test]
    fn test_btreemap_replica2() {
        let mut replica: BTreeMapReplica<Car, 2> = BTreeMapReplica::new();
        replica.insert(Car::new("Alice", 123, "abc"));
        replica.insert(Car::new("Charlie", 1000, "ghi"));
        replica.insert(Car::new("Charlie", 1001, "ghi"));
        replica.insert(Car::new("Bob", 456, "def"));
        replica.insert(Car::new("Bob", 888, "def"));
        replica.insert(Car::new("Charlie", 1002, "ghi"));
        replica.insert(Car::new("Charlie", 1003, "ghi"));
        let mut io = vec![];
        replica.for_range1(Cow::Borrowed("Alice")..=Cow::Borrowed("Bob"), |car| {
            io.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io, vec!["Alice 123 abc", "Bob 456 def", "Bob 888 def"]);
        let mut io2 = vec![];
        replica.for_range2(Cow::Borrowed("Alice"), 1000..=1002, |car| {
            io2.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io2, Vec::<String>::new());
        let mut io3 = vec![];
        replica.for_range2(Cow::Borrowed("Bob"), 456..=1003, |car| {
            io3.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io3, vec!["Bob 456 def", "Bob 888 def"]);
        let mut io4 = vec![];
        replica.for_range2(Cow::Borrowed("Charlie"), 1000..1003, |car| {
            io4.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io4, vec!["Charlie 1000 ghi", "Charlie 1001 ghi", "Charlie 1002 ghi"]);
    }
}

// TODO: consider https://github.com/boustrophedon/pgtemp for integration testing
