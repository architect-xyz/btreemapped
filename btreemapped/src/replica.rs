//! Concrete replica of a database table in memory as a BTreeMap.

use crate::lvalue::*;
use crate::BTreeMapped;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use thiserror::Error;
use tokio::sync::{broadcast, Notify};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeSnapshot<T: BTreeMapped> {
    pub epoch: DateTime<Utc>,
    pub seqno: u64,
    // NB: BTreeMap<K, V> isn't representable in JSON for complex K,
    // so we deliver Vec<(K, V)> instead.
    pub snapshot: Vec<(T::Index, T::Unindexed)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeUpdate<T: BTreeMapped> {
    pub epoch: DateTime<Utc>,
    pub seqno: u64,
    // if snapshot is provided, always use it, then apply updates
    // as normal; server may supply a snapshot at any time
    pub snapshot: Option<Vec<(T::Index, T::Unindexed)>>,
    // semantics of updates are (K, None) for delete, (K, Some(V)) for insert/update
    pub updates: Vec<(T::Index, Option<T::Unindexed>)>,
}

#[derive(Debug, Clone)]
pub struct Sequenced<T> {
    pub epoch: DateTime<Utc>,
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

#[derive(Debug, Clone)]
pub struct BTreeMapReplica<T: BTreeMapped> {
    pub replica: Arc<RwLock<Sequenced<BTreeMap<T::LIndex, T::Unindexed>>>>,
    pub changed: Arc<Notify>,
    pub updates: broadcast::Sender<Arc<BTreeUpdate<T>>>,
}

#[derive(Debug, Error)]
pub enum BTreeMapSyncError {
    #[error("wrong epoch")]
    WrongEpoch,
    #[error("seqno skip")]
    SeqnoSkip,
}

impl<T: BTreeMapped> BTreeMapReplica<T> {
    pub fn new() -> Self {
        let (updates, _) = broadcast::channel(100);
        Self {
            replica: Arc::new(RwLock::new(Sequenced {
                epoch: DateTime::<Utc>::MIN_UTC,
                seqno: 0,
                t: BTreeMap::new(),
            })),
            changed: Arc::new(Notify::new()),
            updates,
        }
    }

    #[cfg(test)]
    fn insert(&mut self, t: T) {
        let mut replica = self.replica.write().unwrap();
        let (i, u) = t.into_kv();
        replica.insert(i.into(), u);
    }

    pub(crate) fn set_epoch(&mut self, epoch: DateTime<Utc>) {
        let mut replica = self.replica.write().unwrap();
        replica.epoch = epoch;
    }

    pub fn snapshot(&self) -> BTreeSnapshot<T> {
        let replica = self.replica.read().unwrap();
        let mut snapshot = vec![];
        for (k, v) in replica.iter() {
            let i: T::Index = k.try_into().ok().unwrap();
            let u = v.clone();
            snapshot.push((i, u));
        }
        BTreeSnapshot {
            epoch: replica.epoch,
            seqno: replica.seqno,
            snapshot,
        }
    }

    pub fn apply_snapshot(&mut self, snap: BTreeSnapshot<T>) -> (DateTime<Utc>, u64) {
        let mut replica = self.replica.write().unwrap();
        replica.clear();
        for (i, u) in snap.snapshot.iter() {
            replica.insert(i.clone().into(), u.clone());
        }
        replica.epoch = snap.epoch;
        replica.seqno = snap.seqno;
        self.changed.notify_waiters();
        (replica.epoch, replica.seqno)
    }

    pub fn apply_update(
        &mut self,
        up: BTreeUpdate<T>,
    ) -> Result<(DateTime<Utc>, u64), BTreeMapSyncError> {
        let mut replica = self.replica.write().unwrap();
        if let Some(snapshot) = up.snapshot.as_ref() {
            replica.clear();
            for (i, u) in snapshot.iter() {
                replica.insert(i.clone().into(), u.clone());
            }
            replica.epoch = up.epoch;
            replica.seqno = up.seqno;
        } else {
            if up.epoch != replica.epoch {
                return Err(BTreeMapSyncError::WrongEpoch);
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
        Ok((replica.epoch, replica.seqno))
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(T::Ref<'_>),
    {
        let replica = self.replica.read().unwrap();
        for (k, v) in replica.iter() {
            if let Some(t) = T::kv_as_ref(k, v) {
                f(t);
            }
        }
    }
}

impl<T: BTreeMapped> std::ops::Deref for BTreeMapReplica<T> {
    type Target = Arc<RwLock<Sequenced<BTreeMap<T::LIndex, T::Unindexed>>>>;

    fn deref(&self) -> &Self::Target {
        &self.replica
    }
}

// CR alee: consider the trick in https://stackoverflow.com/questions/45786717/how-to-get-value-from-hashmap-with-two-keys-via-references-to-both-keys
impl<T, I0, I1> BTreeMapReplica<T>
where
    T: BTreeMapped<LIndex = LIndex2<I0, I1>>,
    // TODO: possibly relaxable
    I0: Clone + Ord + 'static,
    I1: Clone + Ord + 'static,
{
    // NB: returning a mapped iterator not really possible given lifetime
    // constraints, so we expose for_rangeN() functions instead.
    pub fn for_range1<R, F>(&self, range: R, mut f: F)
    where
        R: std::ops::RangeBounds<I0>,
        F: FnMut(T::Ref<'_>),
    {
        // TODO: how do we get a str out here...it seems so close...
        let start: std::ops::Bound<LIndex2<I0, I1>> = range
            .start_bound()
            .map(|s| LIndex2(LValue::Exact(s.clone()), LValue::NegInfinity));
        let end: std::ops::Bound<LIndex2<I0, I1>> = range
            .end_bound()
            .map(|e| LIndex2(LValue::Exact(e.clone()), LValue::Infinity));
        let replica = self.replica.read().unwrap();
        for (k, v) in replica.range((start, end)) {
            if let Some(t) = T::kv_as_ref(k, v) {
                f(t);
            }
        }
    }

    pub fn for_range2<R, F>(&self, i0: I0, range: R, mut f: F)
    where
        R: std::ops::RangeBounds<I1>,
        F: FnMut(T::Ref<'_>),
    {
        let start: std::ops::Bound<LIndex2<I0, I1>> = range
            .start_bound()
            .map(|s| LIndex2(LValue::Exact(i0.clone()), LValue::Exact(s.clone())));
        let end: std::ops::Bound<LIndex2<I0, I1>> = range
            .end_bound()
            .map(|e| LIndex2(LValue::Exact(i0.clone()), LValue::Exact(e.clone())));
        let replica = self.replica.read().unwrap();
        for (k, v) in replica.range((start, end)) {
            if let Some(t) = T::kv_as_ref(k, v) {
                f(t);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btreemapped_derive::BTreeMapped;
    use std::borrow::Cow;

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
        let mut replica: BTreeMapReplica<Car> = BTreeMapReplica::new();
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
        assert_eq!(
            io4,
            vec!["Charlie 1000 ghi", "Charlie 1001 ghi", "Charlie 1002 ghi"]
        );
    }
}

// TODO: consider https://github.com/boustrophedon/pgtemp for integration testing
