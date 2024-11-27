//! Concrete replica of a database table in memory as a BTreeMap.

use crate::{lvalue::*, BTreeMapped};
use anyhow::{bail, Result};
use btreemapped_derive::impl_for_range;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;
use tokio::sync::{broadcast, Notify};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeSnapshot<T: BTreeMapped<N>, const N: usize> {
    pub seqid: u64,
    pub seqno: u64,
    pub snapshot: Vec<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeUpdate<T: BTreeMapped<N>, const N: usize> {
    pub seqid: u64,
    pub seqno: u64,
    // if snapshot is provided, always use it, then apply updates
    // as normal; server may supply a snapshot at any time
    pub snapshot: Option<Vec<T>>,
    // semantics of updates are (K, None) for delete, (K, Some(V)) for insert/update
    pub updates: Vec<(T::Index, Option<T>)>,
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
    pub replica: Arc<RwLock<Sequenced<BTreeMap<T::LIndex, T>>>>,
    pub changed: Arc<Notify>,
    pub updates: broadcast::Sender<Arc<BTreeUpdate<T, N>>>,
    // If true, insert/remove directly to the replica will work; normally,
    // this should be false (signifying that this BTreeMapReplica is a
    // read-only or follower of some upstream source like a database or
    // a BTreeMapped publisher)--writes should go directly to the source
    // of truth.
    //
    // The use case for writable replicas is for testing or as a convenience
    // for local dev where you don't want to run a Postgres, e.g.
    read_only_replica: bool,
}

#[derive(Debug, Error)]
pub enum BTreeMapSyncError {
    #[error("seqid mismatch")]
    SeqidMismatch,
    #[error("seqno skip")]
    SeqnoSkip,
}

impl<T: BTreeMapped<N>, const N: usize> BTreeMapReplica<T, N> {
    pub fn new(seqid: u64) -> Self {
        let (updates, _) = broadcast::channel(100);
        Self {
            replica: Arc::new(RwLock::new(Sequenced {
                seqid,
                seqno: 0,
                t: BTreeMap::new(),
            })),
            changed: Arc::new(Notify::new()),
            updates,
            read_only_replica: true,
        }
    }

    /// Create an in-memory-only replica that can be written to directly.
    /// For testing or local dev convenience (e.g. not running a Postgres).
    pub fn new_in_memory() -> Self {
        let mut t = Self::new(0);
        t.read_only_replica = false;
        t
    }

    /// Same as `insert` but ignores the `read_only_replica` flag.
    #[cfg(test)]
    fn insert_for_test(&mut self, t: T) {
        let mut replica = self.replica.write();
        let i = t.index();
        replica.insert(i.into(), t);
    }

    pub fn insert(&mut self, t: T) -> Result<()> {
        if self.read_only_replica {
            bail!("cannot insert into read-only replica");
        }
        let (i, seqid, seqno) = {
            let mut replica = self.replica.write();
            let i = t.index();
            replica.insert(i.clone().into(), t.clone());
            replica.seqno += 1;
            (i, replica.seqid, replica.seqno)
        };
        self.changed.notify_waiters();
        let _ = self.updates.send(Arc::new(BTreeUpdate {
            seqid,
            seqno,
            snapshot: None,
            updates: vec![(i, Some(t))],
        }));
        Ok(())
    }

    pub fn remove(&mut self, i: &T::Index) -> Result<()> {
        if self.read_only_replica {
            bail!("cannot remove from read-only replica");
        }
        let (seqid, seqno) = {
            let mut replica = self.replica.write();
            replica.remove(&i.clone().into());
            replica.seqno += 1;
            (replica.seqid, replica.seqno)
        };
        self.changed.notify_waiters();
        let _ = self.updates.send(Arc::new(BTreeUpdate {
            seqid,
            seqno,
            snapshot: None,
            updates: vec![(i.clone(), None)],
        }));
        Ok(())
    }

    pub fn snapshot(&self) -> BTreeSnapshot<T, N> {
        let replica = self.replica.read();
        let mut snapshot = vec![];
        for (_i, t) in replica.iter() {
            snapshot.push(t.clone());
        }
        BTreeSnapshot { seqid: replica.seqid, seqno: replica.seqno, snapshot }
    }

    pub fn apply_snapshot(&mut self, snap: BTreeSnapshot<T, N>) -> (u64, u64) {
        let mut replica = self.replica.write();
        replica.clear();
        for t in snap.snapshot {
            let i = t.index();
            replica.insert(i.into(), t);
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
        if let Some(snapshot) = up.snapshot {
            replica.clear();
            for t in snapshot {
                let i = t.index();
                replica.insert(i.into(), t);
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
        for (i, t) in up.updates {
            match t {
                Some(t) => {
                    replica.insert(i.into(), t);
                }
                None => {
                    replica.remove(&i.into());
                }
            }
        }
        replica.seqno = up.seqno;
        Ok((replica.seqid, replica.seqno))
    }

    pub fn get<Q>(&self, i: Q) -> Option<MappedRwLockReadGuard<T>>
    where
        Q: Into<T::LIndex>,
    {
        let i: T::LIndex = i.into();
        let replica = self.replica.read();
        match RwLockReadGuard::try_map(replica, |r| r.get(&i)) {
            Ok(t) => Some(t),
            Err(_) => None,
        }
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&T),
    {
        let replica = self.replica.read();
        for (_i, t) in replica.iter() {
            f(t);
        }
    }
}

impl<T: BTreeMapped<N>, const N: usize> std::ops::Deref for BTreeMapReplica<T, N> {
    type Target = Arc<RwLock<Sequenced<BTreeMap<T::LIndex, T>>>>;

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
        let mut replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new(0);
        replica.insert_for_test(Foo::new("abc", None));
        replica.insert_for_test(Foo::new(
            "def",
            Some("2024-03-01T00:30:44Z".parse().unwrap()),
        ));
        replica.insert_for_test(Foo::new("ghi", None));
        let foo = replica.get("def".to_string()).unwrap();
        assert_eq!(foo.key, "def");
        assert_eq!(foo.bar, Some("2024-03-01T00:30:44Z".parse().unwrap()));
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
        let mut replica: BTreeMapReplica<Car, 2> = BTreeMapReplica::new(0);
        replica.insert_for_test(Car::new("Alice", 123, "abc"));
        replica.insert_for_test(Car::new("Charlie", 1000, "ghi"));
        replica.insert_for_test(Car::new("Charlie", 1001, "ghi"));
        replica.insert_for_test(Car::new("Bob", 456, "def"));
        replica.insert_for_test(Car::new("Bob", 888, "def"));
        replica.insert_for_test(Car::new("Charlie", 1002, "ghi"));
        replica.insert_for_test(Car::new("Charlie", 1003, "ghi"));
        let elem = replica.get((Cow::Borrowed("Alice"), 123)).unwrap();
        assert_eq!(elem.key, "abc");
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
    
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    enum Something {
        A,
        B
    }

    impl std::str::FromStr for Something {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(match s {
                "A" => Self::A,
                "B" => Self::B,
                _ => bail!("invalid value"),
            })
        }
    }

    /// Shows that parse works for Option<T>s when T implements FromStr
    #[derive(Debug, Clone, BTreeMapped)]
    #[btreemap(index = ["custom"])]
    struct Baz {
        #[parse]
        custom: Option<Something>,
    }

}

// TODO: consider https://github.com/boustrophedon/pgtemp for integration testing
