//! Concrete replica of a database table in memory as a BTreeMap.

use crate::{lvalue::*, BTreeMapped};
use anyhow::{bail, Result};
use async_stream::try_stream;
use btreemapped_derive::impl_for_range;
use futures::Stream;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;
use tokio::sync::{broadcast, watch};

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

#[derive(Debug, Clone, Copy, Error)]
pub enum BTreeUpdateStreamError {
    #[error("seqno skipped")]
    SeqnoSkip,
    #[error("stream lagged")]
    Lagged,
    #[error("stream ended")]
    Closed,
}

/// Template struct provided for e.g. proxied writes to database;
///
/// Deletes should be effected after the upsert, and it's not expected
/// for the upsert/delete pair to be atomic.  Writes from multiple
/// sources may interleave and the ultimate source of truth should
/// always be via replication.
///
/// E.g. knowing that you have send a sequence BTreeWrites to the
/// upstream replicator does not imply that the final state of your
/// replica can be computed from your own knowledge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeWrite<T: BTreeMapped<N>, const N: usize> {
    pub upsert: Option<Vec<T>>,
    pub delete: Option<Vec<T::Index>>,
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
    pub sequence: watch::Sender<(u64, u64)>,
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
    /// Create a new replica with a random sequence ID
    pub fn new() -> Self {
        let seqid = rand::random::<u64>();
        Self::new_sequence(seqid)
    }

    /// Create a new replica with a specific sequence ID
    pub fn new_sequence(seqid: u64) -> Self {
        let (updates, _) = broadcast::channel(100);
        let (changed, _) = watch::channel((seqid, 0));
        Self {
            replica: Arc::new(RwLock::new(Sequenced {
                seqid,
                seqno: 0,
                t: BTreeMap::new(),
            })),
            sequence: changed,
            updates,
            read_only_replica: true,
        }
    }

    /// Create an in-memory-only replica that can be written to directly.
    /// For testing or local dev convenience (e.g. not running a Postgres).
    pub fn new_in_memory() -> Self {
        let mut t = Self::new_sequence(0);
        t.read_only_replica = false;
        t
    }

    /// Same as `insert` but ignores the `read_only_replica` flag.
    #[cfg(test)]
    fn insert_for_test(&self, t: T) {
        let mut replica = self.replica.write();
        let i = t.index();
        replica.insert(i.into(), t);
    }

    pub fn insert(&self, t: T) -> Result<()> {
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
        let _ = self.sequence.send_replace((seqid, seqno));
        let _ = self.updates.send(Arc::new(BTreeUpdate {
            seqid,
            seqno,
            snapshot: None,
            updates: vec![(i, Some(t))],
        }));
        Ok(())
    }

    pub fn update(&self, i: &T::Index, f: impl FnOnce(T) -> Result<T>) -> Result<()> {
        if self.read_only_replica {
            bail!("cannot insert into read-only replica");
        }
        if let Some((i, t_new, seqid, seqno)) = {
            let mut replica = self.replica.write();
            match replica.remove(&i.clone().into()) {
                Some(t) => {
                    let t_new = f(t)?;
                    replica.insert(i.clone().into(), t_new.clone());
                    replica.seqno += 1;
                    Some((i.clone(), t_new, replica.seqid, replica.seqno))
                }
                None => None,
            }
        } {
            let _ = self.sequence.send_replace((seqid, seqno));
            let _ = self.updates.send(Arc::new(BTreeUpdate {
                seqid,
                seqno,
                snapshot: None,
                updates: vec![(i, Some(t_new))],
            }));
        }
        Ok(())
    }

    pub fn remove(&self, i: &T::Index) -> Result<()> {
        if self.read_only_replica {
            bail!("cannot remove from read-only replica");
        }
        let (seqid, seqno) = {
            let mut replica = self.replica.write();
            replica.remove(&i.clone().into());
            replica.seqno += 1;
            (replica.seqid, replica.seqno)
        };
        let _ = self.sequence.send_replace((seqid, seqno));
        let _ = self.updates.send(Arc::new(BTreeUpdate {
            seqid,
            seqno,
            snapshot: None,
            updates: vec![(i.clone(), None)],
        }));
        Ok(())
    }

    pub fn apply_write(&self, w: BTreeWrite<T, N>) -> Result<()> {
        if self.read_only_replica {
            bail!("cannot apply write to read-only replica");
        }
        let mut applied_updates = vec![];
        let (seqid, seqno) = {
            let mut replica = self.replica.write();
            if let Some(upsert) = w.upsert {
                for t in upsert {
                    let i = t.index();
                    applied_updates.push((i.clone(), Some(t.clone())));
                    replica.insert(i.into(), t);
                }
            }
            if let Some(delete) = w.delete {
                for i in delete {
                    applied_updates.push((i.clone(), None));
                    replica.remove(&i.into());
                }
            }
            replica.seqno += 1;
            (replica.seqid, replica.seqno)
        };
        let _ = self.sequence.send_replace((seqid, seqno));
        let _ = self.updates.send(Arc::new(BTreeUpdate {
            seqid,
            seqno,
            snapshot: None,
            updates: applied_updates,
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

    /// Subscribe to a self-contained updates stream.  The first element is
    /// guaranteed to contain a snapshot, and subsequent updates guaranteed
    /// to be in strict seqno order.
    pub fn subscribe(
        &self,
    ) -> impl Stream<Item = Result<Arc<BTreeUpdate<T, N>>, BTreeUpdateStreamError>> {
        let mut updates = self.updates.subscribe();
        let snap = self.snapshot();
        let mut last_seqno = snap.seqno;
        let snap_as_update = Arc::new(BTreeUpdate {
            seqid: snap.seqid,
            seqno: snap.seqno,
            snapshot: Some(snap.snapshot),
            updates: vec![],
        });
        let stream = try_stream! {
            yield snap_as_update;
            loop {
                match updates.recv().await {
                    Ok(up) => {
                        if up.seqno <= last_seqno {
                            continue;
                        } else if up.seqno == last_seqno + 1 {
                            last_seqno = up.seqno;
                            yield up;
                        } else {
                            Err(BTreeUpdateStreamError::SeqnoSkip)?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        Err(BTreeUpdateStreamError::Lagged)?;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        Err(BTreeUpdateStreamError::Closed)?;
                    }
                }
            }
        };
        stream
    }

    /// When syncing from an upstream replicator, use `apply_snapshot`
    /// on every BTreeSnapshot received.
    pub fn apply_snapshot(&mut self, snap: BTreeSnapshot<T, N>) -> (u64, u64) {
        let mut replica = self.replica.write();
        replica.clear();
        for t in snap.snapshot {
            let i = t.index();
            replica.insert(i.into(), t);
        }
        replica.seqid = snap.seqid;
        replica.seqno = snap.seqno;
        let _ = self.sequence.send_replace((replica.seqid, replica.seqno));
        (replica.seqid, replica.seqno)
    }

    /// When syncing from an upstream replicator, use `apply_update`
    /// on every BTreeUpdate received.
    pub fn apply_update(
        &mut self,
        up: Arc<BTreeUpdate<T, N>>,
    ) -> Result<(u64, u64), BTreeMapSyncError> {
        let mut replica = self.replica.write();
        if let Some(snapshot) = &up.snapshot {
            replica.clear();
            for t in snapshot {
                let i = t.index();
                replica.insert(i.into(), t.clone());
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
        for (i, t) in &up.updates {
            let i = i.clone();
            match t {
                Some(t) => {
                    replica.insert(i.into(), t.clone());
                }
                None => {
                    replica.remove(&i.into());
                }
            }
        }
        replica.seqno = up.seqno;
        let _ = self.sequence.send_replace((replica.seqid, replica.seqno));
        let _ = self.updates.send(up);
        Ok((replica.seqid, replica.seqno))
    }

    pub fn contains_key<Q: Lookup<T::LIndex>>(&self, i: Q) -> bool {
        let replica = self.replica.read();
        i.lookup(&replica).is_some()
    }

    pub fn get<Q: Lookup<T::LIndex>>(&self, i: Q) -> Option<MappedRwLockReadGuard<'_, T>> {
        let replica = self.replica.read();
        match RwLockReadGuard::try_map(replica, |r| i.lookup(r)) {
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
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        replica.insert_for_test(Foo::new("abc", None));
        replica.insert_for_test(Foo::new(
            "def",
            Some("2024-03-01T00:30:44Z".parse().unwrap()),
        ));
        replica.insert_for_test(Foo::new("ghi", None));
        // Zero-alloc lookup via Borrow<str> — no .to_string() needed
        let foo = replica.get("def").unwrap();
        assert_eq!(foo.key, "def");
        assert_eq!(foo.bar, Some("2024-03-01T00:30:44Z".parse().unwrap()));
        // Tuple form also works
        let foo2 = replica.get(("def",)).unwrap();
        assert_eq!(foo2.key, "def");
        let mut io = vec![];
        replica.for_range1("abc"..="ghi", |foo| {
            io.push(format!("{} {:?}", foo.key, foo.bar));
        });
        assert_eq!(io, vec!["abc None", "def Some(2024-03-01T00:30:44Z)", "ghi None"]);
    }

    #[derive(Debug, Clone, BTreeMapped)]
    #[btreemap(index = ["owner", "license_plate"])]
    struct Car {
        owner: String,
        license_plate: i64,
        key: String,
    }

    impl Car {
        fn new(owner: &str, license_plate: i64, key: &str) -> Self {
            Self {
                owner: owner.to_string(),
                license_plate,
                key: key.to_string(),
            }
        }
    }

    #[test]
    fn test_btreemap_replica2() {
        let replica: BTreeMapReplica<Car, 2> = BTreeMapReplica::new();
        replica.insert_for_test(Car::new("Alice", 123, "abc"));
        replica.insert_for_test(Car::new("Charlie", 1000, "ghi"));
        replica.insert_for_test(Car::new("Charlie", 1001, "ghi"));
        replica.insert_for_test(Car::new("Bob", 456, "def"));
        replica.insert_for_test(Car::new("Bob", 888, "def"));
        replica.insert_for_test(Car::new("Charlie", 1002, "ghi"));
        replica.insert_for_test(Car::new("Charlie", 1003, "ghi"));
        // &str auto-converts to String via Into
        let elem = replica.get(("Alice", 123)).unwrap();
        assert_eq!(elem.key, "abc");
        let mut io = vec![];
        replica.for_range1("Alice"..="Bob", |car| {
            io.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io, vec!["Alice 123 abc", "Bob 456 def", "Bob 888 def"]);
        let mut io2 = vec![];
        replica.for_range2("Alice", 1000..=1002, |car| {
            io2.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io2, Vec::<String>::new());
        let mut io3 = vec![];
        replica.for_range2("Bob", 456..=1003, |car| {
            io3.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io3, vec!["Bob 456 def", "Bob 888 def"]);
        let mut io4 = vec![];
        replica.for_range2("Charlie", 1000..1003, |car| {
            io4.push(format!("{} {} {}", car.owner, car.license_plate, car.key));
        });
        assert_eq!(io4, vec!["Charlie 1000 ghi", "Charlie 1001 ghi", "Charlie 1002 ghi"]);
    }

    // NB: testing the macro in `Baz` compiles
    #[allow(dead_code)]
    #[derive(
        Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
    )]
    enum Something {
        A,
        B,
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
    #[allow(dead_code)] // NB: testing the macro compiles
    #[derive(Debug, Clone, BTreeMapped)]
    #[btreemap(index = ["custom"])]
    struct Baz {
        #[parse]
        custom: Option<Something>,
    }
}

// TODO: consider https://github.com/boustrophedon/pgtemp for integration testing
