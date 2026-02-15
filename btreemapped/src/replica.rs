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

    pub fn contains_key<Q>(&self, i: Q) -> bool
    where
        Q: Into<T::LIndex>,
    {
        let i: T::LIndex = i.into();
        let replica = self.replica.read();
        replica.contains_key(&i)
    }

    pub fn get<Q>(&self, i: Q) -> Option<MappedRwLockReadGuard<'_, T>>
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
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
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
        #[try_from(String)]
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
        let replica: BTreeMapReplica<Car, 2> = BTreeMapReplica::new();
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

    #[test]
    fn test_new_in_memory_insert() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        replica.insert(Foo::new("a", None)).unwrap();
        replica.insert(Foo::new("b", None)).unwrap();
        assert!(replica.contains_key("a".to_string()));
        assert!(replica.contains_key("b".to_string()));
        assert!(!replica.contains_key("c".to_string()));
    }

    #[test]
    fn test_read_only_insert_fails() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        let result = replica.insert(Foo::new("a", None));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_only_update_fails() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        let result =
            replica.update(&("a".to_string(),), |f| Ok(f));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_only_remove_fails() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        let result = replica.remove(&("a".to_string(),));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_only_apply_write_fails() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        let w = BTreeWrite { upsert: None, delete: None };
        let result = replica.apply_write(w);
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_update() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        let bar = Some("2024-03-01T00:00:00Z".parse().unwrap());
        replica.insert(Foo::new("a", None)).unwrap();
        replica
            .update(&("a".to_string(),), |mut f| {
                f.bar = bar;
                Ok(f)
            })
            .unwrap();
        let got = replica.get("a".to_string()).unwrap();
        assert_eq!(got.bar, bar);
    }

    #[test]
    fn test_in_memory_update_nonexistent() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        // Updating a key that doesn't exist should be a no-op
        replica
            .update(&("missing".to_string(),), |f| Ok(f))
            .unwrap();
        assert!(!replica.contains_key("missing".to_string()));
    }

    #[test]
    fn test_in_memory_remove() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        replica.insert(Foo::new("a", None)).unwrap();
        assert!(replica.contains_key("a".to_string()));
        replica.remove(&("a".to_string(),)).unwrap();
        assert!(!replica.contains_key("a".to_string()));
    }

    #[test]
    fn test_apply_write_upsert_and_delete() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        replica.insert(Foo::new("delete_me", None)).unwrap();
        let w = BTreeWrite {
            upsert: Some(vec![Foo::new("new_key", None)]),
            delete: Some(vec![("delete_me".to_string(),)]),
        };
        replica.apply_write(w).unwrap();
        assert!(replica.contains_key("new_key".to_string()));
        assert!(!replica.contains_key("delete_me".to_string()));
    }

    #[test]
    fn test_snapshot() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        replica.insert(Foo::new("a", None)).unwrap();
        replica.insert(Foo::new("b", None)).unwrap();
        let snap = replica.snapshot();
        assert_eq!(snap.snapshot.len(), 2);
        assert_eq!(snap.snapshot[0].key, "a");
        assert_eq!(snap.snapshot[1].key, "b");
    }

    #[test]
    fn test_for_each() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        replica.insert_for_test(Foo::new("x", None));
        replica.insert_for_test(Foo::new("y", None));
        let mut keys = vec![];
        replica.for_each(|f| keys.push(f.key.clone()));
        assert_eq!(keys, vec!["x", "y"]);
    }

    #[test]
    fn test_get_nonexistent() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        assert!(replica.get("missing".to_string()).is_none());
    }

    #[test]
    fn test_deref_to_arc() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        replica.insert_for_test(Foo::new("a", None));
        // Test Deref to Arc<RwLock<...>>
        let r = replica.read();
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn test_sequenced_deref() {
        let s = Sequenced { seqid: 1, seqno: 2, t: vec![1, 2, 3] };
        assert_eq!(s.len(), 3);
        assert_eq!(s[0], 1);
    }

    #[test]
    fn test_sequenced_deref_mut() {
        let mut s = Sequenced { seqid: 1, seqno: 2, t: vec![1, 2, 3] };
        s.push(4);
        assert_eq!(s.len(), 4);
    }

    #[test]
    fn test_apply_snapshot() {
        let mut replica: BTreeMapReplica<Foo, 1> =
            BTreeMapReplica::new_in_memory();
        replica.insert(Foo::new("old", None)).unwrap();

        let snap = BTreeSnapshot {
            seqid: 42,
            seqno: 10,
            snapshot: vec![Foo::new("new1", None), Foo::new("new2", None)],
        };
        let (seqid, seqno) = replica.apply_snapshot(snap);
        assert_eq!(seqid, 42);
        assert_eq!(seqno, 10);
        assert!(!replica.contains_key("old".to_string()));
        assert!(replica.contains_key("new1".to_string()));
        assert!(replica.contains_key("new2".to_string()));
    }

    #[test]
    fn test_apply_update_with_snapshot() {
        let mut replica: BTreeMapReplica<Foo, 1> =
            BTreeMapReplica::new_sequence(0);
        let update = Arc::new(BTreeUpdate {
            seqid: 42,
            seqno: 5,
            snapshot: Some(vec![Foo::new("snap", None)]),
            updates: vec![(("extra".to_string(),), Some(Foo::new("extra", None)))],
        });
        let (seqid, seqno) = replica.apply_update(update).unwrap();
        assert_eq!(seqid, 42);
        assert_eq!(seqno, 5);
        assert!(replica.contains_key("snap".to_string()));
        assert!(replica.contains_key("extra".to_string()));
    }

    #[test]
    fn test_apply_update_incremental() {
        let mut replica: BTreeMapReplica<Foo, 1> =
            BTreeMapReplica::new_sequence(42);
        // seqno starts at 0, so next expected is 1
        let update = Arc::new(BTreeUpdate {
            seqid: 42,
            seqno: 1,
            snapshot: None,
            updates: vec![(("a".to_string(),), Some(Foo::new("a", None)))],
        });
        let result = replica.apply_update(update);
        assert!(result.is_ok());
        assert!(replica.contains_key("a".to_string()));
    }

    #[test]
    fn test_apply_update_seqid_mismatch() {
        let mut replica: BTreeMapReplica<Foo, 1> =
            BTreeMapReplica::new_sequence(42);
        let update = Arc::new(BTreeUpdate {
            seqid: 99, // wrong seqid
            seqno: 1,
            snapshot: None,
            updates: vec![],
        });
        let result = replica.apply_update(update);
        assert!(matches!(result, Err(BTreeMapSyncError::SeqidMismatch)));
    }

    #[test]
    fn test_apply_update_seqno_skip() {
        let mut replica: BTreeMapReplica<Foo, 1> =
            BTreeMapReplica::new_sequence(42);
        let update = Arc::new(BTreeUpdate {
            seqid: 42,
            seqno: 5, // skipped from 0 to 5
            snapshot: None,
            updates: vec![],
        });
        let result = replica.apply_update(update);
        assert!(matches!(result, Err(BTreeMapSyncError::SeqnoSkip)));
    }

    #[test]
    fn test_apply_update_delete() {
        let mut replica: BTreeMapReplica<Foo, 1> =
            BTreeMapReplica::new_sequence(42);
        // First insert via update with snapshot
        let update1 = Arc::new(BTreeUpdate {
            seqid: 42,
            seqno: 1,
            snapshot: None,
            updates: vec![(("a".to_string(),), Some(Foo::new("a", None)))],
        });
        replica.apply_update(update1).unwrap();
        assert!(replica.contains_key("a".to_string()));

        // Then delete via update
        let update2 = Arc::new(BTreeUpdate {
            seqid: 42,
            seqno: 2,
            snapshot: None,
            updates: vec![(("a".to_string(),), None)],
        });
        replica.apply_update(update2).unwrap();
        assert!(!replica.contains_key("a".to_string()));
    }

    #[tokio::test]
    async fn test_subscribe_stream() {
        use futures::StreamExt;

        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        replica.insert(Foo::new("initial", None)).unwrap();

        let mut stream = Box::pin(replica.subscribe());

        // First item should be a snapshot
        let first = stream.next().await.unwrap().unwrap();
        assert!(first.snapshot.is_some());
        assert_eq!(first.snapshot.as_ref().unwrap().len(), 1);
        assert_eq!(first.snapshot.as_ref().unwrap()[0].key, "initial");
    }

    #[test]
    fn test_btree_update_stream_error_display() {
        assert_eq!(
            format!("{}", BTreeUpdateStreamError::SeqnoSkip),
            "seqno skipped"
        );
        assert_eq!(
            format!("{}", BTreeUpdateStreamError::Lagged),
            "stream lagged"
        );
        assert_eq!(
            format!("{}", BTreeUpdateStreamError::Closed),
            "stream ended"
        );
    }

    #[test]
    fn test_btree_map_sync_error_display() {
        assert_eq!(
            format!("{}", BTreeMapSyncError::SeqidMismatch),
            "seqid mismatch"
        );
        assert_eq!(
            format!("{}", BTreeMapSyncError::SeqnoSkip),
            "seqno skip"
        );
    }

    #[test]
    fn test_new_random_seqid() {
        let r1: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        let r2: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new();
        // Extremely unlikely to get the same random seqid
        let s1 = r1.replica.read().seqid;
        let s2 = r2.replica.read().seqid;
        // Not a hard guarantee, but practically always true
        assert_ne!(s1, s2);
    }

    #[test]
    fn test_sequence_watch() {
        let replica: BTreeMapReplica<Foo, 1> = BTreeMapReplica::new_in_memory();
        let rx = replica.sequence.subscribe();
        replica.insert(Foo::new("a", None)).unwrap();
        assert!(rx.has_changed().unwrap());
    }
}

// TODO: consider https://github.com/boustrophedon/pgtemp for integration testing
