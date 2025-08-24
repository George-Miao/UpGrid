use std::{
    cell::RefCell,
    collections::BTreeMap,
    fmt::Debug,
    io::Cursor,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership,
    alias::SnapshotDataOf,
    storage::{RaftStateMachine, Snapshot},
};
use serde::{Deserialize, Serialize};

use crate::raft::{Res, TC};

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<TC>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// Data contained in the Raft state machine.
///
/// Note that we are using `serde` to serialize the
/// `data`, which has a implementation to be serialized. Note that for this test
/// we set both the key and value as String, but you could set any type of value
/// that has the serialization impl.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<TC>>,

    pub last_membership: StoredMembership<TC>,

    /// Application data.
    pub data: BTreeMap<String, String>,
}

/// Defines a state machine for the Raft cluster. This state machine represents
/// a copy of the data for this node. Additionally, it is responsible for
/// storing the last snapshot of the data.
#[derive(Debug, Default)]
pub struct StateMachine {
    /// The Raft state machine.
    pub state_machine: RefCell<StateMachineData>,

    /// Used in identifier for snapshot.
    ///
    /// Note that concurrently created snapshots and snapshots created on
    /// different nodes are not guaranteed to have sequential `snapshot_idx`
    /// values, but this does not matter for correctness.
    snapshot_idx: AtomicU64,

    /// The last received snapshot.
    current_snapshot: RefCell<Option<StoredSnapshot>>,
}

impl RaftSnapshotBuilder<TC> for Rc<StateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TC>, StorageError<TC>> {
        let state_machine = self.state_machine.borrow();
        let data = postcard::to_stdvec(&state_machine.data)
            .map_err(|e| StorageError::read_state_machine(&e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!(
                "{}-{}-{}",
                last.committed_leader_id(),
                last.index(),
                snapshot_idx
            )
        } else {
            format!("--{snapshot_idx}",)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.current_snapshot.replace(Some(snapshot));

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftStateMachine<TC> for Rc<StateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TC>>, StoredMembership<TC>), StorageError<TC>> {
        let state_machine = self.state_machine.borrow();
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Res>, StorageError<TC>>
    where
        I: IntoIterator<Item = Entry<TC>>,
    {
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator

        let mut sm = self.state_machine.borrow_mut();

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Res {}),
                EntryPayload::Normal(_) => {}
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Res {})
                }
            };
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotDataOf<TC>, StorageError<TC>> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TC>,
        snapshot: SnapshotDataOf<TC>,
    ) -> Result<(), StorageError<TC>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        let updated_state_machine_data = postcard::from_bytes(&new_snapshot.data)
            .map_err(|e| StorageError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data: updated_state_machine_data,
        };
        let mut state_machine = self.state_machine.borrow_mut();
        *state_machine = updated_state_machine;

        // Lock the current snapshot before releasing the lock on the state machine, to
        // avoid a race condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.borrow_mut();
        drop(state_machine);

        // Update current snapshot.
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TC>>, StorageError<TC>> {
        match &*self.current_snapshot.borrow_mut() {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Cursor::new(data),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
