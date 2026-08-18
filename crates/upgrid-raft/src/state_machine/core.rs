use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use openraft_rt_compio::futures::{Stream, StreamExt};

use super::codec::{decode_snapshot, encode_snapshot};
use crate::database::{RaftDatabase, StateRepository};
use crate::domain::ApplicationState;
use crate::raft::{Res, TC};

pub(super) const CHECKPOINT_INTERVAL: u64 = 256;

#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMetaOf<TC>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// Data contained in the Raft state machine.
#[derive(Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogIdOf<TC>>,

    pub last_membership: StoredMembershipOf<TC>,

    /// Replicated application data.
    pub application: ApplicationState,
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

    repository: Option<StateRepository>,

    /// Applied entries since the last durable state-machine checkpoint.
    ///
    /// The Raft log remains durable for every write. On restart, OpenRaft
    /// replays committed entries newer than the checkpoint.
    uncheckpointed: Cell<u64>,
}

impl StateMachine {
    pub(crate) fn open(database: Rc<RaftDatabase>) -> io::Result<Self> {
        let repository = StateRepository::new(database);
        let (state_machine, current_snapshot, snapshot_idx) = repository.load()?;
        Ok(Self {
            state_machine: RefCell::new(state_machine),
            snapshot_idx: AtomicU64::new(snapshot_idx),
            current_snapshot: RefCell::new(current_snapshot),
            repository: Some(repository),
            uncheckpointed: Cell::new(0),
        })
    }

    /// Returns a point-in-time clone of the locally applied application state.
    ///
    /// Callers that expose Cluster API data must establish a Raft read barrier
    /// before using this snapshot.
    pub fn application_state(&self) -> ApplicationState {
        self.state_machine.borrow().application.clone()
    }

    pub fn applied_index(&self) -> Option<u64> {
        self.state_machine
            .borrow()
            .last_applied_log
            .map(|log_id| log_id.index())
    }

    pub(super) fn persist(&self) -> io::Result<()> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        let state = self.state_machine.borrow().clone();
        let snapshot = self.current_snapshot.borrow().clone();
        repository
            .replace(
                &state,
                snapshot.as_ref(),
                self.snapshot_idx.load(Ordering::Relaxed),
            )
            .map_err(io::Error::from)
    }
}

impl RaftSnapshotBuilder<TC> for Rc<StateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<TC, Self::SnapshotData>> {
        let state_machine = self.state_machine.borrow().clone();
        let data = encode_snapshot(&state_machine.application)?;
        let meta = SnapshotMetaOf::<TC> {
            last_log_id: state_machine.last_applied_log,
            last_membership: state_machine.last_membership.clone(),
        };
        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let snapshot_idx = self
            .snapshot_idx
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(|| io::Error::other("snapshot index overflow"))?;
        if let Some(repository) = &self.repository {
            repository.replace(&state_machine, Some(&snapshot), snapshot_idx)?;
        }

        self.snapshot_idx.store(snapshot_idx, Ordering::Relaxed);
        self.current_snapshot.replace(Some(snapshot));
        self.uncheckpointed.set(0);

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftStateMachine<TC> for Rc<StateMachine> {
    type SnapshotBuilder = Self;
    type SnapshotData = Cursor<Vec<u8>>;

    async fn applied_state(&mut self) -> io::Result<(Option<LogIdOf<TC>>, StoredMembershipOf<TC>)> {
        let state_machine = self.state_machine.borrow();
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<(EntryOf<TC>, Option<openraft::storage::ApplyResponder<TC>>)>>
            + Unpin
            + OptionalSend,
    {
        let mut applied = 0_u64;
        let mut membership_changed = false;

        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            applied = applied.saturating_add(1);
            let mut sm = self.state_machine.borrow_mut();
            sm.last_applied_log = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => Res::default(),
                EntryPayload::Normal(request) => {
                    let result = sm.application.apply_operation(
                        request.operation_id,
                        request.submitted_at_ms,
                        request.command,
                    );
                    Res { result }
                }
                EntryPayload::Membership(mem) => {
                    membership_changed = true;
                    sm.last_membership = StoredMembershipOf::<TC>::new(Some(entry.log_id), mem);
                    Res::default()
                }
            };
            drop(sm);
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        let uncheckpointed = self.uncheckpointed.get().saturating_add(applied);
        if membership_changed || uncheckpointed >= CHECKPOINT_INTERVAL {
            self.persist()?;
            self.uncheckpointed.set(0);
        } else {
            self.uncheckpointed.set(uncheckpointed);
        }
        Ok(())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TC>,
        snapshot: Self::SnapshotData,
    ) -> io::Result<()> {
        let mut data = snapshot.into_inner();
        let decoded = decode_snapshot(&data)?;
        if decoded.migrated {
            data = encode_snapshot(&decoded.value)?;
        }
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            application: decoded.value,
        };
        if let Some(repository) = &self.repository {
            repository.replace(
                &updated_state_machine,
                Some(&new_snapshot),
                self.snapshot_idx.load(Ordering::Relaxed),
            )?;
        }

        self.state_machine.replace(updated_state_machine);
        self.current_snapshot.replace(Some(new_snapshot));
        self.uncheckpointed.set(0);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> io::Result<Option<SnapshotOf<TC, Self::SnapshotData>>> {
        match &*self.current_snapshot.borrow() {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Cursor::new(snapshot.data.clone()),
            })),
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
