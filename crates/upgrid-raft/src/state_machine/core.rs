use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, io};

use openraft::alias::SnapshotDataOf;
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use upgrid_config::durable;

use crate::domain::{
    ApplicationState, LegacyApplicationState, NamedApplicationState, PreviousApplicationState,
    TokenApplicationState,
};
use crate::raft::{Res, TC};

pub(super) const STATE_MAGIC: &[u8] = b"UPGS4";
pub(super) const NAMED_STATE_MAGIC: &[u8] = b"UPGS3";
pub(super) const TOKEN_STATE_MAGIC: &[u8] = b"UPGS2";
pub(super) const PREVIOUS_STATE_MAGIC: &[u8] = b"UPGS1";
pub(super) const SNAPSHOT_MAGIC: &[u8] = b"UPGA4";
pub(super) const NAMED_SNAPSHOT_MAGIC: &[u8] = b"UPGA3";
pub(super) const TOKEN_SNAPSHOT_MAGIC: &[u8] = b"UPGA2";
pub(super) const PREVIOUS_SNAPSHOT_MAGIC: &[u8] = b"UPGA1";
pub(super) const CHECKPOINT_INTERVAL: u64 = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<TC>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// Data contained in the Raft state machine.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<TC>>,

    pub last_membership: StoredMembership<TC>,

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

    path: Option<PathBuf>,

    /// Applied entries since the last durable state-machine checkpoint.
    ///
    /// The Raft log remains durable for every write. On restart, OpenRaft
    /// replays committed entries newer than the checkpoint.
    uncheckpointed: Cell<u64>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct PersistedStateMachine {
    pub(super) state_machine: StateMachineData,
    pub(super) current_snapshot: Option<StoredSnapshot>,
    pub(super) snapshot_idx: u64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct NamedStateMachineData {
    pub(super) last_applied_log: Option<LogId<TC>>,
    pub(super) last_membership: StoredMembership<TC>,
    pub(super) application: NamedApplicationState,
}

#[derive(Serialize, Deserialize)]
pub(super) struct NamedPersistedStateMachine {
    pub(super) state_machine: NamedStateMachineData,
    pub(super) current_snapshot: Option<StoredSnapshot>,
    pub(super) snapshot_idx: u64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct LegacyStateMachineData {
    pub(super) last_applied_log: Option<LogId<TC>>,
    pub(super) last_membership: StoredMembership<TC>,
    pub(super) application: LegacyApplicationState,
}

#[derive(Serialize, Deserialize)]
pub(super) struct LegacyPersistedStateMachine {
    pub(super) state_machine: LegacyStateMachineData,
    pub(super) current_snapshot: Option<StoredSnapshot>,
    pub(super) snapshot_idx: u64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct PreviousStateMachineData {
    pub(super) last_applied_log: Option<LogId<TC>>,
    pub(super) last_membership: StoredMembership<TC>,
    pub(super) application: PreviousApplicationState,
}

#[derive(Serialize, Deserialize)]
pub(super) struct PreviousPersistedStateMachine {
    pub(super) state_machine: PreviousStateMachineData,
    pub(super) current_snapshot: Option<StoredSnapshot>,
    pub(super) snapshot_idx: u64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct TokenStateMachineData {
    pub(super) last_applied_log: Option<LogId<TC>>,
    pub(super) last_membership: StoredMembership<TC>,
    pub(super) application: TokenApplicationState,
}

#[derive(Serialize, Deserialize)]
pub(super) struct TokenPersistedStateMachine {
    pub(super) state_machine: TokenStateMachineData,
    pub(super) current_snapshot: Option<StoredSnapshot>,
    pub(super) snapshot_idx: u64,
}

fn decode_persisted(bytes: &[u8]) -> io::Result<PersistedStateMachine> {
    if let Some(bytes) = bytes.strip_prefix(STATE_MAGIC) {
        return postcard::from_bytes(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()));
    }
    if let Some(bytes) = bytes.strip_prefix(NAMED_STATE_MAGIC) {
        let previous = postcard::from_bytes::<NamedPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(TOKEN_STATE_MAGIC) {
        let previous = postcard::from_bytes::<TokenPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PREVIOUS_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreviousPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    let legacy = postcard::from_bytes::<LegacyPersistedStateMachine>(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    Ok(PersistedStateMachine {
        state_machine: StateMachineData {
            last_applied_log: legacy.state_machine.last_applied_log,
            last_membership: legacy.state_machine.last_membership,
            application: legacy.state_machine.application.into(),
        },
        current_snapshot: legacy.current_snapshot,
        snapshot_idx: legacy.snapshot_idx,
    })
}

pub(super) fn decode_application(bytes: &[u8]) -> Result<ApplicationState, postcard::Error> {
    if let Some(bytes) = bytes.strip_prefix(SNAPSHOT_MAGIC) {
        postcard::from_bytes(bytes)
    } else if let Some(bytes) = bytes.strip_prefix(NAMED_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<NamedApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(TOKEN_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<TokenApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PREVIOUS_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreviousApplicationState>(bytes).map(Into::into)
    } else {
        postcard::from_bytes::<LegacyApplicationState>(bytes).map(Into::into)
    }
}

impl StateMachine {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let persisted = match fs::read(&path) {
            Ok(bytes) => decode_persisted(&bytes)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => PersistedStateMachine {
                state_machine: StateMachineData::default(),
                current_snapshot: None,
                snapshot_idx: 0,
            },
            Err(error) => return Err(error),
        };
        Ok(Self {
            state_machine: RefCell::new(persisted.state_machine),
            snapshot_idx: AtomicU64::new(persisted.snapshot_idx),
            current_snapshot: RefCell::new(persisted.current_snapshot),
            path: Some(path),
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
        let Some(path) = &self.path else {
            return Ok(());
        };
        let persisted = PersistedStateMachine {
            state_machine: self.state_machine.borrow().clone(),
            current_snapshot: self.current_snapshot.borrow().clone(),
            snapshot_idx: self.snapshot_idx.load(Ordering::Relaxed),
        };
        let bytes =
            postcard::to_stdvec(&persisted).map_err(|error| io::Error::other(error.to_string()))?;
        let mut encoded = Vec::with_capacity(STATE_MAGIC.len() + bytes.len());
        encoded.extend_from_slice(STATE_MAGIC);
        encoded.extend_from_slice(&bytes);
        durable::replace(path, &encoded)
    }
}

impl RaftSnapshotBuilder<TC> for Rc<StateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TC>, StorageError<TC>> {
        let state_machine = self.state_machine.borrow();
        let encoded = postcard::to_stdvec(&state_machine.application)
            .map_err(|e| StorageError::read_state_machine(&e))?;
        let mut data = Vec::with_capacity(SNAPSHOT_MAGIC.len() + encoded.len());
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.extend_from_slice(&encoded);

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
        self.persist()
            .map_err(|error| StorageError::write_snapshot(Some(meta.signature()), &error))?;
        self.uncheckpointed.set(0);

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
        let mut applied = 0_u64;
        let mut membership_changed = false;

        let mut sm = self.state_machine.borrow_mut();

        for entry in entries {
            applied = applied.saturating_add(1);
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Res::default()),
                EntryPayload::Normal(request) => {
                    let result = sm.application.apply_operation(
                        request.operation_id,
                        request.submitted_at_ms,
                        request.command,
                    );
                    res.push(Res { result });
                }
                EntryPayload::Membership(ref mem) => {
                    membership_changed = true;
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Res::default())
                }
            };
        }
        drop(sm);
        let uncheckpointed = self.uncheckpointed.get().saturating_add(applied);
        if membership_changed || uncheckpointed >= CHECKPOINT_INTERVAL {
            self.persist()
                .map_err(|error| StorageError::write_state_machine(&error))?;
            self.uncheckpointed.set(0);
        } else {
            self.uncheckpointed.set(uncheckpointed);
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
        let application = decode_application(&new_snapshot.data)
            .map_err(|e| StorageError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            application,
        };
        let mut state_machine = self.state_machine.borrow_mut();
        *state_machine = updated_state_machine;

        // Lock the current snapshot before releasing the lock on the state machine, to
        // avoid a race condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.borrow_mut();
        drop(state_machine);

        // Update current snapshot.
        *current_snapshot = Some(new_snapshot);
        drop(current_snapshot);
        self.persist()
            .map_err(|error| StorageError::write_snapshot(Some(meta.signature()), &error))?;
        self.uncheckpointed.set(0);
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
