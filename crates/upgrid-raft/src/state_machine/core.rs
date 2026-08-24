use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use openraft_rt_compio::futures::{Stream, StreamExt};

use super::codec::{decode_snapshot, encode_snapshot};
use super::membership::validate_reachability_command;
use crate::database::{RaftDatabase, StateRepository};
use crate::domain::ApplicationState;
use crate::raft::{Res, TC};
use crate::{DirectedRoute, ReachableAddress};

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
        let (mut state_machine, mut current_snapshot, snapshot_idx) = repository.load()?;
        let state_migrated = migrate_legacy_membership(
            &mut state_machine.application,
            &mut state_machine.last_membership,
        );
        let snapshot_migrated = migrate_snapshot_reachability(&mut current_snapshot)?;
        if state_migrated || snapshot_migrated {
            repository.replace(&state_machine, current_snapshot.as_ref(), snapshot_idx)?;
        }
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

    pub(crate) fn reachable_addresses(
        &self,
        node_id: uuid::Uuid,
        observer: uuid::Uuid,
        now_ms: u64,
    ) -> Option<Vec<ReachableAddress>> {
        self.state_machine
            .borrow()
            .application
            .node_reachability
            .get(&node_id)
            .map(|reachability| reachability.ordered_reachable(observer, now_ms))
    }

    pub(crate) fn reachable_address_candidates(
        &self,
        node_id: uuid::Uuid,
        observer: uuid::Uuid,
        now_ms: u64,
    ) -> Option<Vec<ReachableAddress>> {
        self.state_machine
            .borrow()
            .application
            .node_reachability
            .get(&node_id)
            .map(|reachability| reachability.ordered_candidates(observer, now_ms))
    }

    pub(crate) fn is_configured_reachable_address(
        &self,
        node_id: uuid::Uuid,
        address: &ReachableAddress,
    ) -> bool {
        self.state_machine
            .borrow()
            .application
            .node_reachability
            .get(&node_id)
            .is_some_and(|reachability| {
                reachability
                    .configured_reachable_addresses()
                    .contains(address)
            })
    }

    pub(crate) fn is_reachable_address(
        &self,
        node_id: uuid::Uuid,
        address: &ReachableAddress,
        observer: uuid::Uuid,
        now_ms: u64,
    ) -> bool {
        self.state_machine
            .borrow()
            .application
            .node_reachability
            .get(&node_id)
            .is_some_and(|reachability| reachability.is_reachable(address, observer, now_ms))
    }

    pub(crate) fn connectivity_scan_requires_record(
        &self,
        failures: &BTreeSet<DirectedRoute>,
        now_ms: u64,
    ) -> bool {
        self.state_machine
            .borrow()
            .application
            .connectivity_scan_requires_record(failures, now_ms)
    }

    pub(crate) fn expired_join_reservations(
        &self,
        now_ms: u64,
    ) -> Vec<crate::domain::ExpiredJoinReservation> {
        self.state_machine
            .borrow()
            .application
            .expired_join_reservations(now_ms)
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
                    let result = apply_request(&mut sm, request);
                    Res { result }
                }
                EntryPayload::Membership(mem) => {
                    membership_changed = true;
                    let membership = StoredMembershipOf::<TC>::new(Some(entry.log_id), mem);
                    apply_membership(&mut sm, membership);
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
        let mut decoded = decode_snapshot(&data)?;
        let mut meta = meta.clone();
        let seeded = migrate_legacy_membership(&mut decoded.value, &mut meta.last_membership);
        if decoded.migrated || seeded {
            data = encode_snapshot(&decoded.value)?;
        }
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership,
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
fn apply_request(
    state: &mut StateMachineData,
    request: crate::raft::Req,
) -> std::result::Result<crate::domain::CommandResult, crate::domain::DomainError> {
    if let Some(result) = state
        .application
        .processed_operation_result(request.operation_id)
    {
        return result;
    }
    let command = match request.command {
        crate::domain::Command::AbortPendingReadmission {
            reservation_id,
            reservation_operation_id,
            completed_at_ms,
        } if state.last_membership.get_node(&reservation_id).is_none() => {
            crate::domain::Command::AbortPendingJoin {
                reservation_id,
                reservation_operation_id,
                completed_at_ms,
            }
        }
        command => command,
    };
    if let Err(error) = validate_reachability_command(
        &command,
        request.submitted_at_ms,
        &state.last_membership,
        &state.application,
    ) {
        return state.application.cache_operation_result(
            request.operation_id,
            request.submitted_at_ms,
            Err(error),
        );
    }
    state
        .application
        .apply_operation(request.operation_id, request.submitted_at_ms, command)
}

fn apply_membership(state: &mut StateMachineData, mut membership: StoredMembershipOf<TC>) {
    let previous_members = state
        .last_membership
        .nodes()
        .map(|(node_id, _)| *node_id)
        .collect::<BTreeSet<_>>();
    migrate_legacy_membership(&mut state.application, &mut membership);
    let members = membership
        .nodes()
        .map(|(node_id, _)| *node_id)
        .collect::<BTreeSet<_>>();
    state.application.retain_member_reachability(&members);
    if previous_members != members {
        state.application.reset_connectivity_scan();
    }
    state.last_membership = membership;
}

pub(crate) fn migrate_snapshot_reachability(
    snapshot: &mut Option<StoredSnapshot>,
) -> io::Result<bool> {
    let Some(snapshot) = snapshot else {
        return Ok(false);
    };
    let mut decoded = decode_snapshot(&snapshot.data)?;
    let migrated =
        migrate_legacy_membership(&mut decoded.value, &mut snapshot.meta.last_membership);
    if decoded.migrated || migrated {
        snapshot.data = encode_snapshot(&decoded.value)?;
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn migrate_legacy_membership(
    application: &mut ApplicationState,
    membership: &mut StoredMembershipOf<TC>,
) -> bool {
    let legacy = membership
        .nodes()
        .filter_map(|(node_id, node)| {
            node.legacy_address()
                .cloned()
                .map(|address| (*node_id, address))
        })
        .collect::<Vec<_>>();
    if legacy.is_empty() {
        return false;
    }
    for (node_id, address) in &legacy {
        application
            .node_reachability
            .entry(*node_id)
            .or_default()
            .add_configured(address.clone());
    }
    let configs = membership.get_joint_config().clone();
    let node_ids = membership
        .nodes()
        .map(|(node_id, _)| *node_id)
        .collect::<Vec<_>>();
    let identity = openraft::Membership::new_with_defaults(configs, node_ids);
    *membership = StoredMembershipOf::<TC>::new(*membership.log_id(), identity);
    true
}

#[cfg(test)]
mod tests;
