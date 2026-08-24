//! Replicated state machine, persistence, and snapshots.

mod codec;
mod core;
mod membership;
mod migrations;

pub(crate) use core::{
    StateMachine, StateMachineData, StoredSnapshot, migrate_legacy_membership,
    migrate_snapshot_reachability,
};

pub(crate) use codec::encode_snapshot;

#[cfg(test)]
mod tests;
