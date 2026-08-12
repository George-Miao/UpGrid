//! Replicated state machine, persistence, and snapshots.

mod codec;
mod core;
mod migrations;

pub(crate) use core::{StateMachine, StateMachineData, StoredSnapshot};

pub(crate) use codec::encode_snapshot;

#[cfg(test)]
mod tests;
