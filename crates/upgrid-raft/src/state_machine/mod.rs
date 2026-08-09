//! Replicated state machine, persistence, and snapshots.

mod core;
mod version;

pub(crate) use core::StateMachine;

#[cfg(test)]
#[path = "tests.rs"]
mod state_machine_tests;
