//! Replicated state machine, persistence, and snapshots.

mod core;
mod decode;
mod version;

pub(crate) use core::StateMachine;

#[cfg(test)]
#[path = "tests.rs"]
mod state_machine_tests;
