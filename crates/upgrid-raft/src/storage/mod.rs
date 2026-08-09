//! In-memory and persistent Raft storage adapters.

mod core;

pub use core::InMemStore;

#[cfg(test)]
#[path = "tests.rs"]
mod storage_tests;
