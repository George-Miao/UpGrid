//! In-memory and persistent Raft storage adapters.

mod core;

pub use core::InMemStore;
pub(crate) use core::InMemStoreInner;

#[cfg(test)]
mod tests;
