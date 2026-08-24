//! Replicated UpGrid state, Raft runtime, RPC adapter, and storage.

#[doc(hidden)]
pub mod benchmark;
mod cluster;
mod database;
pub mod domain;
mod error;
mod node;
mod raft;
pub mod reachability;
mod rpc;
mod state_machine;
pub mod storage;
mod test;
mod token;

pub const REACHABILITY_LEASE_MS: u64 = 30_000;

pub use cluster::{ClusterError, Handle, MembershipError, Receiver, Status};
pub use error::{Error, Result};
pub use node::{Node, NodeNetworkConfig};
pub use raft::{NodeIdentity, NodeRegistration, Req, Res, TC};
pub use reachability::{
    DirectedRoute, DiscoverySource, NodeReachability, ReachableAddressCandidate,
    ReachableAddressLease,
};
pub use rpc::{ConnectionFailure, ConnectionPhase};
pub use token::hash_join_token;
pub use upgrid_config::ReachableAddress;
