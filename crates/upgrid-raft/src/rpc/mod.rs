//! Raft-specific RPC service and OpenRaft adapter.

mod failure;
mod network;
mod runtime;
mod service;

pub use failure::{ConnectionFailure, ConnectionPhase};
pub(crate) use network::UpgridNetwork;
pub(crate) use runtime::{CandidateDiscovery, ConnectivityReport, Rpc};
pub(crate) use service::{JoinError, remove_member};
