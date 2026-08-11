//! Replicated UpGrid state, Raft runtime, RPC adapter, and storage.

mod cluster;
pub mod domain;
mod error;
mod node;
mod peer;
mod raft;
mod rpc;
mod state_machine;
pub mod storage;
mod test;
mod token;

pub use cluster::{ClusterError, Handle, Receiver, Status};
pub use error::{Error, Result};
pub use node::Node;
pub use peer::UpgridNode;
// OpenRaft's public type configuration requires its data type to remain reachable.
#[doc(hidden)]
pub use raft::Req;
pub use raft::{Identity, Res, TC};
pub use token::hash_join_token;
