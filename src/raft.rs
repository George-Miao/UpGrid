use openraft::declare_raft_types;
use openraft_rt_compio::CompioRuntime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::network::AddrNode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Req {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Res {}

declare_raft_types! {
    pub TC:
       D = Req,
       R = Res,
       Node = AddrNode,
       NodeId = Uuid,
       AsyncRuntime = CompioRuntime,
}

pub type Raft = openraft::raft::Raft<TC>;
