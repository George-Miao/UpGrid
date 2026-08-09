//! Concrete OpenRaft types.

use std::fmt::{Display, Formatter};

use openraft::alias::{NodeIdOf, NodeOf};
use openraft::declare_raft_types;
use openraft_rt_compio::CompioRuntime;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::UpgridNode;
use crate::domain::{Command, CommandResult, DomainError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Req {
    pub operation_id: Uuid,
    pub submitted_at_ms: u64,
    pub command: Command,
}

impl Display for Req {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "operation {}", self.operation_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Res {
    pub result: Result<CommandResult, DomainError>,
}

impl Default for Res {
    fn default() -> Self {
        Self {
            result: Ok(CommandResult::Noop),
        }
    }
}

declare_raft_types! {
    pub TC:
       D = Req,
       R = Res,
       Node = UpgridNode,
       NodeId = Uuid,
       AsyncRuntime = CompioRuntime,
}

pub type Raft = openraft::raft::Raft<TC>;

/// Identity of a node in the Raft cluster. Contains node ID and its public url.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub id: NodeIdOf<TC>,
    pub node: NodeOf<TC>,
}

impl Identity {
    #[cfg(test)]
    pub fn new<U, E>(url: U) -> crate::Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::with_id(Uuid::now_v7(), url)
    }

    pub fn with_id<U, E>(id: Uuid, url: U) -> crate::Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Ok(Self {
            id,
            node: UpgridNode::new(url)?,
        })
    }
}
