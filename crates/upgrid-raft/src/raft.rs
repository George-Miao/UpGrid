//! Concrete OpenRaft types.

use std::fmt::{Display, Formatter};
use std::rc::Rc;

use openraft::alias::{NodeIdOf, NodeOf};
use openraft::declare_raft_types;
use openraft_rt_compio::CompioRuntime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::UpgridNode;
use crate::domain::{Command, CommandResult, DomainError};
use crate::state_machine::StateMachine;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Req {
    pub(crate) operation_id: Uuid,
    pub(crate) submitted_at_ms: u64,
    pub(crate) command: Command,
}

impl Req {
    pub(crate) fn new(command: Command) -> Self {
        Self {
            operation_id: Uuid::now_v7(),
            submitted_at_ms: upgrid_config::now_ms(),
            command,
        }
    }
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

pub type Raft = openraft::raft::Raft<TC, Rc<StateMachine>>;

/// Identity of a node in the Raft cluster. Contains node ID and its public url.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub id: NodeIdOf<TC>,
    pub node: NodeOf<TC>,
}

impl Identity {
    #[cfg(test)]
    pub fn new(url: impl AsRef<str>) -> crate::Result<Self> {
        Self::with_id(Uuid::now_v7(), url)
    }

    pub fn with_id(id: Uuid, url: impl AsRef<str>) -> crate::Result<Self> {
        Ok(Self {
            id,
            node: UpgridNode::parse(url.as_ref())?,
        })
    }
}
