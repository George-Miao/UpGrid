use openraft::{
    alias::{NodeIdOf, NodeOf},
    declare_raft_types,
};
use openraft_rt_compio::CompioRuntime;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use url::Url;
use uuid::Uuid;

use crate::{UrlParseSnafu, network::UpgridNode, utils::uuid_v7_now};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Req {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Res {}

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
    pub fn new<U, E>(url: U) -> crate::Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + 'static,
    {
        let url = url
            .try_into()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
            .context(UrlParseSnafu)?;
        Ok(Self {
            id: uuid_v7_now(),
            node: UpgridNode::new(url)?,
        })
    }
}
