//! Concrete OpenRaft types.

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

use openraft::alias::NodeIdOf;
use openraft::declare_raft_types;
use openraft_rt_compio::CompioRuntime;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::ReachableAddress;
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

/// Identity-only OpenRaft membership metadata.
///
/// The private legacy address exists only while old persisted memberships are
/// migrated into replicated reachability. New identity values serialize an
/// unusable legacy-shaped value so they contain no endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeIdentity {
    legacy_address: Option<ReachableAddress>,
}

impl NodeIdentity {
    pub(crate) fn legacy_address(&self) -> Option<&ReachableAddress> {
        self.legacy_address.as_ref()
    }
}

impl Serialize for NodeIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (host, port) = self
            .legacy_address
            .as_ref()
            .map_or(("", 0), |address| (address.host(), address.port()));
        let mut fields = serializer.serialize_struct("ReachableAddress", 2)?;
        fields.serialize_field("host", host)?;
        fields.serialize_field("port", &port)?;
        fields.end()
    }
}

impl<'de> Deserialize<'de> for NodeIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "ReachableAddress")]
        struct LegacyAddress {
            host: String,
            port: u16,
        }

        let LegacyAddress { host, port } = LegacyAddress::deserialize(deserializer)?;
        let legacy_address = ReachableAddress::from_host_port(host, port);
        Ok(Self { legacy_address })
    }
}

impl Display for NodeIdentity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("node identity")
    }
}

declare_raft_types! {
    pub TC:
       D = Req,
       R = Res,
       Node = NodeIdentity,
       NodeId = Uuid,
       AsyncRuntime = CompioRuntime,
}

pub type Raft = openraft::raft::Raft<TC, Rc<StateMachine>>;

/// Admission claim for one durable node identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub id: NodeIdOf<TC>,
    pub bootstrap: ReachableAddress,
    pub configured: BTreeSet<ReachableAddress>,
    #[serde(default)]
    pub configured_explicit: bool,
    #[serde(default)]
    pub candidates: Vec<crate::ReachableAddressCandidate>,
}

impl NodeRegistration {
    #[cfg(test)]
    pub fn new(url: impl AsRef<str>) -> crate::Result<Self> {
        Self::with_id(Uuid::now_v7(), url)
    }

    pub fn with_id(id: Uuid, url: impl AsRef<str>) -> crate::Result<Self> {
        let bootstrap = ReachableAddress::parse(url.as_ref())?;
        Ok(Self {
            id,
            configured: BTreeSet::from([bootstrap.clone()]),
            bootstrap,
            configured_explicit: true,
            candidates: Vec::new(),
        })
    }

    pub fn from_network(
        id: Uuid,
        bootstrap: ReachableAddress,
        configured: BTreeSet<ReachableAddress>,
        configured_explicit: bool,
        candidates: Vec<crate::ReachableAddressCandidate>,
    ) -> Self {
        Self {
            id,
            bootstrap,
            configured,
            configured_explicit,
            candidates,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::NodeIdentity;

    #[derive(Serialize)]
    #[serde(rename = "ReachableAddress")]
    struct LegacyAddress<'a> {
        host: &'a str,
        port: u16,
    }

    #[test]
    fn legacy_membership_address_decodes_for_migration_only() {
        let legacy = LegacyAddress {
            host: "legacy.example",
            port: 11451,
        };
        let encoded = postcard::to_stdvec(&legacy).unwrap();
        let identity = postcard::from_bytes::<NodeIdentity>(&encoded).unwrap();

        assert_eq!(
            identity.legacy_address().map(ToString::to_string),
            Some("up://legacy.example:11451".to_owned())
        );
        assert_eq!(
            postcard::to_stdvec(&identity).unwrap(),
            postcard::to_stdvec(&legacy).unwrap()
        );
    }

    #[test]
    fn new_membership_json_has_no_usable_endpoint() {
        assert_eq!(
            serde_json::to_value(NodeIdentity::default()).unwrap(),
            serde_json::json!({ "host": "", "port": 0 })
        );
    }
}
