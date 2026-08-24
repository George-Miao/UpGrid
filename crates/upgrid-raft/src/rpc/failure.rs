use serde::{Deserialize, Serialize};

use crate::ReachableAddress;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionPhase {
    Leadership,
    LinearizableRead,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionFailure {
    Transport(upgrid_transport::Failure),
    Rpc(upgrid_rpc::CallFailure),
    NoReachableAddress {
        node_id: uuid::Uuid,
    },
    NodeIdentityMismatch {
        address: ReachableAddress,
        expected_node_id: uuid::Uuid,
        actual_node_id: uuid::Uuid,
    },
    ConnectTimeout {
        node: ReachableAddress,
    },
    Deadline {
        phase: ConnectionPhase,
    },
    DeploymentKeyMismatch,
    InvalidAddress {
        diagnostic: String,
    },
    Internal {
        diagnostic: String,
    },
}

impl From<upgrid_rpc::CallError> for ConnectionFailure {
    fn from(error: upgrid_rpc::CallError) -> Self {
        Self::Rpc(error.into())
    }
}

impl From<crate::Error> for ConnectionFailure {
    fn from(error: crate::Error) -> Self {
        match error {
            crate::Error::Transport { source } => Self::Transport(source.into()),
            crate::Error::RpcError { source } => Self::Rpc(source.into()),
            crate::Error::NoReachableAddress { node_id } => Self::NoReachableAddress { node_id },
            crate::Error::NodeIdentityMismatch {
                address,
                expected_node_id,
                actual_node_id,
            } => Self::NodeIdentityMismatch {
                address,
                expected_node_id,
                actual_node_id,
            },
            crate::Error::ForwardConnectTimeout { node } => Self::ConnectTimeout { node },
            crate::Error::LeadershipDeadline => Self::Deadline {
                phase: ConnectionPhase::Leadership,
            },
            crate::Error::LinearizableReadDeadline => Self::Deadline {
                phase: ConnectionPhase::LinearizableRead,
            },
            crate::Error::DeploymentKeyMismatch => Self::DeploymentKeyMismatch,
            crate::Error::ReachableAddress { source } => Self::InvalidAddress {
                diagnostic: source.to_string(),
            },
            error => Self::Internal {
                diagnostic: error.to_string(),
            },
        }
    }
}

impl std::fmt::Display for ConnectionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::Rpc(error) => error.fmt(formatter),
            Self::NoReachableAddress { node_id } => write!(
                formatter,
                "node {node_id} has no configured or verified reachable address"
            ),
            Self::NodeIdentityMismatch {
                address,
                expected_node_id,
                actual_node_id,
            } => write!(
                formatter,
                "address {address} belongs to node {actual_node_id}, expected node \
                 {expected_node_id}"
            ),
            Self::ConnectTimeout { node } => {
                write!(formatter, "timed out connecting to node {node}")
            }
            Self::Deadline {
                phase: ConnectionPhase::Leadership,
            } => formatter.write_str("leadership could not be established before the deadline"),
            Self::Deadline {
                phase: ConnectionPhase::LinearizableRead,
            } => formatter.write_str("linearizable read was unavailable before the deadline"),
            Self::DeploymentKeyMismatch => {
                formatter.write_str("deployment key does not match the cluster")
            }
            Self::InvalidAddress { diagnostic } => {
                write!(formatter, "node address is invalid: {diagnostic}")
            }
            Self::Internal { diagnostic } => formatter.write_str(diagnostic),
        }
    }
}

impl std::error::Error for ConnectionFailure {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_failure_keeps_resolution_context_on_the_wire() {
        let failure = ConnectionFailure::from(crate::Error::Transport {
            source: upgrid_transport::Error::ResolveEmpty {
                host: "node.example".to_owned(),
            },
        });

        assert!(matches!(
            &failure,
            ConnectionFailure::Transport(upgrid_transport::Failure::ResolveEmpty { host })
                if host == "node.example"
        ));
        let encoded = postcard::to_stdvec(&failure).unwrap();
        assert_eq!(
            postcard::from_bytes::<ConnectionFailure>(&encoded).unwrap(),
            failure
        );
    }
}
