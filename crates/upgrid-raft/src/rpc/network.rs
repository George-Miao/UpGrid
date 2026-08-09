//! OpenRaft tarpc network adapter.

use std::time::{Duration, Instant};

use openraft::alias::{NodeIdOf, NodeOf};
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Timeout, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RPCTypes, RaftNetwork, RaftNetworkFactory};
use tap::Tap;
use tarpc::client::RpcError;
use tarpc::context::Context;
use tracing::debug;

use super::runtime::Rpc;
use super::service::UpgridServiceClient;
use crate::raft::{Identity, TC};
use crate::{Result, UpgridNode};

/// [`RaftNetworkFactory`] implementation for Upgrid using tarpc.
pub struct UpgridNetwork {
    id: Identity,
    rpc: Rpc,
}

impl UpgridNetwork {
    pub fn new(id: Identity, rpc: Rpc) -> Self {
        UpgridNetwork { id, rpc }
    }
}

impl RaftNetworkFactory<TC> for UpgridNetwork {
    type Network = TarpcConnector;

    async fn new_client(&mut self, target_id: NodeIdOf<TC>, target: &NodeOf<TC>) -> Self::Network {
        TarpcConnector {
            self_id: self.id.id,
            target_id,
            target: target.clone(),
            rpc: self.rpc.clone(),
        }
    }
}

/// Raft network connector implemented with tarpc
#[derive(Clone)]
pub struct TarpcConnector {
    self_id: NodeIdOf<TC>,
    target_id: NodeIdOf<TC>,
    target: UpgridNode,
    rpc: Rpc,
}

impl TarpcConnector {
    async fn client(&self) -> Result<UpgridServiceClient> {
        self.rpc.client(&self.target).await
    }

    fn context(&self, option: RPCOption) -> Context {
        Context::current().tap_mut(|c| c.deadline = Instant::now() + option.hard_ttl())
    }

    fn map_tarpc_err<E: snafu::Error>(
        &self,
        action: RPCTypes,
        error: RpcError,
    ) -> RPCError<TC, RaftError<TC, E>> {
        match error {
            error @ (RpcError::Shutdown | RpcError::Send(_) | RpcError::Channel(_)) => {
                debug!(%error, "connection error");
                self.rpc.invalidate(&self.target);
                Unreachable::new(&error).into()
            }
            RpcError::DeadlineExceeded => {
                debug!("deadline exceeded");
                Timeout {
                    action,
                    id: self.self_id,
                    target: self.target_id,
                    timeout: Duration::ZERO,
                }
                .into()
            }
            RpcError::Server(error) => {
                debug!(%error, "server error");
                self.rpc.invalidate(&self.target);
                Unreachable::new(&error).into()
            }
        }
    }
}

impl RaftNetwork<TC> for TarpcConnector {
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TC>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<TC>, RPCError<TC, RaftError<TC, InstallSnapshotError>>>
    {
        self.client()
            .await
            .map_err(|e| NetworkError::new(&e))?
            .install_snapshot(self.context(option), rpc)
            .await
            .map_err(|e| self.map_tarpc_err(RPCTypes::InstallSnapshot, e))?
            .map_err(|e| RemoteError::new_with_node(self.target_id, self.target.clone(), e).into())
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TC>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TC>, RPCError<TC, RaftError<TC>>> {
        self.client()
            .await
            .map_err(|e| NetworkError::new(&e))?
            .append_entries(self.context(option), rpc)
            .await
            .map_err(|e| self.map_tarpc_err(RPCTypes::AppendEntries, e))?
            .map_err(|e| RemoteError::new_with_node(self.target_id, self.target.clone(), e).into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TC>,
        option: RPCOption,
    ) -> Result<VoteResponse<TC>, RPCError<TC, RaftError<TC>>> {
        self.client()
            .await
            .map_err(|e| NetworkError::new(&e))?
            .vote(self.context(option), rpc)
            .await
            .map_err(|e| self.map_tarpc_err(RPCTypes::Vote, e))?
            .map_err(|e| RemoteError::new_with_node(self.target_id, self.target.clone(), e).into())
    }
}
