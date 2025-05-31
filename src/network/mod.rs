mod rpc;
mod transport;

use std::time::{Duration, Instant};

use openraft::{
    RPCTypes, RaftNetwork, RaftNetworkFactory,
    alias::{NodeIdOf, NodeOf},
    error::{
        InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Timeout, Unreachable,
    },
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use openraft_rt_compio::futures::io;
pub use rpc::UpgridServer;
use serde::{Deserialize, Serialize};
use tap::Tap;
use tarpc::{client::RpcError, context::Context};
use tracing::debug;
use url::Url;

use crate::{
    TC,
    network::rpc::{ClientPool, UpgridServiceClient},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UrlNode {
    url: Url,
    scheme: Scheme,
}

impl UrlNode {
    pub fn new(url: Url) -> crate::Result<Self> {
        let scheme = match url.scheme() {
            "up" => Scheme::Up,
            "ups" => Scheme::Ups,
            _ => return Err(crate::Error::UrlInvalidScheme { url }),
        };

        Ok(Self { url, scheme })
    }

    pub fn is_up(&self) -> bool {
        self.scheme == Scheme::Up
    }

    pub fn is_ups(&self) -> bool {
        self.scheme == Scheme::Ups
    }

    pub fn url(&self) -> &Url {
        &self.url
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Scheme {
    Up,
    Ups,
}

/// [`RaftNetworkFactory`] implementation for Upgrid using tarpc.
pub struct UpgridNetwork {
    self_id: NodeIdOf<TC>,
    pool: ClientPool,
}

impl UpgridNetwork {
    pub fn new(self_id: NodeIdOf<TC>) -> Self {
        UpgridNetwork {
            self_id,
            pool: Default::default(),
        }
    }
}

impl RaftNetworkFactory<TC> for UpgridNetwork {
    type Network = TarpcConnector;

    async fn new_client(&mut self, target_id: NodeIdOf<TC>, target: &NodeOf<TC>) -> Self::Network {
        TarpcConnector {
            self_id: self.self_id,
            target_id,
            target: target.clone(),
            pool: self.pool.clone(),
        }
    }
}

#[derive(Clone)]
pub struct TarpcConnector {
    self_id: NodeIdOf<TC>,
    target_id: NodeIdOf<TC>,
    target: UrlNode,
    pool: ClientPool,
}

impl TarpcConnector {
    async fn client(&self) -> io::Result<UpgridServiceClient> {
        self.pool.get_client(self.target_id, &self.target.url).await
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
                self.pool.drop_client(&self.target_id);
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
