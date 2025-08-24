mod controller;
mod rpc;
mod transport;

use std::{
    fmt::{Display, Formatter},
    time::{Duration, Instant},
};

pub use controller::Controller;
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
pub use rpc::UpgridServer;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tap::Tap;
use tarpc::{client::RpcError, context::Context};
use tracing::debug;
pub use transport::bi_stream_framed;
use url::Url;

use crate::{
    Result, UrlInvalidHostSnafu, UrlParseSnafu,
    network::rpc::UpgridServiceClient,
    raft::{Identity, TC},
};

const DEFAULT_UP_PORT: u16 = 11451;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgridNode {
    host: String,
    port: u16,
    scheme: Scheme,
}

impl Display for UpgridNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.host, self.port)
    }
}

impl UpgridNode {
    pub fn new<U, E>(url: U) -> crate::Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + 'static,
    {
        let url = url
            .try_into()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
            .context(UrlParseSnafu)?;
        let scheme = match url.scheme() {
            "up" => Scheme::Up,
            _ => return Err(crate::Error::UrlInvalidScheme { url }),
        };
        let host = url
            .host_str()
            .with_context(|| UrlInvalidHostSnafu { url: url.clone() })?
            .to_string();
        let port = url.port().unwrap_or(DEFAULT_UP_PORT);

        Ok(Self { host, port, scheme })
    }

    pub fn is_up(&self) -> bool {
        self.scheme == Scheme::Up
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Scheme {
    Up,
}

impl Display for Scheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scheme::Up => write!(f, "up"),
        }
    }
}

/// [`RaftNetworkFactory`] implementation for Upgrid using tarpc.
pub struct UpgridNetwork {
    id: Identity,
    controller: Controller,
}

impl UpgridNetwork {
    pub fn new(id: Identity, controller: Controller) -> Self {
        UpgridNetwork { id, controller }
    }
}

impl RaftNetworkFactory<TC> for UpgridNetwork {
    type Network = TarpcConnector;

    async fn new_client(&mut self, target_id: NodeIdOf<TC>, target: &NodeOf<TC>) -> Self::Network {
        TarpcConnector {
            self_id: self.id.id,
            target_id,
            target: target.clone(),
            controller: self.controller.clone(),
        }
    }
}

/// Raft network connector implemented with tarpc
#[derive(Clone)]
pub struct TarpcConnector {
    self_id: NodeIdOf<TC>,
    target_id: NodeIdOf<TC>,
    target: UpgridNode,
    controller: Controller,
}

impl TarpcConnector {
    async fn client(&self) -> Result<UpgridServiceClient> {
        self.controller.get_client(&self.target).await
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
