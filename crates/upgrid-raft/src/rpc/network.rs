//! OpenRaft tarpc network adapter.

use std::future::Future;
use std::io::Cursor;
use std::time::{Duration, Instant};

use compio::time::timeout;
use openraft::alias::{NodeIdOf, NodeOf, SnapshotOf, VoteOf};
use openraft::error::{
    NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable,
};
use openraft::network::RPCOption;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RPCTypes, RaftNetworkFactory};
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

    fn context(&self, option: &RPCOption) -> Context {
        Context::current().tap_mut(|c| c.deadline = Instant::now() + option.soft_ttl())
    }

    fn map_tarpc_err(&self, action: RPCTypes, timeout: Duration, error: RpcError) -> RPCError<TC> {
        match error {
            error @ (RpcError::Shutdown | RpcError::Send(_) | RpcError::Channel(_)) => {
                debug!(%error, "connection error");
                self.rpc.invalidate(&self.target);
                Unreachable::new(&error).into()
            }
            RpcError::DeadlineExceeded => {
                debug!("deadline exceeded");
                self.rpc.invalidate(&self.target);
                Timeout {
                    action,
                    id: self.self_id,
                    target: self.target_id,
                    timeout,
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

    fn timeout_error(&self, action: RPCTypes, duration: Duration) -> RPCError<TC> {
        self.rpc.invalidate(&self.target);
        Timeout {
            action,
            id: self.self_id,
            target: self.target_id,
            timeout: duration,
        }
        .into()
    }

    fn map_remote_err(&self, error: impl std::error::Error + 'static) -> RPCError<TC> {
        debug!(%error, "remote Raft error");
        self.rpc.invalidate(&self.target);
        Unreachable::new(&error).into()
    }
}

impl RaftNetworkV2<TC> for TarpcConnector {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TC>,
        snapshot: SnapshotOf<TC, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TC>, StreamingError<TC>> {
        let duration = option.soft_ttl();
        let response = timeout(duration, async {
            self.client()
                .await
                .map_err(|error| NetworkError::new(&error))?
                .full_snapshot(
                    self.context(&option),
                    vote,
                    snapshot.meta,
                    snapshot.snapshot.into_inner(),
                )
                .await
                .map_err(|error| self.map_tarpc_err(RPCTypes::InstallSnapshot, duration, error))
        })
        .await
        .map_err(|_| self.timeout_error(RPCTypes::InstallSnapshot, duration))??;
        response.map_err(|error| self.map_remote_err(error).into())
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TC>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TC>, RPCError<TC>> {
        let duration = option.soft_ttl();
        let response = timeout(duration, async {
            self.client()
                .await
                .map_err(|error| NetworkError::new(&error))?
                .append_entries(self.context(&option), rpc)
                .await
                .map_err(|error| self.map_tarpc_err(RPCTypes::AppendEntries, duration, error))
        })
        .await
        .map_err(|_| self.timeout_error(RPCTypes::AppendEntries, duration))??;
        response.map_err(|error| self.map_remote_err(error))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TC>,
        option: RPCOption,
    ) -> Result<VoteResponse<TC>, RPCError<TC>> {
        let duration = option.soft_ttl();
        let response = timeout(duration, async {
            self.client()
                .await
                .map_err(|error| NetworkError::new(&error))?
                .vote(self.context(&option), rpc)
                .await
                .map_err(|error| self.map_tarpc_err(RPCTypes::Vote, duration, error))
        })
        .await
        .map_err(|_| self.timeout_error(RPCTypes::Vote, duration))??;
        response.map_err(|error| self.map_remote_err(error))
    }
}
