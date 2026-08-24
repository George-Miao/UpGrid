//! OpenRaft RPC network adapter.

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
use tracing::debug;
use upgrid_rpc::{CallError, Context};

use super::runtime::Rpc;
use super::service::UpgridServiceClient;
use crate::Result;
use crate::raft::TC;

/// [`RaftNetworkFactory`] implementation for UpGrid RPC.
pub struct UpgridNetwork {
    self_id: NodeIdOf<TC>,
    rpc: Rpc,
}

impl UpgridNetwork {
    pub fn new(self_id: NodeIdOf<TC>, rpc: Rpc) -> Self {
        Self { self_id, rpc }
    }
}

impl RaftNetworkFactory<TC> for UpgridNetwork {
    type Network = RpcConnector;

    async fn new_client(&mut self, target_id: NodeIdOf<TC>, _: &NodeOf<TC>) -> Self::Network {
        RpcConnector {
            self_id: self.self_id,
            target_id,
            rpc: self.rpc.clone(),
        }
    }
}

/// Raft network connector implemented with UpGrid RPC.
#[derive(Clone)]
pub struct RpcConnector {
    self_id: NodeIdOf<TC>,
    target_id: NodeIdOf<TC>,
    rpc: Rpc,
}

impl RpcConnector {
    async fn client(&self) -> Result<UpgridServiceClient> {
        self.rpc.client_to(self.target_id).await
    }

    fn context(&self, option: &RPCOption) -> Context {
        Context::with_deadline(Instant::now() + option.soft_ttl())
    }

    fn map_call_error(
        &self,
        action: RPCTypes,
        timeout: Duration,
        error: CallError,
    ) -> RPCError<TC> {
        if matches!(error, CallError::DeadlineExceeded) {
            debug!("deadline exceeded");
            self.rpc.invalidate_node(self.target_id);
            return Timeout {
                action,
                id: self.self_id,
                target: self.target_id,
                timeout,
            }
            .into();
        }

        debug!(%error, "connection error");
        self.rpc.invalidate_node(self.target_id);
        Unreachable::new(&error).into()
    }

    fn timeout_error(&self, action: RPCTypes, duration: Duration) -> RPCError<TC> {
        self.rpc.invalidate_node(self.target_id);
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
        self.rpc.invalidate_node(self.target_id);
        Unreachable::new(&error).into()
    }
}

impl RaftNetworkV2<TC> for RpcConnector {
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
                .map_err(|error| self.map_call_error(RPCTypes::InstallSnapshot, duration, error))
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
                .map_err(|error| self.map_call_error(RPCTypes::AppendEntries, duration, error))
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
                .map_err(|error| self.map_call_error(RPCTypes::Vote, duration, error))
        })
        .await
        .map_err(|_| self.timeout_error(RPCTypes::Vote, duration))??;
        response.map_err(|error| self.map_remote_err(error))
    }
}
