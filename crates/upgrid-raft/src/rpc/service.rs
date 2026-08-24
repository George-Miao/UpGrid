//! Typed Raft and membership RPC interface.

mod admission;

use std::collections::BTreeSet;
use std::io::Cursor;
use std::rc::Rc;
use std::time::Instant;

use compio::time::sleep;
use openraft::alias::{LogIdOf, SnapshotMetaOf, VoteOf};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ChangeMembershipError, ClientWriteError, LinearizableReadError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, SnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{ChangeMembers, ReadPolicy};
use openraft_rt_compio::futures::lock::Mutex;
use serde::{Deserialize, Serialize};
use upgrid_rpc::Context;
use upgrid_transport::{PeerAddress, PeerIdentity};

use super::runtime::Rpc;
use crate::domain::{Command, DomainError};
use crate::raft::{NodeRegistration, Raft, Req, TC};
use crate::{ConnectionFailure, DirectedRoute, ReachableAddress, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinError {
    Raft(RaftError<TC, ClientWriteError<TC>>),
    Rejected(DomainError),
    Connectivity(Vec<DirectedRoute>),
    Connection(ConnectionFailure),
    Redirect {
        node_id: uuid::Uuid,
        addresses: Vec<ReachableAddress>,
    },
    NodeIdentity {
        node_id: uuid::Uuid,
    },
    Membership(crate::MembershipError),
    RuntimeStopped,
    Deadline,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raft(error) => error.fmt(formatter),
            Self::Rejected(error) => error.fmt(formatter),
            Self::Connection(error) => error.fmt(formatter),
            Self::Redirect { node_id, addresses } => write!(
                formatter,
                "join through leader node {node_id} using {} route(s)",
                addresses.len()
            ),
            Self::NodeIdentity { node_id } => write!(
                formatter,
                "authenticated peer does not match joining node {node_id}",
            ),
            Self::Membership(error) => error.fmt(formatter),
            Self::RuntimeStopped => formatter.write_str("join runtime stopped"),
            Self::Deadline => formatter.write_str("join deadline elapsed"),
            Self::Connectivity(failures) => write!(
                formatter,
                "{} directed cluster route(s) are unavailable",
                failures.len()
            ),
        }
    }
}

impl std::error::Error for JoinError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientWriteFailure {
    Raft(RaftError<TC, ClientWriteError<TC>>),
    NodeIdentity { node_id: uuid::Uuid },
    LocalLeaderOnly,
}

impl std::fmt::Display for ClientWriteFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raft(error) => error.fmt(formatter),
            Self::NodeIdentity { node_id } => write!(
                formatter,
                "authenticated peer cannot publish configured addresses for node {node_id}",
            ),
            Self::LocalLeaderOnly => {
                formatter.write_str("reachability maintenance writes require the local leader")
            }
        }
    }
}

impl std::error::Error for ClientWriteFailure {}

fn authorize_reachability_write(
    command: &Command,
    matches_identity: impl Fn(uuid::Uuid) -> bool,
) -> std::result::Result<(), ClientWriteFailure> {
    let owner = match command {
        Command::ReplaceConfiguredReachableAddresses { node_id, .. } => Some(*node_id),
        Command::RenewReachabilityLeases(leases) => leases
            .iter()
            .find(|lease| !matches_identity(lease.node_id))
            .map(|lease| lease.node_id),
        Command::VerifyReachableAddress { .. }
        | Command::RecordConnectivity { .. }
        | Command::ReplaceAdmissionConfiguredReachableAddresses { .. }
        | Command::RenewAdmissionReachabilityLeases { .. }
        | Command::VerifyAdmissionReachableAddress { .. } => {
            return Err(ClientWriteFailure::LocalLeaderOnly);
        }
        _ => None,
    };
    if let Some(node_id) = owner.filter(|node_id| !matches_identity(*node_id)) {
        return Err(ClientWriteFailure::NodeIdentity { node_id });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResult {
    pub reachable_address: ReachableAddress,
    pub source_reachable_address_candidate: Option<ReachableAddress>,
}

upgrid_rpc::service! {
    pub service {
        trait UpgridService;
        client UpgridServiceClient;
        server UpgridServiceAdapter;
        request UpgridServiceRequest;
        response UpgridServiceResponse;
        DeploymentKeyFingerprint => deployment_key_fingerprint() -> [u8; 32];
        NodeIdentity => node_identity() -> uuid::Uuid;
        ReachableAddressCandidate => reachable_address_candidate(
            node_id: uuid::Uuid,
        ) -> Option<ReachableAddress>;
        AskToJoin => ask_to_join(
            remote: NodeRegistration,
            token: String,
        ) -> Result<(), JoinError>;
        ProbeNode => probe_node(
            node_id: uuid::Uuid,
            candidates: Vec<ReachableAddress>,
        ) -> Vec<ProbeResult>;
        RemoveNode => remove_node(
            node_id: uuid::Uuid,
        ) -> Result<(), crate::MembershipError>;
        FullSnapshot => full_snapshot(
            vote: VoteOf<TC>,
            meta: SnapshotMetaOf<TC>,
            data: Vec<u8>,
        ) -> Result<SnapshotResponse<TC>, RaftError<TC>>;
        AppendEntries => append_entries(
            req: AppendEntriesRequest<TC>,
        ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>>;
        Vote => vote(
            req: VoteRequest<TC>,
        ) -> Result<VoteResponse<TC>, RaftError<TC>>;
        ClientWrite => client_write(
            req: Req,
        ) -> Result<ClientWriteResponse<TC>, ClientWriteFailure>;
        ReadIndex => read_index(
        ) -> Result<LogIdOf<TC>, RaftError<TC, LinearizableReadError<TC>>>;
    }
}

#[derive(Clone)]
pub struct UpgridServer {
    self_id: uuid::Uuid,
    source_endpoint: PeerAddress,
    peer_identity: PeerIdentity,
    rpc: Rpc,
    raft: Raft,
    membership_changes: Rc<Mutex<()>>,
    deployment_key_fingerprint: [u8; 32],
}

impl UpgridServer {
    pub fn new(
        self_id: uuid::Uuid,
        source_endpoint: PeerAddress,
        peer_identity: PeerIdentity,
        rpc: Rpc,
        raft: Raft,
        membership_changes: Rc<Mutex<()>>,
        deployment_key_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            self_id,
            peer_identity,
            source_endpoint,
            rpc,
            raft,
            membership_changes,
            deployment_key_fingerprint,
        }
    }

    fn has_member(&self, node_id: uuid::Uuid) -> bool {
        let metrics = self.raft.metrics();
        metrics
            .borrow_watched()
            .membership_config
            .nodes()
            .any(|(member_id, _)| *member_id == node_id)
    }
}

fn is_membership_change_in_progress(error: &RaftError<TC, ClientWriteError<TC>>) -> bool {
    matches!(
        error,
        RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::InProgress(_)
        ))
    )
}

pub(crate) async fn remove_member(
    context: &Context,
    node_id: uuid::Uuid,
    raft: &Raft,
    rpc: &Rpc,
) -> Result<(), crate::MembershipError> {
    let _membership_change = rpc.membership_changes().lock().await;
    let metrics = raft.metrics();
    let current = metrics.borrow_watched();
    let member = current
        .membership_config
        .nodes()
        .any(|(candidate_id, _)| *candidate_id == node_id);
    let voters = current
        .membership_config
        .membership()
        .voter_ids()
        .collect::<BTreeSet<_>>();
    drop(current);
    if !member {
        return Err(crate::MembershipError::NodeNotFound(node_id));
    }
    loop {
        let result = if voters.contains(&node_id) {
            let voters = voters_without(voters.clone(), node_id)?;
            raft.change_membership(voters, false).await
        } else {
            raft.change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
                .await
        };
        match result {
            Ok(_) => break,
            Err(error)
                if is_membership_change_in_progress(&error)
                    && Instant::now() < context.deadline() =>
            {
                sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(error) => {
                return Err(crate::MembershipError::ChangeRejected(error.to_string()));
            }
        }
    }
    Ok(())
}

fn voters_without(
    mut voters: std::collections::BTreeSet<uuid::Uuid>,
    node_id: uuid::Uuid,
) -> Result<std::collections::BTreeSet<uuid::Uuid>, crate::MembershipError> {
    if !voters.remove(&node_id) {
        return Err(crate::MembershipError::NodeNotFound(node_id));
    }
    if voters.is_empty() {
        return Err(crate::MembershipError::LastVoter);
    }
    Ok(voters)
}

impl UpgridService for UpgridServer {
    async fn deployment_key_fingerprint(self, _: Context) -> [u8; 32] {
        self.deployment_key_fingerprint
    }

    async fn node_identity(self, _: Context) -> uuid::Uuid {
        self.self_id
    }

    async fn reachable_address_candidate(
        self,
        _: Context,
        node_id: uuid::Uuid,
    ) -> Option<ReachableAddress> {
        (self.has_member(node_id) && self.peer_identity.matches(node_id))
            .then(|| self.connection_reachable_address_candidate())
    }

    async fn probe_node(
        self,
        _: Context,
        node_id: uuid::Uuid,
        candidates: Vec<ReachableAddress>,
    ) -> Vec<ProbeResult> {
        self.rpc.probe_candidates(node_id, &candidates).await
    }

    async fn ask_to_join(
        self,
        context: Context,
        remote: NodeRegistration,
        token: String,
    ) -> Result<(), JoinError> {
        if !self.peer_identity.matches(remote.id) {
            return Err(JoinError::NodeIdentity { node_id: remote.id });
        }
        self.admit(context, remote, token).await
    }

    async fn remove_node(
        self,
        context: Context,
        node_id: uuid::Uuid,
    ) -> Result<(), crate::MembershipError> {
        remove_member(&context, node_id, &self.raft, &self.rpc).await
    }

    async fn full_snapshot(
        self,
        _: Context,
        vote: VoteOf<TC>,
        meta: SnapshotMetaOf<TC>,
        data: Vec<u8>,
    ) -> Result<SnapshotResponse<TC>, RaftError<TC>> {
        self.raft
            .install_full_snapshot(
                vote,
                Snapshot {
                    meta,
                    snapshot: Cursor::new(data),
                },
            )
            .await
            .map_err(RaftError::Fatal)
    }

    async fn append_entries(
        self,
        _: Context,
        req: AppendEntriesRequest<TC>,
    ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>> {
        self.raft.append_entries(req).await
    }

    async fn vote(
        self,
        _: Context,
        req: VoteRequest<TC>,
    ) -> Result<VoteResponse<TC>, RaftError<TC>> {
        self.raft.vote(req).await
    }

    async fn client_write(
        self,
        _: Context,
        mut req: Req,
    ) -> Result<ClientWriteResponse<TC>, ClientWriteFailure> {
        authorize_reachability_write(&req.command, |node_id| self.peer_identity.matches(node_id))?;
        req.command
            .stamp_reachability_leases(upgrid_config::now_ms());
        self.raft
            .client_write(req)
            .await
            .map_err(ClientWriteFailure::Raft)
    }

    async fn read_index(
        self,
        _: Context,
    ) -> Result<LogIdOf<TC>, RaftError<TC, LinearizableReadError<TC>>> {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map(|read_log_id| *read_log_id.log_id())
    }
}

#[cfg(test)]
mod test;
