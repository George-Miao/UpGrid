//! Typed Raft and membership RPC interface.

use std::io::Cursor;
use std::rc::Rc;
use std::time::Instant;

use compio::time::sleep;
use openraft::ReadPolicy;
use openraft::alias::{LogIdOf, SnapshotMetaOf, VoteOf};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ChangeMembershipError, ClientWriteError, LinearizableReadError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, SnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft_rt_compio::futures::lock::Mutex;
use serde::{Deserialize, Serialize};
use tarpc::context::Context;
use upgrid_config::now_ms;

use crate::Result;
use crate::domain::{Command, DomainError};
use crate::raft::{Identity, Raft, Req, TC};
use crate::token::hash_join_token;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinError {
    Raft(RaftError<TC, ClientWriteError<TC>>),
    Rejected(DomainError),
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raft(error) => error.fmt(formatter),
            Self::Rejected(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for JoinError {}

#[tarpc::service]
pub trait UpgridService {
    async fn deployment_key_fingerprint() -> [u8; 32];

    async fn ping();

    async fn ask_to_join(remote: Identity, token: String) -> Result<(), JoinError>;

    async fn remove_node(node_id: uuid::Uuid) -> Result<(), crate::MembershipError>;

    async fn full_snapshot(
        vote: VoteOf<TC>,
        meta: SnapshotMetaOf<TC>,
        data: Vec<u8>,
    ) -> Result<SnapshotResponse<TC>, RaftError<TC>>;

    async fn append_entries(
        req: AppendEntriesRequest<TC>,
    ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>>;

    async fn vote(req: VoteRequest<TC>) -> Result<VoteResponse<TC>, RaftError<TC>>;

    async fn client_write(
        req: Req,
    ) -> Result<ClientWriteResponse<TC>, RaftError<TC, ClientWriteError<TC>>>;

    async fn read_index() -> Result<LogIdOf<TC>, RaftError<TC, LinearizableReadError<TC>>>;
}

#[derive(Clone)]
pub struct UpgridServer {
    raft: Raft,
    membership_changes: Rc<Mutex<()>>,
    deployment_key_fingerprint: [u8; 32],
}

impl UpgridServer {
    pub fn new(
        raft: Raft,
        membership_changes: Rc<Mutex<()>>,
        deployment_key_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            raft,
            membership_changes,
            deployment_key_fingerprint,
        }
    }

    async fn add_learner_when_ready(
        &self,
        context: &Context,
        remote: &Identity,
    ) -> Result<(), RaftError<TC, ClientWriteError<TC>>> {
        loop {
            match self
                .raft
                .add_learner(remote.id, remote.node.clone(), true)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error)
                    if is_membership_change_in_progress(&error)
                        && Instant::now() < context.deadline =>
                {
                    sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn promote_learner_when_ready(
        &self,
        context: &Context,
        voters: &std::collections::BTreeSet<uuid::Uuid>,
    ) -> Result<(), RaftError<TC, ClientWriteError<TC>>> {
        loop {
            match self.raft.change_membership(voters.clone(), false).await {
                Ok(_) => return Ok(()),
                Err(error)
                    if is_membership_change_in_progress(&error)
                        && Instant::now() < context.deadline =>
                {
                    sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
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

    async fn ping(self, _: Context) {}

    async fn ask_to_join(
        self,
        context: tarpc::context::Context,
        remote: Identity,
        token: String,
    ) -> Result<(), JoinError> {
        let _membership_change = self.membership_changes.lock().await;
        let remote_id = remote.id;
        let metrics = self.raft.metrics();
        let (already_known, mut voters) = {
            let current = metrics.borrow_watched();
            let membership = current.membership_config.membership();
            (
                membership.nodes().any(|(node_id, _)| *node_id == remote_id),
                membership
                    .voter_ids()
                    .collect::<std::collections::BTreeSet<_>>(),
            )
        };
        if voters.contains(&remote_id) {
            return Ok(());
        }
        let authorized_at_ms = now_ms();
        let token_hash = hash_join_token(&token);
        let authorized = self
            .raft
            .client_write(Req::new(Command::AuthorizeJoinToken {
                hash: token_hash,
                authorized_at_ms,
            }))
            .await
            .map_err(JoinError::Raft)?;
        authorized.data.result.map_err(JoinError::Rejected)?;
        if !already_known {
            self.add_learner_when_ready(&context, &remote)
                .await
                .map_err(JoinError::Raft)?;
            voters = self
                .raft
                .metrics()
                .borrow_watched()
                .membership_config
                .membership()
                .voter_ids()
                .collect();
        }
        voters.insert(remote_id);
        self.promote_learner_when_ready(&context, &voters)
            .await
            .map_err(JoinError::Raft)?;
        Ok(())
    }

    async fn remove_node(
        self,
        context: tarpc::context::Context,
        node_id: uuid::Uuid,
    ) -> Result<(), crate::MembershipError> {
        let _membership_change = self.membership_changes.lock().await;
        let voters = self
            .raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        let voters = voters_without(voters, node_id)?;
        self.promote_learner_when_ready(&context, &voters)
            .await
            .map_err(|error| crate::MembershipError::ChangeRejected(error.to_string()))
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
        req: Req,
    ) -> Result<ClientWriteResponse<TC>, RaftError<TC, ClientWriteError<TC>>> {
        self.raft.client_write(req).await
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
#[path = "service/test.rs"]
mod test;
