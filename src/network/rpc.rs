use std::rc::Rc;
use std::time::Instant;

use compio::time::sleep;
use openraft::ReadPolicy;
use openraft::alias::LogIdOf;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{
    ChangeMembershipError, CheckIsLeaderError, ClientWriteError, InstallSnapshotError, RaftError,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft_rt_compio::futures::lock::Mutex;
use serde::{Deserialize, Serialize};
use tarpc::context::Context;

use crate::Result;
use crate::domain::{Command, DomainError};
use crate::raft::{Identity, Raft, Req, TC};
use crate::secret::{hash_join_token, join_operation_id};

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

    async fn ask_to_join(remote: Identity, token: String) -> Result<(), JoinError>;

    async fn install_snapshot(
        req: InstallSnapshotRequest<TC>,
    ) -> Result<InstallSnapshotResponse<TC>, RaftError<TC, InstallSnapshotError>>;

    async fn append_entries(
        req: AppendEntriesRequest<TC>,
    ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>>;

    async fn vote(req: VoteRequest<TC>) -> Result<VoteResponse<TC>, RaftError<TC>>;

    async fn client_write(
        req: Req,
    ) -> Result<ClientWriteResponse<TC>, RaftError<TC, ClientWriteError<TC>>>;

    async fn read_index() -> Result<Option<LogIdOf<TC>>, RaftError<TC, CheckIsLeaderError<TC>>>;
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

impl UpgridService for UpgridServer {
    async fn deployment_key_fingerprint(self, _: Context) -> [u8; 32] {
        self.deployment_key_fingerprint
    }

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
        let now_ms = crate::app::now_ms();
        let token_hash = hash_join_token(&token);
        let consumed = self
            .raft
            .client_write(Req {
                operation_id: join_operation_id(&token, remote_id),
                submitted_at_ms: now_ms,
                command: Command::ConsumeJoinToken {
                    hash: token_hash,
                    consumed_at_ms: now_ms,
                },
            })
            .await
            .map_err(JoinError::Raft)?;
        consumed.data.result.map_err(JoinError::Rejected)?;
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

    async fn install_snapshot(
        self,
        _: Context,
        req: InstallSnapshotRequest<TC>,
    ) -> Result<InstallSnapshotResponse<TC>, RaftError<TC, InstallSnapshotError>> {
        self.raft.install_snapshot(req).await
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
    ) -> Result<Option<LogIdOf<TC>>, RaftError<TC, CheckIsLeaderError<TC>>> {
        self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use compio::runtime::spawn;
    use compio::time::sleep;
    use openraft_rt_compio::futures::StreamExt;
    use openraft_rt_compio::futures::future::try_join;
    use tarpc::client::{Config, NewClient};
    use tarpc::context::Context;
    use tarpc::server::{BaseChannel, Channel};
    use tracing::info;
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::Layer;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use super::*;
    use crate::network::bi_stream_framed;
    use crate::utils::unsafe_endpoint;

    #[test]
    fn recognizes_membership_change_in_progress() {
        let error = RaftError::APIError(ClientWriteError::ChangeMembershipError(
            openraft::error::ChangeMembershipError::InProgress(openraft::error::InProgress {
                committed: None,
                membership_log_id: None,
            }),
        ));
        assert!(is_membership_change_in_progress(&error));
    }

    #[derive(Clone)]
    pub struct DummyServer {}

    impl UpgridService for DummyServer {
        async fn deployment_key_fingerprint(self, _: Context) -> [u8; 32] {
            [0; 32]
        }

        async fn ask_to_join(
            self,
            _: tarpc::context::Context,
            remote: Identity,
            _: String,
        ) -> Result<(), JoinError> {
            info!(?remote, "Ask to join");
            Ok(())
        }

        async fn install_snapshot(
            self,
            _: Context,
            req: InstallSnapshotRequest<TC>,
        ) -> Result<InstallSnapshotResponse<TC>, RaftError<TC, InstallSnapshotError>> {
            info!(?req, "Install snapshot");
            Ok(InstallSnapshotResponse { vote: req.vote })
        }

        async fn append_entries(
            self,
            _: Context,
            req: AppendEntriesRequest<TC>,
        ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>> {
            info!(?req, "Append entries");
            Ok(AppendEntriesResponse::Success)
        }

        async fn vote(
            self,
            _: Context,
            req: VoteRequest<TC>,
        ) -> Result<VoteResponse<TC>, RaftError<TC>> {
            info!(?req, "Vote");
            Ok(VoteResponse {
                vote: req.vote,
                vote_granted: true,
                last_log_id: None,
            })
        }

        async fn client_write(
            self,
            _: Context,
            _: Req,
        ) -> Result<ClientWriteResponse<TC>, RaftError<TC, ClientWriteError<TC>>> {
            unimplemented!("the transport smoke test does not issue writes")
        }

        async fn read_index(
            self,
            _: Context,
        ) -> Result<Option<LogIdOf<TC>>, RaftError<TC, CheckIsLeaderError<TC>>> {
            unimplemented!("the transport smoke test does not issue reads")
        }
    }

    #[compio::test]
    async fn reused_client_supports_concurrent_requests() {
        let target_filter = Targets::new()
            .with_default(LevelFilter::INFO)
            .with_target("rustls", LevelFilter::WARN);

        let fmt = tracing_subscriber::fmt::layer().with_filter(target_filter);

        tracing_subscriber::registry().with(fmt).init();

        let (e1, e2) = try_join(
            unsafe_endpoint("localhost".to_owned(), 0),
            unsafe_endpoint("localhost".to_owned(), 0),
        )
        .await
        .unwrap();
        let server_addr = std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            e1.local_addr().unwrap().port(),
        ));

        let server_handle = spawn(async move {
            let conn = e1
                .wait_incoming()
                .await
                .expect("Should be connected")
                .await
                .unwrap();

            info!("Connection established");

            let transport = conn
                .accept_bi()
                .await
                .map(|(s, r)| bi_stream_framed(s, r))
                .unwrap();

            info!("Stream accepted");

            let mut requests = Box::pin(BaseChannel::with_defaults(transport).requests());
            while let Some(request) = requests.next().await {
                match request {
                    Ok(request) => spawn(request.execute(DummyServer {}.serve())).detach(),
                    Err(error) => return Some(format!("{error:?}")),
                }
            }
            None
        });

        sleep(Duration::from_secs(1)).await;

        info!("Connecting to server...");

        let conn = e2
            .connect(server_addr, "localhost", None)
            .unwrap()
            .await
            .unwrap();

        sleep(Duration::from_secs(1)).await;

        info!("Get ID...");

        let transport = conn
            .open_bi_wait()
            .await
            .map(|(s, r)| bi_stream_framed(s, r))
            .unwrap();

        info!("Stream Opened...");

        let config = Config::default();
        let NewClient { client, dispatch } = UpgridServiceClient::new(config, transport);
        let dispatch_handle = spawn(dispatch);
        let first_client = client.clone();
        let second_client = client.clone();
        let first = first_client.ask_to_join(
            Context::current(),
            Identity::new("up://dummy").unwrap(),
            "test".to_owned(),
        );
        let second = second_client.ask_to_join(
            Context::current(),
            Identity::new("up://dummy").unwrap(),
            "test".to_owned(),
        );
        let (first, second) = try_join(first, second).await.unwrap();
        first.unwrap();
        second.unwrap();
        drop(first_client);
        drop(second_client);
        drop(client);
        dispatch_handle.await.unwrap().unwrap();

        let server_error = server_handle.await.unwrap();
        assert_eq!(server_error, None, "server transport should close cleanly");
    }
}
