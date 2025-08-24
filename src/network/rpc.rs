use compio::runtime::spawn;
use openraft::{
    error::{ClientWriteError, InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use tarpc::context::Context;

use crate::{
    Result,
    raft::{Identity, Raft, TC},
};

#[tarpc::service]
pub trait UpgridService {
    async fn ask_to_join(remote: Identity) -> Result<(), RaftError<TC, ClientWriteError<TC>>>;

    async fn install_snapshot(
        req: InstallSnapshotRequest<TC>,
    ) -> Result<InstallSnapshotResponse<TC>, RaftError<TC, InstallSnapshotError>>;

    async fn append_entries(
        req: AppendEntriesRequest<TC>,
    ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>>;

    async fn vote(req: VoteRequest<TC>) -> Result<VoteResponse<TC>, RaftError<TC>>;
}

#[derive(Clone)]
pub struct UpgridServer {
    raft: Raft,
}

impl UpgridServer {
    pub fn new(raft: Raft) -> Self {
        Self { raft }
    }
}

impl UpgridService for UpgridServer {
    async fn ask_to_join(
        self,
        _: tarpc::context::Context,
        remote: Identity,
    ) -> Result<(), RaftError<TC, ClientWriteError<TC>>> {
        spawn(async move {
            if let Err(e) = self.raft.add_learner(remote.id, remote.node, false).await {
                tracing::error!(?e, "Failed to add learner");
            }
        })
        .detach();

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
}

#[cfg(test)]
mod test {
    use std::{str::FromStr, time::Duration};

    use compio::{runtime::spawn, time::sleep};
    use openraft_rt_compio::futures::{StreamExt, future::try_join};
    use tarpc::{
        client::{Config, NewClient},
        context::Context,
        server::{BaseChannel, Channel},
    };
    use tracing::{info, level_filters::LevelFilter};
    use tracing_subscriber::{
        Layer, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt,
    };

    use super::*;
    use crate::{network::bi_stream_framed, utils::unsafe_endpoint};

    #[derive(Clone)]
    pub struct DummyServer {}

    impl UpgridService for DummyServer {
        async fn ask_to_join(
            self,
            _: tarpc::context::Context,
            remote: Identity,
        ) -> Result<(), RaftError<TC, ClientWriteError<TC>>> {
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
    }

    #[compio::test]
    async fn test_dummy_server() {
        let target_filter = Targets::new()
            .with_default(LevelFilter::INFO)
            .with_target("rustls", LevelFilter::WARN);

        let fmt = tracing_subscriber::fmt::layer().with_filter(target_filter);

        tracing_subscriber::registry().with(fmt).init();

        let (e1, e2) = try_join(
            unsafe_endpoint("localhost".to_owned(), 11451),
            unsafe_endpoint("localhost".to_owned(), 11452),
        )
        .await
        .unwrap();

        let _handle = spawn(async move {
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

            BaseChannel::with_defaults(transport)
                .requests()
                .execute(DummyServer {}.serve())
                .for_each(|f| async move {
                    spawn(f).detach();
                })
                .await;
        });

        sleep(Duration::from_secs(1)).await;

        info!("Connecting to server...");

        let conn = e2
            .connect(
                FromStr::from_str("127.0.0.1:11451").unwrap(),
                "localhost",
                None,
            )
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
        spawn(dispatch).detach();
        client
            .ask_to_join(Context::current(), Identity::new("up://dummy").unwrap())
            .await
            .unwrap()
            .unwrap();

        client
            .ask_to_join(Context::current(), Identity::new("up://dummy").unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
