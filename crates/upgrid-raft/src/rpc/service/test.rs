
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
use upgrid_transport::{bi_stream_framed, insecure_endpoint};

use super::*;

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

#[test]
fn removal_requires_a_known_node_and_retains_a_voter() {
    let first = uuid::Uuid::from_u128(1);
    let second = uuid::Uuid::from_u128(2);
    assert_eq!(
        voters_without(std::collections::BTreeSet::from([first]), first),
        Err(crate::MembershipError::LastVoter)
    );
    assert_eq!(
        voters_without(
            std::collections::BTreeSet::from([first, second]),
            uuid::Uuid::nil()
        ),
        Err(crate::MembershipError::NodeNotFound(uuid::Uuid::nil()))
    );
    assert_eq!(
        voters_without(std::collections::BTreeSet::from([first, second]), first).unwrap(),
        std::collections::BTreeSet::from([second])
    );
}

#[derive(Clone)]
pub struct DummyServer {}

impl UpgridService for DummyServer {
    async fn deployment_key_fingerprint(self, _: Context) -> [u8; 32] {
        [0; 32]
    }

    async fn ping(self, _: Context) {}

    async fn ask_to_join(
        self,
        _: tarpc::context::Context,
        remote: Identity,
        _: String,
    ) -> Result<(), JoinError> {
        info!(?remote, "Ask to join");
        Ok(())
    }

    async fn remove_node(
        self,
        _: tarpc::context::Context,
        _: uuid::Uuid,
    ) -> Result<(), crate::MembershipError> {
        Ok(())
    }

    async fn full_snapshot(
        self,
        _: Context,
        vote: VoteOf<TC>,
        meta: SnapshotMetaOf<TC>,
        data: Vec<u8>,
    ) -> Result<SnapshotResponse<TC>, RaftError<TC>> {
        info!(?vote, ?meta, bytes = data.len(), "Install snapshot");
        Ok(SnapshotResponse::new(vote))
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
    ) -> Result<LogIdOf<TC>, RaftError<TC, LinearizableReadError<TC>>> {
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
        insecure_endpoint("localhost".to_owned(), 0),
        insecure_endpoint("localhost".to_owned(), 0),
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
