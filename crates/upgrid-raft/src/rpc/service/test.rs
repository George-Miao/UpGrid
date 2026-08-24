use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};

use compio::runtime::spawn;
use compio::time::sleep;
use openraft_rt_compio::futures::future::try_join;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use upgrid_config::{LocalAddress, QuicCaKey};
use upgrid_rpc::Context;
use upgrid_rpc::server::Channel;
use upgrid_transport::{bi_stream_framed, secure_endpoints};

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

#[test]
fn reachability_writes_require_the_authenticated_owner_or_local_leader() {
    let owner = uuid::Uuid::from_u128(1);
    let other = uuid::Uuid::from_u128(2);
    let address = ReachableAddress::parse("up://node.example:11451").unwrap();
    let configured = Command::ReplaceConfiguredReachableAddresses {
        node_id: owner,
        addresses: BTreeSet::from([address.clone()]),
    };
    assert!(authorize_reachability_write(&configured, |node_id| node_id == owner).is_ok());
    assert!(matches!(
        authorize_reachability_write(&configured, |node_id| node_id == other),
        Err(ClientWriteFailure::NodeIdentity { node_id }) if node_id == owner
    ));

    let leases = Command::RenewReachabilityLeases(vec![crate::ReachableAddressLease {
        node_id: other,
        address,
        source: crate::DiscoverySource::Service {
            url: "https://discovery.example/nodes".to_owned(),
        },
        expires_at_ms: 20,
        discovered_at_ms: 10,
    }]);
    assert!(matches!(
        authorize_reachability_write(&leases, |node_id| node_id == owner),
        Err(ClientWriteFailure::NodeIdentity { node_id }) if node_id == other
    ));

    let maintenance = Command::RecordConnectivity {
        leases: Vec::new(),
        verified: Some(Default::default()),
        checked_at_ms: 10,
        failures: BTreeSet::new(),
    };
    assert!(matches!(
        authorize_reachability_write(&maintenance, |_| true),
        Err(ClientWriteFailure::LocalLeaderOnly)
    ));
}

#[test]
fn client_write_failure_serializes_fatal_raft_errors() {
    let failure = ClientWriteFailure::Raft(RaftError::Fatal(openraft::error::Fatal::Stopped));
    let encoded = postcard::to_stdvec(&failure).unwrap();
    let decoded: ClientWriteFailure = postcard::from_bytes(&encoded).unwrap();

    assert!(matches!(
        decoded,
        ClientWriteFailure::Raft(RaftError::Fatal(openraft::error::Fatal::Stopped))
    ));
}

#[test]
fn join_redirect_serializes_every_leader_route() {
    let node_id = uuid::Uuid::from_u128(1);
    let addresses = vec![
        ReachableAddress::parse("up://first.example:11451").unwrap(),
        ReachableAddress::parse("up://second.example:11451").unwrap(),
    ];
    let error = JoinError::Redirect {
        node_id,
        addresses: addresses.clone(),
    };

    let encoded = postcard::to_stdvec(&error).unwrap();
    let decoded: JoinError = postcard::from_bytes(&encoded).unwrap();

    assert!(matches!(
        decoded,
        JoinError::Redirect {
            node_id: decoded_id,
            addresses: decoded_addresses,
        } if decoded_id == node_id && decoded_addresses == addresses
    ));
}

#[derive(Clone)]
pub struct DummyServer {}

impl UpgridService for DummyServer {
    async fn deployment_key_fingerprint(self, _: Context) -> [u8; 32] {
        [0; 32]
    }

    async fn node_identity(self, _: Context) -> uuid::Uuid {
        uuid::Uuid::nil()
    }

    async fn reachable_address_candidate(
        self,
        _: Context,
        _: uuid::Uuid,
    ) -> Option<ReachableAddress> {
        None
    }

    async fn ask_to_join(
        self,
        _: Context,
        remote: NodeRegistration,
        token: String,
    ) -> Result<(), JoinError> {
        info!(?remote, "Ask to join");
        if token == "slow" {
            sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn probe_node(
        self,
        _: Context,
        _: uuid::Uuid,
        candidates: Vec<ReachableAddress>,
    ) -> Vec<super::ProbeResult> {
        candidates
            .into_iter()
            .map(|reachable_address| super::ProbeResult {
                reachable_address,
                source_reachable_address_candidate: None,
            })
            .collect()
    }

    async fn remove_node(self, _: Context, _: uuid::Uuid) -> Result<(), crate::MembershipError> {
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
    ) -> Result<ClientWriteResponse<TC>, ClientWriteFailure> {
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
async fn multiplexes_requests_and_recovers_after_deadline() {
    let target_filter = Targets::new()
        .with_default(LevelFilter::INFO)
        .with_target("rustls", LevelFilter::WARN);

    let fmt = tracing_subscriber::fmt::layer().with_filter(target_filter);

    tracing_subscriber::registry().with(fmt).init();

    let quic_ca_key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
    let mut e1 = secure_endpoints(
        &BTreeSet::from([LocalAddress {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
        }]),
        uuid::Uuid::from_u128(1),
        &quic_ca_key,
    )
    .await
    .unwrap();
    let mut e2 = secure_endpoints(
        &BTreeSet::from([LocalAddress {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
        }]),
        uuid::Uuid::from_u128(2),
        &quic_ca_key,
    )
    .await
    .unwrap();
    let e1 = e1.pop().unwrap();
    let e2 = e2.pop().unwrap();
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

        Channel::new(transport)
            .execute(UpgridServiceAdapter::new(DummyServer {}))
            .run()
            .await
            .err()
            .map(|error| format!("{error:?}"))
    });

    sleep(Duration::from_secs(1)).await;

    info!("Connecting to server...");

    let conn = e2
        .connect(server_addr, "upgrid-node", None)
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

    let (client, dispatch) = UpgridServiceClient::new(transport);
    let dispatch_handle = spawn(dispatch);
    assert_eq!(
        client.node_identity(Context::current()).await.unwrap(),
        uuid::Uuid::nil()
    );
    let first_client = client.clone();
    let second_client = client.clone();
    let first = first_client.ask_to_join(
        Context::current(),
        NodeRegistration::new("up://dummy:11451").unwrap(),
        "test".to_owned(),
    );
    let second = second_client.ask_to_join(
        Context::current(),
        NodeRegistration::new("up://dummy:11451").unwrap(),
        "test".to_owned(),
    );
    let (first, second) = try_join(first, second).await.unwrap();
    first.unwrap();
    second.unwrap();
    let context = Context::with_deadline(Instant::now() + Duration::from_millis(50));
    let error = client
        .ask_to_join(
            context,
            NodeRegistration::new("up://dummy:11451").unwrap(),
            "slow".to_owned(),
        )
        .await
        .expect_err("slow RPC must exceed its deadline");
    assert!(matches!(error, upgrid_rpc::CallError::DeadlineExceeded));
    client
        .reachable_address_candidate(Context::current(), uuid::Uuid::nil())
        .await
        .unwrap();
    drop(first_client);
    drop(second_client);
    drop(client);
    dispatch_handle.await.unwrap().unwrap();

    let server_error = server_handle.await.unwrap();
    assert_eq!(server_error, None, "server transport should close cleanly");
}
