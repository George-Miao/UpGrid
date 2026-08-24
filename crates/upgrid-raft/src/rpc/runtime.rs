//! Adapts generic transport channels to the UpGrid RPC service.

use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use compio::runtime::spawn;
use compio::time::{sleep, timeout};
use futures_channel::mpsc::UnboundedSender;
use openraft::error::{ClientWriteError, RaftError};
use openraft_rt_compio::futures::lock::Mutex;
use openraft_rt_compio::futures::{FutureExt, StreamExt, stream};
use tracing::debug;
use upgrid_rpc::Context;
use upgrid_rpc::server::Channel;
use upgrid_transport::{RpcSession, RpcTransport};

mod routes;

use super::service::{ClientWriteFailure, UpgridServer, UpgridServiceAdapter, UpgridServiceClient};
use crate::raft::{Raft, Req, Res};
use crate::state_machine::StateMachine;
use crate::{
    DirectedRoute, DiscoverySource, Error, ReachableAddress, ReachableAddressLease, Result,
};

const CONNECTIVITY_CHECK_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECTIVITY_FAN_OUT: usize = 4;
type RouteConnectivityTable = BTreeMap<uuid::Uuid, BTreeMap<ReachableAddress, bool>>;

#[derive(Clone)]
pub(crate) struct Rpc {
    raft: Rc<OnceCell<Raft>>,
    transport: RpcTransport,
    state_machine: Rc<StateMachine>,
    node_id: uuid::Uuid,
    membership_changes: Rc<Mutex<()>>,
    route_connectivity: Rc<RefCell<RouteConnectivityTable>>,
    deployment_key_fingerprint: [u8; 32],
}

#[derive(Clone)]
pub(crate) struct CandidateDiscovery {
    pub discovering_node_id: uuid::Uuid,
    pub candidate_node_id: uuid::Uuid,
    pub candidate: ReachableAddress,
    pub discovered_at_ms: u64,
}

impl CandidateDiscovery {
    pub(crate) fn into_lease(self, lease_ms: u64) -> ReachableAddressLease {
        ReachableAddressLease {
            node_id: self.candidate_node_id,
            address: self.candidate,
            source: DiscoverySource::Node {
                discovering_node_id: self.discovering_node_id,
            },
            discovered_at_ms: self.discovered_at_ms,
            expires_at_ms: self.discovered_at_ms.saturating_add(lease_ms),
        }
    }
}

pub(crate) struct ConnectivityReport {
    pub failures: BTreeSet<DirectedRoute>,
    pub verified: BTreeMap<uuid::Uuid, BTreeSet<ReachableAddress>>,
    pub candidate_discoveries: Vec<CandidateDiscovery>,
}

impl Rpc {
    pub(crate) fn new(
        node_id: uuid::Uuid,
        transport: RpcTransport,
        state_machine: Rc<StateMachine>,
        deployment_key_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            raft: Rc::new(OnceCell::new()),
            transport,
            state_machine,
            node_id,
            membership_changes: Rc::new(Mutex::new(())),
            route_connectivity: Rc::new(RefCell::new(BTreeMap::new())),
            deployment_key_fingerprint,
        }
    }

    pub(crate) fn init_raft(&self, raft: Raft) {
        assert!(
            self.raft.set(raft).is_ok(),
            "Raft should be initialized only once"
        );
    }

    fn raft(&self) -> &Raft {
        self.raft.get().expect("Raft should be initialized")
    }

    pub(crate) fn processed_operation_result(
        &self,
        operation_id: uuid::Uuid,
    ) -> Option<std::result::Result<crate::domain::CommandResult, crate::domain::DomainError>> {
        self.state_machine
            .application_state()
            .processed_operation_result(operation_id)
    }

    pub(crate) async fn write_to_leader(&self, request: Req, deadline: Instant) -> Result<Res> {
        let mut leader = None;
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                return Err(last_error.unwrap_or(Error::LeadershipDeadline));
            }

            let result = if let Some(leader_id) = leader.take() {
                let attempt_timeout = deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(1));
                let client = match timeout(attempt_timeout, self.client_to(leader_id)).await {
                    Ok(Ok(client)) => client,
                    Ok(Err(error)) => {
                        last_error = Some(error);
                        self.invalidate_node(leader_id);
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(_) => {
                        last_error = Some(Error::LeadershipDeadline);
                        self.invalidate_node(leader_id);
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let context = Context::with_deadline(
                    Instant::now()
                        + deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_secs(1)),
                );
                match client.client_write(context, request.clone()).await {
                    Ok(Ok(response)) => Ok(response),
                    Ok(Err(ClientWriteFailure::Raft(source))) => Err(source),
                    Ok(Err(ClientWriteFailure::NodeIdentity { node_id })) => {
                        return Err(Error::ConfiguredReachableAddressAuthentication { node_id });
                    }
                    Ok(Err(ClientWriteFailure::LocalLeaderOnly)) => {
                        return Err(Error::ReachabilityMaintenanceAuthentication);
                    }
                    Err(source) => {
                        last_error = Some(Error::RpcError { source });
                        self.invalidate_node(leader_id);
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
            } else {
                self.raft().client_write(request.clone()).await
            };
            match result {
                Ok(response) => return Ok(response.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward)))
                    if Instant::now() < deadline =>
                {
                    leader = forward.leader_id;
                    if leader.is_none() {
                        sleep(Duration::from_millis(50)).await;
                    }
                }
                Err(source) => return Err(Error::RaftWrite { source }),
            }
        }
    }

    pub(crate) fn membership_changes(&self) -> &Mutex<()> {
        &self.membership_changes
    }

    pub(crate) async fn admission_connectivity(
        &self,
        nodes: &BTreeSet<uuid::Uuid>,
    ) -> ConnectivityReport {
        self.connectivity_with_seed(nodes, None, Some(self.node_id))
            .await
    }

    pub(crate) async fn connectivity_reporting(
        &self,
        nodes: &BTreeSet<uuid::Uuid>,
        candidate_sender: Option<&UnboundedSender<Vec<CandidateDiscovery>>>,
    ) -> ConnectivityReport {
        self.connectivity_with_seed(nodes, candidate_sender, None)
            .await
    }

    async fn connectivity_with_seed(
        &self,
        nodes: &BTreeSet<uuid::Uuid>,
        candidate_sender: Option<&UnboundedSender<Vec<CandidateDiscovery>>>,
        seed_observer: Option<uuid::Uuid>,
    ) -> ConnectivityReport {
        let now_ms = upgrid_config::now_ms();
        let checks = nodes
            .iter()
            .flat_map(|source_id| {
                nodes
                    .iter()
                    .filter(move |destination_id| source_id != *destination_id)
                    .map(move |destination_id| {
                        let mut addresses = self
                            .state_machine
                            .reachable_address_candidates(*destination_id, *source_id, now_ms)
                            .unwrap_or_default();
                        if let Some(seed_observer) = seed_observer {
                            let seed_candidates = self
                                .state_machine
                                .reachable_address_candidates(
                                    *destination_id,
                                    seed_observer,
                                    now_ms,
                                )
                                .unwrap_or_default();
                            for candidate in seed_candidates {
                                if !addresses.contains(&candidate) {
                                    addresses.push(candidate);
                                }
                            }
                        }
                        (*source_id, *destination_id, addresses)
                    })
            })
            .collect::<Vec<_>>();
        let mut checks = stream::iter(checks)
            .map(
                |(source_id, destination_id, destination_candidates)| async move {
                    let probe_results = if source_id == self.node_id {
                        timeout(
                            CONNECTIVITY_CHECK_TIMEOUT,
                            self.probe_candidates(destination_id, &destination_candidates),
                        )
                        .await
                        .unwrap_or_default()
                    } else {
                        let probe = async {
                            self.client_to_candidates(source_id)
                                .await?
                                .probe_node(
                                    Context::current(),
                                    destination_id,
                                    destination_candidates,
                                )
                                .await
                                .map_err(|source| crate::Error::RpcError { source })
                        };
                        match timeout(CONNECTIVITY_CHECK_TIMEOUT, probe).await {
                            Ok(Ok(results)) => results,
                            _ => Vec::new(),
                        }
                    };
                    (
                        source_id,
                        destination_id,
                        probe_results,
                        upgrid_config::now_ms(),
                    )
                },
            )
            .buffer_unordered(CONNECTIVITY_FAN_OUT);
        let mut report = ConnectivityReport {
            failures: BTreeSet::new(),
            verified: BTreeMap::new(),
            candidate_discoveries: Vec::new(),
        };
        while let Some((source_id, destination_id, probe_results, discovered_at_ms)) =
            checks.next().await
        {
            if probe_results.is_empty() {
                report.failures.insert(DirectedRoute {
                    source: source_id,
                    destination: destination_id,
                });
                continue;
            }
            let first_discovery = report.candidate_discoveries.len();
            for probe_result in probe_results {
                report
                    .verified
                    .entry(destination_id)
                    .or_default()
                    .insert(probe_result.reachable_address.clone());
                report.candidate_discoveries.push(CandidateDiscovery {
                    discovering_node_id: source_id,
                    candidate_node_id: destination_id,
                    candidate: probe_result.reachable_address,
                    discovered_at_ms,
                });
                if let Some(candidate) = probe_result.source_reachable_address_candidate {
                    report.candidate_discoveries.push(CandidateDiscovery {
                        discovering_node_id: destination_id,
                        candidate_node_id: source_id,
                        candidate,
                        discovered_at_ms,
                    });
                }
            }
            if let Some(sender) = candidate_sender {
                let _ =
                    sender.unbounded_send(report.candidate_discoveries[first_discovery..].to_vec());
            }
        }
        report
    }

    async fn open_client(
        &self,
        node_id: uuid::Uuid,
        node: &ReachableAddress,
    ) -> Result<UpgridServiceClient> {
        let channel = self
            .transport
            .connect(node.host(), node.port(), node_id)
            .await?;
        let (client, dispatch) = UpgridServiceClient::new(channel);
        spawn(dispatch.map(|result| {
            if let Err(error) = result {
                debug!(%error, "client RPC stream closed");
            }
        }))
        .detach();
        Ok(client)
    }

    pub(crate) fn invalidate_node(&self, node_id: uuid::Uuid) {
        let addresses = self
            .state_machine
            .reachable_address_candidates(node_id, self.node_id, upgrid_config::now_ms())
            .unwrap_or_default();
        for address in addresses {
            self.route_failed(node_id, &address);
        }
    }

    pub(crate) fn invalidate(&self, node_id: uuid::Uuid, node: &ReachableAddress) {
        self.transport.invalidate(node.host(), node.port(), node_id);
    }

    pub(crate) async fn run(self) {
        while let Some(session) = self.transport.accept().await {
            match session {
                Ok(session) => spawn(self.clone().serve(session)).detach(),
                Err(error) => tracing::warn!(%error, "could not accept Node RPC session"),
            }
        }
        tracing::error!("Node RPC endpoint closed");
    }

    async fn serve(self, session: RpcSession) {
        let peer = session.peer_address();
        let peer_identity = session.peer_identity();
        let server = UpgridServer::new(
            self.node_id,
            peer.clone(),
            peer_identity,
            self.clone(),
            self.raft().clone(),
            self.membership_changes.clone(),
            self.deployment_key_fingerprint,
        );
        let mut channels = pin!(session.channels());
        while let Some(channel) = channels.next().await {
            match channel {
                Ok(channel) => {
                    let peer = peer.clone();
                    let service = UpgridServiceAdapter::new(server.clone());
                    spawn(async move {
                        if let Err(error) = Channel::new(channel).execute(service).run().await {
                            tracing::warn!(%peer, %error, "Node RPC stream failed");
                        }
                        debug!(%peer, "Node RPC stream disconnected");
                    })
                    .detach();
                }
                Err(error) => debug!(%peer, %error, "could not accept RPC channel"),
            }
        }
        debug!(%peer, "Node RPC session disconnected");
    }
}
