use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use compio::runtime::{JoinHandle, spawn};
use compio::time::{sleep, timeout};
use futures_channel::mpsc::unbounded;
use futures_util::future::{Either, select};
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use openraft::Config;
use openraft::async_runtime::watch::WatchReceiver as _;
use tracing::{debug, info};
use upgrid_rpc::Context;

use super::Node;
use crate::error::Error;
use crate::raft::{Raft, Req};
use crate::rpc::{CandidateDiscovery, ConnectivityReport, Rpc, remove_member};
use crate::state_machine::StateMachine;
use crate::{ReachableAddress, Result};

const REACHABLE_ADDRESS_CANDIDATE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
const CONNECTIVITY_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);

impl Node {
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.id)
    }

    pub async fn probe_node(&self, node_id: uuid::Uuid) -> Result<ReachableAddress> {
        if node_id == self.id {
            return self
                .registration
                .as_ref()
                .map(|registration| registration.bootstrap.clone())
                .ok_or(Error::NoReachableAddress { node_id });
        }
        match timeout(Duration::from_secs(2), self.rpc.probe_node(node_id)).await {
            Ok(addresses) => addresses
                .into_iter()
                .next()
                .ok_or(Error::NodeProbeFailed { node_id }),
            Err(_) => Err(Error::NodeProbeFailed { node_id }),
        }
    }

    pub(crate) async fn remove_node(
        &self,
        node_id: uuid::Uuid,
    ) -> Result<(), crate::MembershipError> {
        let leader_id = {
            let metrics = self.raft.metrics();
            let current = metrics.borrow_watched();
            current
                .current_leader
                .ok_or(crate::MembershipError::LeaderUnavailable)?
        };
        if leader_id == self.id {
            return remove_member(&Context::current(), node_id, &self.raft, &self.rpc).await;
        }
        let client = self
            .rpc
            .client_to(leader_id)
            .await
            .map_err(|error| crate::MembershipError::Connection(error.into()))?;
        client
            .remove_node(Context::current(), node_id)
            .await
            .map_err(|error| crate::MembershipError::Connection(error.into()))?
    }

    pub fn voters(&self) -> BTreeSet<uuid::Uuid> {
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    pub fn cluster_topology(&self) -> (Option<uuid::Uuid>, BTreeSet<uuid::Uuid>) {
        let metrics = self.raft.metrics();
        let current = metrics.borrow_watched();
        let leader = current.current_leader;
        let member_ids = current
            .membership_config
            .nodes()
            .map(|(node_id, _)| *node_id)
            .collect();
        (leader, member_ids)
    }
}

pub(super) fn bootstrap_address(
    node_id: uuid::Uuid,
    configured: &BTreeSet<ReachableAddress>,
    candidates: &[crate::ReachableAddressCandidate],
) -> Result<ReachableAddress> {
    configured
        .first()
        .cloned()
        .or_else(|| {
            candidates
                .first()
                .map(|candidate| candidate.address.clone())
        })
        .ok_or(Error::NoReachableAddress { node_id })
}

// Admission owns learner promotion. This task only records observations and
// failures.
pub(super) fn watch_connectivity(
    node_id: uuid::Uuid,
    raft: Raft,
    rpc: Rpc,
    state_machine: Rc<StateMachine>,
) -> JoinHandle<()> {
    spawn(async move {
        let mut last_recorded_at_ms = 0;
        loop {
            let members = member_ids(&raft);
            if !members.is_empty() && !members.contains(&node_id) {
                break;
            }
            sleep(Duration::from_secs(2)).await;
            if raft.current_leader().await != Some(node_id) {
                last_recorded_at_ms = 0;
                continue;
            }
            let nodes = member_ids(&raft);
            let Some(report) =
                connectivity_report_with_renewals(node_id, &nodes, &raft, &rpc).await
            else {
                last_recorded_at_ms = 0;
                continue;
            };
            let _membership_change = rpc.membership_changes().lock().await;
            if raft.current_leader().await != Some(node_id) {
                last_recorded_at_ms = 0;
                continue;
            }
            let current_nodes = member_ids(&raft);
            if current_nodes != nodes {
                continue;
            }
            let now_ms = upgrid_config::now_ms();
            let renewal_due = now_ms.saturating_sub(last_recorded_at_ms) >= 20_000;
            let new_candidate = report.candidate_discoveries.iter().any(|discovery| {
                let directly_reached = report
                    .verified
                    .get(&discovery.candidate_node_id)
                    .is_some_and(|addresses| addresses.contains(&discovery.candidate));
                if directly_reached {
                    !state_machine.is_reachable_address(
                        discovery.candidate_node_id,
                        &discovery.candidate,
                        discovery.discovering_node_id,
                        now_ms,
                    )
                } else {
                    !state_machine
                        .reachable_address_candidates(
                            discovery.candidate_node_id,
                            discovery.discovering_node_id,
                            now_ms,
                        )
                        .is_some_and(|addresses| addresses.contains(&discovery.candidate))
                }
            });
            if !renewal_due
                && !new_candidate
                && !state_machine.connectivity_scan_requires_record(&report.failures, now_ms)
            {
                continue;
            }
            let leases = report
                .candidate_discoveries
                .into_iter()
                .map(|mut discovery| {
                    discovery.discovered_at_ms = now_ms;
                    discovery.into_lease(crate::REACHABILITY_LEASE_MS)
                })
                .collect();
            let result = raft
                .client_write(Req::new(crate::domain::Command::RecordConnectivity {
                    leases,
                    verified: Some(report.verified),
                    checked_at_ms: now_ms,
                    failures: report.failures,
                }))
                .await;
            match result {
                Ok(_) => last_recorded_at_ms = now_ms,
                Err(error) => debug!(%error, "could not record cluster connectivity"),
            }
        }
    })
}

type CandidateKey = (uuid::Uuid, uuid::Uuid, ReachableAddress);

fn retain_candidate_discoveries(
    retained: &mut BTreeSet<CandidateKey>,
    discoveries: impl IntoIterator<Item = CandidateDiscovery>,
) {
    retained.extend(discoveries.into_iter().map(|discovery| {
        (
            discovery.discovering_node_id,
            discovery.candidate_node_id,
            discovery.candidate,
        )
    }));
}

fn candidate_lease(
    candidate: &CandidateKey,
    discovered_at_ms: u64,
) -> crate::ReachableAddressLease {
    crate::ReachableAddressLease {
        node_id: candidate.1,
        address: candidate.2.clone(),
        source: crate::DiscoverySource::Node {
            discovering_node_id: candidate.0,
        },
        discovered_at_ms,
        expires_at_ms: discovered_at_ms.saturating_add(crate::REACHABILITY_LEASE_MS),
    }
}

fn candidate_discovery(candidate: CandidateKey, discovered_at_ms: u64) -> CandidateDiscovery {
    CandidateDiscovery {
        discovering_node_id: candidate.0,
        candidate_node_id: candidate.1,
        candidate: candidate.2,
        discovered_at_ms,
    }
}

enum ConnectivityProgress {
    Complete(ConnectivityReport),
    Candidates(Vec<CandidateDiscovery>),
    Closed,
    Renew,
}

async fn next_connectivity_progress(
    scan: std::pin::Pin<&mut impl std::future::Future<Output = ConnectivityReport>>,
    receiver: &mut futures_channel::mpsc::UnboundedReceiver<Vec<CandidateDiscovery>>,
    renewal_delay: Duration,
) -> ConnectivityProgress {
    let renewal = Box::pin(sleep(renewal_delay));
    let progress = Box::pin(select(receiver.next(), renewal));
    match select(scan, progress).await {
        Either::Left((report, _)) => ConnectivityProgress::Complete(report),
        Either::Right((Either::Left((Some(batch), _)), _)) => {
            ConnectivityProgress::Candidates(batch)
        }
        Either::Right((Either::Left((None, _)), _)) => ConnectivityProgress::Closed,
        Either::Right((Either::Right(((), _)), _)) => ConnectivityProgress::Renew,
    }
}

async fn connectivity_report_with_renewals(
    node_id: uuid::Uuid,
    nodes: &BTreeSet<uuid::Uuid>,
    raft: &Raft,
    rpc: &Rpc,
) -> Option<ConnectivityReport> {
    let (sender, mut receiver) = unbounded();
    let mut scan = Box::pin(rpc.connectivity_reporting(nodes, Some(&sender)));
    let mut candidate_discoveries = BTreeSet::new();
    let mut renewed_at = Instant::now();
    loop {
        let renewal_delay = CONNECTIVITY_LEASE_RENEW_INTERVAL.saturating_sub(renewed_at.elapsed());
        match next_connectivity_progress(scan.as_mut(), &mut receiver, renewal_delay).await {
            ConnectivityProgress::Complete(mut report) => {
                retain_candidate_discoveries(
                    &mut candidate_discoveries,
                    std::mem::take(&mut report.candidate_discoveries),
                );
                let discovered_at_ms = upgrid_config::now_ms();
                report.candidate_discoveries = candidate_discoveries
                    .into_iter()
                    .map(|candidate| candidate_discovery(candidate, discovered_at_ms))
                    .collect();
                return Some(report);
            }
            ConnectivityProgress::Candidates(batch) => {
                retain_candidate_discoveries(&mut candidate_discoveries, batch);
            }
            ConnectivityProgress::Closed => return None,
            ConnectivityProgress::Renew => {}
        }
        if renewed_at.elapsed() < CONNECTIVITY_LEASE_RENEW_INTERVAL {
            continue;
        }
        renewed_at = Instant::now();
        if candidate_discoveries.is_empty() {
            continue;
        }
        if raft.current_leader().await != Some(node_id) || member_ids(raft) != *nodes {
            return None;
        }
        let discovered_at_ms = upgrid_config::now_ms();
        let leases = candidate_discoveries
            .iter()
            .map(|candidate| candidate_lease(candidate, discovered_at_ms))
            .collect();
        if let Err(error) = raft
            .client_write(Req::new(crate::domain::Command::RenewReachabilityLeases(
                leases,
            )))
            .await
        {
            debug!(%error, "could not renew reachable address candidates");
        }
    }
}

pub(super) fn watch_reachable_address_candidates(
    node_id: uuid::Uuid,
    raft: Raft,
    rpc: Rpc,
) -> JoinHandle<()> {
    spawn(async move {
        loop {
            let Some(renewed) = renew_reachable_address_candidates(node_id, &raft, &rpc).await
            else {
                break;
            };
            let delay = if renewed {
                REACHABLE_ADDRESS_CANDIDATE_RENEW_INTERVAL
            } else {
                Duration::from_secs(1)
            };
            sleep(delay).await;
        }
    })
}

async fn renew_reachable_address_candidates(
    node_id: uuid::Uuid,
    raft: &Raft,
    rpc: &Rpc,
) -> Option<bool> {
    let source_node_ids = match candidate_sources(node_id, raft) {
        CandidateSources::Pending => return Some(false),
        CandidateSources::Removed => return None,
        CandidateSources::Active(source_node_ids) => source_node_ids,
    };
    if source_node_ids.is_empty() {
        return Some(true);
    }

    let expected = source_node_ids.len();
    let mut requests = FuturesUnordered::new();
    for source_node_id in source_node_ids {
        let rpc = rpc.clone();
        requests.push(async move {
            let result = timeout(Duration::from_secs(3), async {
                rpc.client_to(source_node_id)
                    .await?
                    .reachable_address_candidate(Context::current(), node_id)
                    .await
                    .map_err(|source| crate::Error::RpcError { source })
            })
            .await;
            (source_node_id, result)
        });
    }

    let discovered_at_ms = upgrid_config::now_ms();
    let expires_at_ms = discovered_at_ms.saturating_add(crate::REACHABILITY_LEASE_MS);
    let mut leases = Vec::with_capacity(expected);
    while let Some((source_node_id, result)) = requests.next().await {
        match result {
            Ok(Ok(Some(address))) => leases.push(crate::ReachableAddressLease {
                node_id,
                address,
                source: crate::DiscoverySource::Node {
                    discovering_node_id: source_node_id,
                },
                discovered_at_ms,
                expires_at_ms,
            }),
            Ok(Ok(None)) => {
                debug!(%source_node_id, "source node did not recognize this node");
            }
            Ok(Err(error)) => {
                debug!(%source_node_id, %error, "could not request a reachable address candidate");
            }
            Err(_) => {
                debug!(%source_node_id, "reachable address candidate request timed out");
            }
        }
    }
    if leases.is_empty() {
        return Some(false);
    }

    let complete = leases.len() == expected;
    match rpc
        .write_to_leader(
            Req::new(crate::domain::Command::RenewReachabilityLeases(leases)),
            Instant::now() + Duration::from_secs(3),
        )
        .await
    {
        Ok(_) => Some(complete),
        Err(error) => {
            debug!(%error, "could not renew node-reported reachable address candidates");
            Some(false)
        }
    }
}

enum CandidateSources {
    Pending,
    Removed,
    Active(Vec<uuid::Uuid>),
}

fn candidate_sources(node_id: uuid::Uuid, raft: &Raft) -> CandidateSources {
    let members = member_ids(raft);
    if members.is_empty() {
        return CandidateSources::Pending;
    }
    if !members.contains(&node_id) {
        return CandidateSources::Removed;
    }
    CandidateSources::Active(
        members
            .into_iter()
            .filter(|member_id| *member_id != node_id)
            .collect(),
    )
}

fn member_ids(raft: &Raft) -> BTreeSet<uuid::Uuid> {
    let metrics = raft.metrics();
    let current = metrics.borrow_watched();
    current
        .membership_config
        .nodes()
        .map(|(node_id, _)| *node_id)
        .collect()
}

pub(super) fn watch_cluster_events(raft: Raft, rpc: Rpc) -> JoinHandle<()> {
    spawn(async move {
        let mut metrics = raft.metrics();
        let mut previous_leader = None;
        let mut previous_members = BTreeSet::new();

        loop {
            let (leader, members) = {
                let current = metrics.borrow_watched();
                let members = current
                    .membership_config
                    .nodes()
                    .map(|(node_id, _)| *node_id)
                    .collect::<BTreeSet<_>>();
                (current.current_leader, members)
            };
            rpc.retain_member_route_connectivity(&members);

            for node_id in members.difference(&previous_members) {
                info!(%node_id, "Node joined Cluster");
            }
            for node_id in previous_members.difference(&members) {
                info!(%node_id, "Node exited Cluster");
            }
            if leader != previous_leader {
                info!(?previous_leader, ?leader, "Cluster leader changed");
            }

            previous_leader = leader;
            previous_members = members;
            if metrics.changed().await.is_err() {
                break;
            }
        }
    })
}

pub(super) fn raft_config() -> Config {
    Config {
        cluster_name: "UpGrid".to_string(),
        heartbeat_interval: 1_000,
        election_timeout_min: 3_000,
        election_timeout_max: 6_000,
        install_snapshot_timeout: 10_000,
        ..Config::default()
    }
}

#[cfg(test)]
mod tests;
