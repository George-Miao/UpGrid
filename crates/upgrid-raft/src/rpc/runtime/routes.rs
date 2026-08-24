use std::pin::pin;
use std::time::Duration;

use compio::time::{sleep, timeout};
use openraft_rt_compio::futures::{StreamExt, stream};
use upgrid_rpc::Context;

use super::{RouteConnectivityTable, Rpc};
use crate::rpc::service::{ProbeResult, UpgridServiceClient};
use crate::{Error, ReachableAddress, Result};

pub(super) const CANDIDATE_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(600);
const CANDIDATE_STAGGER: Duration = Duration::from_millis(10);
const MAX_CANDIDATE_STAGGER: Duration = Duration::from_millis(50);

pub(super) fn stable_connectivity_order(
    addresses: &mut [ReachableAddress],
    mut is_configured: impl FnMut(&ReachableAddress) -> bool,
    mut is_connected: impl FnMut(&ReachableAddress) -> bool,
) {
    addresses.sort_by_key(|address| (!is_configured(address), !is_connected(address)));
}

pub(super) fn candidate_attempts(
    addresses: Vec<ReachableAddress>,
) -> impl Iterator<Item = (usize, ReachableAddress, Duration)> {
    addresses
        .into_iter()
        .enumerate()
        .map(|(rank, address)| (rank, address, candidate_stagger(rank)))
}

fn include_connected_candidates(
    mut reachable: Vec<ReachableAddress>,
    candidates: Vec<ReachableAddress>,
    mut is_connected: impl FnMut(&ReachableAddress) -> bool,
) -> Vec<ReachableAddress> {
    for candidate in candidates {
        if is_connected(&candidate) && !reachable.contains(&candidate) {
            reachable.push(candidate);
        }
    }
    reachable
}

fn candidate_stagger(rank: usize) -> Duration {
    CANDIDATE_STAGGER
        .saturating_mul(u32::try_from(rank).unwrap_or(u32::MAX))
        .min(MAX_CANDIDATE_STAGGER)
}

fn preference_order(mut successes: Vec<(usize, ProbeResult)>) -> Vec<ProbeResult> {
    successes.sort_by_key(|(preference, _)| *preference);
    successes.into_iter().map(|(_, result)| result).collect()
}

fn retain_current_routes(
    connectivity: &mut RouteConnectivityTable,
    node_id: uuid::Uuid,
    addresses: &[ReachableAddress],
) {
    let remove_node = connectivity.get_mut(&node_id).is_some_and(|routes| {
        routes.retain(|address, _| addresses.contains(address));
        routes.is_empty()
    });
    if remove_node {
        connectivity.remove(&node_id);
    }
}

impl Rpc {
    fn ordered_addresses(
        &self,
        node_id: uuid::Uuid,
        mut addresses: Vec<ReachableAddress>,
    ) -> Vec<ReachableAddress> {
        let mut connectivity = self.route_connectivity.borrow_mut();
        retain_current_routes(&mut connectivity, node_id, &addresses);
        stable_connectivity_order(
            &mut addresses,
            |address| {
                self.state_machine
                    .is_configured_reachable_address(node_id, address)
            },
            |address| {
                connectivity
                    .get(&node_id)
                    .is_some_and(|addresses| addresses.get(address) == Some(&true))
            },
        );
        addresses
    }

    fn set_route_connectivity(
        &self,
        node_id: uuid::Uuid,
        address: &ReachableAddress,
        connected: bool,
    ) {
        self.route_connectivity
            .borrow_mut()
            .entry(node_id)
            .or_default()
            .insert(address.clone(), connected);
    }

    pub(crate) fn retain_member_route_connectivity(
        &self,
        members: &std::collections::BTreeSet<uuid::Uuid>,
    ) {
        self.route_connectivity
            .borrow_mut()
            .retain(|node_id, _| members.contains(node_id));
    }

    pub(super) fn route_failed(&self, node_id: uuid::Uuid, address: &ReachableAddress) {
        self.set_route_connectivity(node_id, address, false);
        self.invalidate(node_id, address);
    }

    pub(crate) async fn client_to_addresses(
        &self,
        node_id: uuid::Uuid,
        addresses: Vec<ReachableAddress>,
    ) -> Result<UpgridServiceClient> {
        self.open_first(node_id, addresses).await
    }

    pub(crate) async fn client_to(&self, node_id: uuid::Uuid) -> Result<UpgridServiceClient> {
        let now_ms = upgrid_config::now_ms();
        let reachable = self
            .state_machine
            .reachable_addresses(node_id, self.node_id, now_ms)
            .unwrap_or_default();
        let candidates = self
            .state_machine
            .reachable_address_candidates(node_id, self.node_id, now_ms)
            .unwrap_or_default();
        let addresses = {
            let connectivity = self.route_connectivity.borrow();
            include_connected_candidates(reachable, candidates, |address| {
                connectivity
                    .get(&node_id)
                    .is_some_and(|addresses| addresses.get(address) == Some(&true))
            })
        };
        self.open_first(node_id, addresses).await
    }

    pub(crate) fn reachable_addresses(&self, node_id: uuid::Uuid) -> Vec<ReachableAddress> {
        let addresses = self
            .state_machine
            .reachable_addresses(node_id, self.node_id, upgrid_config::now_ms())
            .unwrap_or_default();
        self.ordered_addresses(node_id, addresses)
    }

    pub(super) async fn client_to_candidates(
        &self,
        node_id: uuid::Uuid,
    ) -> Result<UpgridServiceClient> {
        let addresses = self
            .state_machine
            .reachable_address_candidates(node_id, self.node_id, upgrid_config::now_ms())
            .unwrap_or_default();
        self.open_first(node_id, addresses).await
    }

    async fn open_first(
        &self,
        node_id: uuid::Uuid,
        addresses: Vec<ReachableAddress>,
    ) -> Result<UpgridServiceClient> {
        if addresses.is_empty() {
            return Err(Error::NoReachableAddress { node_id });
        }

        let addresses = self.ordered_addresses(node_id, addresses);
        let attempt_count = addresses.len();
        let attempts = stream::iter(candidate_attempts(addresses))
            .map(|(_, address, stagger)| async move {
                sleep(stagger).await;
                match timeout(
                    CANDIDATE_ATTEMPT_TIMEOUT,
                    self.open_candidate(node_id, &address),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        self.route_failed(node_id, &address);
                        Err(Error::ForwardConnectTimeout { node: address })
                    }
                }
            })
            .buffer_unordered(attempt_count);
        let mut attempts = pin!(attempts);
        let mut last_error = None;
        while let Some(result) = attempts.next().await {
            match result {
                Ok(client) => return Ok(client),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("a non-empty address list ensures one connection attempt"))
    }

    async fn open_candidate(
        &self,
        node_id: uuid::Uuid,
        address: &ReachableAddress,
    ) -> Result<UpgridServiceClient> {
        let client = match self.open_client(node_id, address).await {
            Ok(client) => client,
            Err(error) => {
                self.route_failed(node_id, address);
                return Err(error);
            }
        };
        let actual_node_id = match client.node_identity(Context::current()).await {
            Ok(node_id) => node_id,
            Err(source) => {
                self.route_failed(node_id, address);
                return Err(Error::RpcError { source });
            }
        };
        if actual_node_id != node_id {
            self.route_failed(node_id, address);
            return Err(Error::NodeIdentityMismatch {
                address: address.clone(),
                expected_node_id: node_id,
                actual_node_id,
            });
        }
        self.set_route_connectivity(node_id, address, true);
        Ok(client)
    }

    pub(crate) async fn probe_node(&self, node_id: uuid::Uuid) -> Vec<ReachableAddress> {
        let addresses = self
            .state_machine
            .reachable_address_candidates(node_id, self.node_id, upgrid_config::now_ms())
            .unwrap_or_default();
        self.probe_candidates(node_id, &addresses)
            .await
            .into_iter()
            .map(|probe| probe.reachable_address)
            .collect()
    }

    pub(crate) async fn probe_candidates(
        &self,
        node_id: uuid::Uuid,
        addresses: &[ReachableAddress],
    ) -> Vec<ProbeResult> {
        if addresses.is_empty() {
            return Vec::new();
        }
        let addresses = self.ordered_addresses(node_id, addresses.to_vec());
        let attempt_count = addresses.len();
        let attempts = stream::iter(candidate_attempts(addresses))
            .map(|(preference, address, stagger)| async move {
                sleep(stagger).await;
                let attempt = timeout(CANDIDATE_ATTEMPT_TIMEOUT, async {
                    self.open_candidate(node_id, &address)
                        .await?
                        .reachable_address_candidate(Context::current(), self.node_id)
                        .await
                        .map_err(|source| Error::RpcError { source })
                })
                .await;
                match attempt {
                    Ok(Ok(source_reachable_address_candidate)) => Some((
                        preference,
                        ProbeResult {
                            reachable_address: address,
                            source_reachable_address_candidate,
                        },
                    )),
                    Ok(Err(_)) | Err(_) => {
                        self.route_failed(node_id, &address);
                        None
                    }
                }
            })
            .buffer_unordered(attempt_count);
        let mut attempts = pin!(attempts);
        let mut successes = Vec::new();
        while let Some(result) = attempts.next().await {
            if let Some(result) = result {
                successes.push(result);
            }
        }
        preference_order(successes)
    }
}

#[cfg(test)]
mod tests;
