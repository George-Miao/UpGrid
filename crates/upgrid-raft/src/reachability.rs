//! Validated reachability for durable cluster node identities.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use upgrid_config::ReachableAddress;
use uuid::Uuid;

/// The cluster-owned address set for one durable node identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeReachability {
    configured: BTreeSet<ReachableAddress>,
    #[serde(default, with = "crate::domain::map_as_entries")]
    discovered: BTreeMap<ReachableAddress, DiscoveredAddress>,
    #[serde(default, with = "crate::domain::map_as_entries")]
    verified: BTreeMap<ReachableAddress, u64>,
}

impl NodeReachability {
    pub fn configured(addresses: BTreeSet<ReachableAddress>) -> Self {
        Self {
            configured: addresses,
            discovered: BTreeMap::new(),
            verified: BTreeMap::new(),
        }
    }

    pub fn configured_reachable_addresses(&self) -> &BTreeSet<ReachableAddress> {
        &self.configured
    }

    pub fn verified_addresses(&self) -> &BTreeMap<ReachableAddress, u64> {
        &self.verified
    }

    pub fn replace_configured(&mut self, addresses: BTreeSet<ReachableAddress>) {
        self.configured = addresses;
        self.retain_verified();
    }

    pub(crate) fn add_configured(&mut self, address: ReachableAddress) -> bool {
        self.configured.insert(address)
    }

    pub fn renew(
        &mut self,
        address: ReachableAddress,
        source: DiscoverySource,
        discovered_at_ms: u64,
        expires_at_ms: u64,
    ) {
        let starts_new_generation = self
            .discovered
            .get(&address)
            .is_some_and(|discovered| !discovered.has_active_lease(discovered_at_ms));
        if starts_new_generation {
            self.verified.remove(&address);
        }
        let discovered = self
            .discovered
            .entry(address)
            .or_insert_with(|| DiscoveredAddress {
                leases: BTreeMap::new(),
                valid_since_ms: discovered_at_ms,
            });
        if starts_new_generation {
            discovered.valid_since_ms = discovered.valid_since_ms.max(discovered_at_ms);
        }
        discovered
            .leases
            .entry(source)
            .and_modify(|current| *current = (*current).max(expires_at_ms))
            .or_insert(expires_at_ms);
    }

    pub fn verify(&mut self, address: &ReachableAddress, verified_at_ms: u64) {
        let discovered_is_current = self.discovered.get(address).is_some_and(|discovered| {
            verified_at_ms >= discovered.valid_since_ms
                && discovered.has_active_lease(verified_at_ms)
        });
        if self.configured.contains(address) || discovered_is_current {
            self.verified.insert(address.clone(), verified_at_ms);
        }
    }

    pub fn expire(&mut self, now_ms: u64) {
        self.discovered.retain(|_, discovered| {
            discovered
                .leases
                .retain(|_, expires_at_ms| *expires_at_ms > now_ms);
            !discovered.leases.is_empty()
        });
        self.retain_verified();
    }

    pub(crate) fn has_expired_lease(&self, now_ms: u64) -> bool {
        self.discovered.values().any(|discovered| {
            discovered
                .leases
                .values()
                .any(|expires_at_ms| *expires_at_ms <= now_ms)
        })
    }

    pub fn candidates(&self, observer: Uuid, now_ms: u64) -> BTreeSet<ReachableAddress> {
        self.configured
            .iter()
            .chain(
                self.discovered
                    .iter()
                    .filter(|(_, discovered)| discovered.active_for(observer, now_ms))
                    .map(|(address, _)| address),
            )
            .cloned()
            .collect()
    }

    pub fn reachable(&self, observer: Uuid, now_ms: u64) -> BTreeSet<ReachableAddress> {
        self.configured
            .iter()
            .chain(
                self.discovered
                    .iter()
                    .filter(|(address, discovered)| {
                        self.verified.contains_key(*address)
                            && discovered.active_for(observer, now_ms)
                    })
                    .map(|(address, _)| address),
            )
            .cloned()
            .collect()
    }

    pub(crate) fn ordered_candidates(&self, observer: Uuid, now_ms: u64) -> Vec<ReachableAddress> {
        let mut addresses = self.configured.iter().cloned().collect::<Vec<_>>();
        addresses.extend(
            self.discovered
                .iter()
                .filter(|(address, discovered)| {
                    !self.configured.contains(*address) && discovered.active_for(observer, now_ms)
                })
                .map(|(address, _)| address.clone()),
        );
        addresses
    }

    pub(crate) fn ordered_reachable(&self, observer: Uuid, now_ms: u64) -> Vec<ReachableAddress> {
        let mut addresses = self.configured.iter().cloned().collect::<Vec<_>>();
        addresses.extend(
            self.discovered
                .iter()
                .filter(|(address, discovered)| {
                    !self.configured.contains(*address)
                        && self.verified.contains_key(*address)
                        && discovered.active_for(observer, now_ms)
                })
                .map(|(address, _)| address.clone()),
        );
        addresses
    }

    pub fn is_reachable(&self, address: &ReachableAddress, observer: Uuid, now_ms: u64) -> bool {
        self.configured.contains(address)
            || (self.verified.contains_key(address)
                && self
                    .discovered
                    .get(address)
                    .is_some_and(|discovered| discovered.active_for(observer, now_ms)))
    }

    pub fn preferred_reachable_address(
        &self,
        source_node_id: Uuid,
        now_ms: u64,
    ) -> Option<&ReachableAddress> {
        self.configured.iter().next().or_else(|| {
            self.discovered
                .iter()
                .find(|(address, discovered)| {
                    self.verified.contains_key(*address)
                        && discovered.active_for(source_node_id, now_ms)
                })
                .map(|(address, _)| address)
                .or_else(|| {
                    self.discovered
                        .iter()
                        .find(|(_, discovered)| discovered.has_active_service(now_ms))
                        .map(|(address, _)| address)
                })
        })
    }

    pub fn preferred_published_address(&self, now_ms: u64) -> Option<&ReachableAddress> {
        self.configured
            .iter()
            .next()
            .or_else(|| {
                self.discovered
                    .iter()
                    .find(|(address, discovered)| {
                        self.verified.contains_key(*address) && discovered.has_active_lease(now_ms)
                    })
                    .map(|(address, _)| address)
            })
            .or_else(|| {
                self.discovered
                    .iter()
                    .find(|(_, discovered)| discovered.has_active_service(now_ms))
                    .map(|(address, _)| address)
            })
    }

    fn retain_verified(&mut self) {
        self.verified.retain(|address, verified_at_ms| {
            self.configured.contains(address)
                || self
                    .discovered
                    .get(address)
                    .is_some_and(|discovered| *verified_at_ms >= discovered.valid_since_ms)
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReachableAddressCandidate {
    pub address: ReachableAddress,
    pub source: DiscoverySource,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DiscoverySource {
    Node { discovering_node_id: Uuid },
    Service { url: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct DiscoveredAddress {
    #[serde(default, with = "crate::domain::map_as_entries")]
    leases: BTreeMap<DiscoverySource, u64>,
    #[serde(default)]
    valid_since_ms: u64,
}

impl DiscoveredAddress {
    fn has_active_lease(&self, now_ms: u64) -> bool {
        self.leases
            .values()
            .any(|expires_at_ms| *expires_at_ms > now_ms)
    }

    fn active_for(&self, source_node_id: Uuid, now_ms: u64) -> bool {
        self.leases.iter().any(|(source, expires_at_ms)| {
            *expires_at_ms > now_ms
                && match source {
                    DiscoverySource::Node {
                        discovering_node_id,
                    } => *discovering_node_id == source_node_id,
                    DiscoverySource::Service { .. } => true,
                }
        })
    }

    fn has_active_service(&self, now_ms: u64) -> bool {
        self.leases.iter().any(|(source, expires_at_ms)| {
            *expires_at_ms > now_ms && matches!(source, DiscoverySource::Service { .. })
        })
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct NodeReachabilityV20260819 {
    configured: BTreeSet<ReachableAddress>,
    #[serde(default, with = "crate::domain::map_as_entries")]
    discovered: BTreeMap<ReachableAddress, DiscoveredAddressV20260819>,
    #[serde(default, with = "crate::domain::map_as_entries")]
    verified: BTreeMap<ReachableAddress, u64>,
}

#[derive(Serialize, Deserialize)]
struct DiscoveredAddressV20260819 {
    #[serde(default, with = "crate::domain::map_as_entries")]
    leases: BTreeMap<DiscoverySource, u64>,
}

impl From<NodeReachabilityV20260819> for NodeReachability {
    fn from(value: NodeReachabilityV20260819) -> Self {
        Self {
            configured: value.configured,
            discovered: value
                .discovered
                .into_iter()
                .map(|(address, discovered)| {
                    (
                        address,
                        DiscoveredAddress {
                            leases: discovered.leases,
                            valid_since_ms: 0,
                        },
                    )
                })
                .collect(),
            verified: value.verified,
        }
    }
}

#[cfg(test)]
impl From<NodeReachability> for NodeReachabilityV20260819 {
    fn from(value: NodeReachability) -> Self {
        Self {
            configured: value.configured,
            discovered: value
                .discovered
                .into_iter()
                .map(|(address, discovered)| {
                    (
                        address,
                        DiscoveredAddressV20260819 {
                            leases: discovered.leases,
                        },
                    )
                })
                .collect(),
            verified: value.verified,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReachableAddressLease {
    pub node_id: Uuid,
    pub address: ReachableAddress,
    pub source: DiscoverySource,
    pub expires_at_ms: u64,
    #[serde(default, alias = "observed_at_ms")]
    pub discovered_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DirectedRoute {
    pub source: Uuid,
    pub destination: Uuid,
}

#[cfg(test)]
mod tests;
