use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use compio::runtime::spawn;
use compio::time::sleep;
use openraft::async_runtime::watch::WatchReceiver as _;

use super::Node;
use crate::raft::Req;
use crate::rpc::Rpc;
use crate::{ReachableAddress, Result};

impl Node {
    pub async fn publish_configured_reachable_addresses(&self) -> Result<()> {
        publish_reachability(
            self.id,
            &self.configured,
            self.configured_explicit,
            &self.candidates,
            &self.rpc,
        )
        .await
    }

    pub fn retry_reachability_publication(&self) {
        let node_id = self.id;
        let configured = self.configured.clone();
        let configured_explicit = self.configured_explicit;
        let candidates = self.candidates.clone();
        let raft = self.raft.clone();
        let rpc = self.rpc.clone();
        spawn(async move {
            loop {
                if local_removed(node_id, &raft) {
                    break;
                }
                let published = publish_reachability(
                    node_id,
                    &configured,
                    configured_explicit,
                    &candidates,
                    &rpc,
                )
                .await
                .is_ok();
                if published || local_removed(node_id, &raft) {
                    break;
                }
                sleep(Duration::from_secs(1)).await;
            }
        })
        .detach();
    }
}

fn local_removed(node_id: uuid::Uuid, raft: &crate::raft::Raft) -> bool {
    let metrics = raft.metrics();
    let current = metrics.borrow_watched();
    let members = current
        .membership_config
        .nodes()
        .map(|(member_id, _)| *member_id)
        .collect::<BTreeSet<_>>();
    membership_excludes(node_id, &members)
}

fn membership_excludes(node_id: uuid::Uuid, members: &BTreeSet<uuid::Uuid>) -> bool {
    !members.is_empty() && !members.contains(&node_id)
}

async fn publish_reachability(
    node_id: uuid::Uuid,
    configured: &BTreeSet<ReachableAddress>,
    configured_explicit: bool,
    candidates: &[crate::ReachableAddressCandidate],
    rpc: &Rpc,
) -> Result<()> {
    if configured_explicit {
        rpc.write_to_leader(
            Req::new(
                crate::domain::Command::ReplaceConfiguredReachableAddresses {
                    node_id,
                    addresses: configured.clone(),
                },
            ),
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    }
    if candidates.is_empty() {
        return Ok(());
    }
    let discovered_at_ms = upgrid_config::now_ms();
    let expires_at_ms = discovered_at_ms.saturating_add(crate::REACHABILITY_LEASE_MS);
    let leases = candidates
        .iter()
        .map(|candidate| crate::ReachableAddressLease {
            node_id,
            address: candidate.address.clone(),
            source: candidate.source.clone(),
            discovered_at_ms,
            expires_at_ms,
        })
        .collect();
    rpc.write_to_leader(
        Req::new(crate::domain::Command::RenewReachabilityLeases(leases)),
        Instant::now() + Duration::from_secs(5),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::membership_excludes;

    #[test]
    fn removal_requires_an_established_membership_without_the_local_node() {
        let node_id = uuid::Uuid::from_u128(1);
        assert!(!membership_excludes(node_id, &BTreeSet::new()));
        assert!(!membership_excludes(node_id, &BTreeSet::from([node_id])));
        assert!(membership_excludes(
            node_id,
            &BTreeSet::from([uuid::Uuid::from_u128(2)])
        ));
    }
}
