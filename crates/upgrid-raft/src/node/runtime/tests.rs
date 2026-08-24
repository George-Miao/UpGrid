use super::*;

#[test]
fn raft_timing_allows_for_network_and_disk_latency() {
    let config = raft_config();
    assert!(
        config.heartbeat_interval >= 500,
        "AppendEntries must not inherit OpenRaft's 50 ms default deadline"
    );
    assert!(config.election_timeout_min >= config.heartbeat_interval * 3);
    assert!(config.election_timeout_max >= config.election_timeout_min * 2);
    config.validate().unwrap();
}

#[compio::test]
async fn connectivity_renewal_timer_does_not_wait_for_candidates() {
    let (_sender, mut receiver) = unbounded();
    let mut scan = Box::pin(futures_util::future::pending::<ConnectivityReport>());

    let progress =
        next_connectivity_progress(scan.as_mut(), &mut receiver, Duration::from_millis(1)).await;

    assert!(matches!(progress, ConnectivityProgress::Renew));
}

#[test]
fn connectivity_renewal_deduplicates_and_refreshes_candidates() {
    let discovering_node_id = uuid::Uuid::from_u128(1);
    let candidate_node_id = uuid::Uuid::from_u128(2);
    let address = ReachableAddress::parse("up://candidate.example:11451").unwrap();
    let discoveries = [1, 2].map(|discovered_at_ms| CandidateDiscovery {
        discovering_node_id,
        candidate_node_id,
        candidate: address.clone(),
        discovered_at_ms,
    });
    let mut retained = BTreeSet::new();

    retain_candidate_discoveries(&mut retained, discoveries);

    assert_eq!(retained.len(), 1);
    let lease = candidate_lease(retained.first().unwrap(), 20_000);
    assert_eq!(lease.node_id, candidate_node_id);
    assert_eq!(lease.address, address);
    assert_eq!(lease.discovered_at_ms, 20_000);
    assert_eq!(lease.expires_at_ms, 20_000 + crate::REACHABILITY_LEASE_MS);
}
