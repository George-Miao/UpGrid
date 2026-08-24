use super::*;

fn address(name: &str) -> ReachableAddress {
    ReachableAddress::parse(&format!("up://{name}:11451")).unwrap()
}

#[test]
fn connected_routes_are_stably_preferred() {
    let first = address("first.example");
    let second = address("second.example");
    let third = address("third.example");
    let fourth = address("fourth.example");
    let mut addresses = vec![first.clone(), second.clone(), third.clone(), fourth.clone()];

    stable_connectivity_order(
        &mut addresses,
        |_| true,
        |address| address == &second || address == &fourth,
    );

    assert_eq!(addresses, vec![second, fourth, first, third]);
}

#[test]
fn configured_routes_remain_ahead_of_connected_discovery_routes() {
    let configured = address("configured.example");
    let discovered = address("discovered.example");
    let mut addresses = vec![configured.clone(), discovered.clone()];

    stable_connectivity_order(
        &mut addresses,
        |address| address == &configured,
        |address| address == &discovered,
    );

    assert_eq!(addresses, vec![configured, discovered]);
}

#[test]
fn connected_candidates_extend_the_raft_dial_set() {
    let reachable = address("reachable.example");
    let connected_candidate = address("connected.example");
    let unconnected_candidate = address("unconnected.example");

    let addresses = include_connected_candidates(
        vec![reachable.clone()],
        vec![
            reachable.clone(),
            connected_candidate.clone(),
            unconnected_candidate,
        ],
        |address| address == &connected_candidate,
    );

    assert_eq!(addresses, vec![reachable, connected_candidate]);
}

#[test]
fn probe_schedule_accounts_for_every_route_once() {
    let addresses = vec![
        address("first.example"),
        address("second.example"),
        address("third.example"),
    ];

    let scheduled = candidate_attempts(addresses.clone()).collect::<Vec<_>>();

    assert_eq!(scheduled.len(), addresses.len());
    for (preference, address) in addresses.into_iter().enumerate() {
        assert_eq!(scheduled[preference].0, preference);
        assert_eq!(scheduled[preference].1, address);
    }
}

#[test]
fn every_success_is_retained_in_preference_order() {
    let preferred = address("preferred.example");
    let fallback = address("fallback.example");
    let results = preference_order(vec![
        (
            1,
            ProbeResult {
                reachable_address: fallback.clone(),
                source_reachable_address_candidate: None,
            },
        ),
        (
            0,
            ProbeResult {
                reachable_address: preferred.clone(),
                source_reachable_address_candidate: None,
            },
        ),
    ]);

    assert_eq!(
        results
            .into_iter()
            .map(|result| result.reachable_address)
            .collect::<Vec<_>>(),
        vec![preferred, fallback]
    );
}

#[test]
fn every_candidate_starts_inside_a_750_ms_caller_window() {
    assert_eq!(candidate_stagger(0), Duration::ZERO);
    assert_eq!(candidate_stagger(1), CANDIDATE_STAGGER);
    assert_eq!(candidate_stagger(usize::MAX), MAX_CANDIDATE_STAGGER);
    assert!(MAX_CANDIDATE_STAGGER + CANDIDATE_ATTEMPT_TIMEOUT < Duration::from_millis(750));
}

#[test]
fn route_connectivity_retains_only_current_candidates() {
    let node_id = uuid::Uuid::from_u128(1);
    let current = address("current.example");
    let expired = address("expired.example");
    let mut connectivity = RouteConnectivityTable::from([(
        node_id,
        std::collections::BTreeMap::from([(current.clone(), true), (expired, false)]),
    )]);

    retain_current_routes(&mut connectivity, node_id, std::slice::from_ref(&current));

    assert_eq!(
        connectivity.get(&node_id),
        Some(&std::collections::BTreeMap::from([(current, true)]))
    );
}
