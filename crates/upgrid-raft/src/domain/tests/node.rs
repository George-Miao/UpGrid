use super::evaluation::{evaluation, id, state_with_target};
use super::*;

fn node(node_id: Uuid, name: &str) -> NodeTarget {
    NodeTarget {
        node_id,
        name: name.to_owned(),
        url: Url::parse(&format!(
            "up://127.0.0.1:{}",
            11_451 + node_id.as_u128() as u16
        ))
        .unwrap(),
        policy: EvaluationPolicy {
            interval_ms: 10_000,
            timeout_ms: 2_000,
            failure_threshold: 1,
        },
    }
}

#[test]
fn node_availability_transitions_use_default_channels() {
    let (mut state, _, channel_id) = state_with_target();
    state
        .apply(Command::SetNotificationChannelDefault {
            channel_id,
            is_default: true,
        })
        .unwrap();
    let node_id = id(20);
    let node = node(node_id, "green-anchor");
    state
        .apply(Command::SyncNodeTargets(vec![node.clone()]))
        .unwrap();
    let mut failed = evaluation(node.id(), 1_000, false);
    failed.http.final_url = node.url.clone();

    let result = state
        .apply(Command::RecordNodeEvaluation(failed.clone()))
        .unwrap();

    let CommandResult::NodeEvaluationAccepted {
        availability,
        alerts,
    } = result
    else {
        panic!("Node Evaluation should be accepted");
    };
    assert_eq!(availability, AvailabilityState::Down);
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].channel_id, channel_id);
    assert_eq!(
        state.availability_transitions[&failed.id].target_name,
        "green-anchor"
    );
    assert_eq!(state.alerts[&alerts[0]].target_url, node.url);
}

#[test]
fn syncing_membership_preserves_health_and_removes_departed_nodes() {
    let mut state = ApplicationState::default();
    let node_id = id(30);
    let mut current = node(node_id, "green-anchor");
    state
        .apply(Command::SyncNodeTargets(vec![current.clone()]))
        .unwrap();
    let mut passed = evaluation(current.id(), 1_000, true);
    passed.http.final_url = current.url.clone();
    state.apply(Command::RecordNodeEvaluation(passed)).unwrap();

    current.name = "renamed-anchor".to_owned();
    state
        .apply(Command::SyncNodeTargets(vec![current.clone()]))
        .unwrap();
    let retained = &state.node_targets[&current.id()];
    assert_eq!(retained.target.name, "renamed-anchor");
    assert_eq!(retained.availability, AvailabilityState::Up);
    assert_eq!(retained.history.len(), 1);

    state.apply(Command::SyncNodeTargets(Vec::new())).unwrap();
    assert!(state.node_targets.is_empty());
}

#[test]
fn renaming_a_node_updates_its_target_without_losing_history() {
    let mut state = ApplicationState::default();
    let node_id = id(31);
    let node = node(node_id, "green-anchor");
    state
        .apply(Command::SyncNodeTargets(vec![node.clone()]))
        .unwrap();
    let mut passed = evaluation(node.id(), 1_000, true);
    passed.http.final_url = node.url.clone();
    state.apply(Command::RecordNodeEvaluation(passed)).unwrap();

    state
        .apply(Command::SetNodeName {
            node_id,
            name: "  renamed-anchor  ".to_owned(),
        })
        .unwrap();

    let renamed = &state.node_targets[&node.id()];
    assert_eq!(renamed.target.name, "renamed-anchor");
    assert_eq!(renamed.history.len(), 1);
}

#[test]
fn replicated_state_tracks_configured_and_discovered_addresses() {
    use crate::{DiscoverySource, ReachableAddress, ReachableAddressLease};

    let mut state = ApplicationState::default();
    let node_id = id(32);
    let configured = ReachableAddress::parse("up://configured.example:11451").unwrap();
    let discovered = ReachableAddress::parse("up://discovered.example:11451").unwrap();
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id,
            addresses: BTreeSet::from([configured.clone()]),
        })
        .unwrap();
    state
        .apply(Command::RenewReachabilityLeases(vec![
            ReachableAddressLease {
                node_id,
                address: discovered.clone(),
                source: DiscoverySource::Service {
                    url: "https://discovery.example/nodes".to_owned(),
                },
                discovered_at_ms: 10,
                expires_at_ms: 20,
            },
        ]))
        .unwrap();
    state
        .apply(Command::VerifyReachableAddress {
            node_id,
            address: discovered.clone(),
            verified_at_ms: 10,
        })
        .unwrap();

    let reachability = &state.node_reachability[&node_id];
    assert_eq!(
        reachability.configured_reachable_addresses(),
        &BTreeSet::from([configured])
    );
    assert_eq!(
        reachability.verified_addresses().get(&discovered),
        Some(&10)
    );

    state
        .apply(Command::RecordConnectivity {
            leases: Vec::new(),
            verified: Some(BTreeMap::new()),
            checked_at_ms: 20,
            failures: BTreeSet::new(),
        })
        .unwrap();
    assert!(
        !state.node_reachability[&node_id]
            .candidates(Uuid::nil(), 20)
            .contains(&discovered)
    );
}

#[test]
fn renewing_an_active_lease_preserves_verified_reachability() {
    use crate::{DiscoverySource, ReachableAddress, ReachableAddressLease};

    let mut state = ApplicationState::default();
    let node_id = id(33);
    let address = ReachableAddress::parse("up://active.example:11451").unwrap();
    let source = DiscoverySource::Service {
        url: "https://discovery.example/nodes".to_owned(),
    };
    state
        .apply(Command::RenewReachabilityLeases(vec![
            ReachableAddressLease {
                node_id,
                address: address.clone(),
                source: source.clone(),
                discovered_at_ms: 10,
                expires_at_ms: 20,
            },
        ]))
        .unwrap();
    state
        .apply(Command::VerifyReachableAddress {
            node_id,
            address: address.clone(),
            verified_at_ms: 11,
        })
        .unwrap();

    state
        .apply(Command::RenewReachabilityLeases(vec![
            ReachableAddressLease {
                node_id,
                address: address.clone(),
                source,
                discovered_at_ms: 19,
                expires_at_ms: 30,
            },
        ]))
        .unwrap();

    assert!(
        state.node_reachability[&node_id]
            .reachable(Uuid::nil(), 19)
            .contains(&address)
    );
    assert_eq!(
        state.node_reachability[&node_id]
            .verified_addresses()
            .get(&address),
        Some(&11)
    );
}

#[test]
fn renewing_after_a_lease_gap_requires_new_verification() {
    use crate::{DiscoverySource, ReachableAddress, ReachableAddressLease};

    let mut state = ApplicationState::default();
    let node_id = id(34);
    let address = ReachableAddress::parse("up://late.example:11451").unwrap();
    let source = DiscoverySource::Service {
        url: "https://discovery.example/nodes".to_owned(),
    };
    state
        .apply(Command::RenewReachabilityLeases(vec![
            ReachableAddressLease {
                node_id,
                address: address.clone(),
                source: source.clone(),
                discovered_at_ms: 10,
                expires_at_ms: 20,
            },
        ]))
        .unwrap();
    state
        .apply(Command::VerifyReachableAddress {
            node_id,
            address: address.clone(),
            verified_at_ms: 11,
        })
        .unwrap();

    state
        .apply(Command::RenewReachabilityLeases(vec![
            ReachableAddressLease {
                node_id,
                address: address.clone(),
                source,
                discovered_at_ms: 30,
                expires_at_ms: 40,
            },
        ]))
        .unwrap();

    let reachability = &state.node_reachability[&node_id];
    assert!(reachability.candidates(Uuid::nil(), 30).contains(&address));
    assert!(!reachability.reachable(Uuid::nil(), 30).contains(&address));
    state
        .apply(Command::VerifyReachableAddress {
            node_id,
            address: address.clone(),
            verified_at_ms: 12,
        })
        .unwrap();
    assert!(
        !state.node_reachability[&node_id]
            .reachable(Uuid::nil(), 30)
            .contains(&address)
    );
    state
        .apply(Command::VerifyReachableAddress {
            node_id,
            address: address.clone(),
            verified_at_ms: 31,
        })
        .unwrap();
    assert!(
        state.node_reachability[&node_id]
            .reachable(Uuid::nil(), 31)
            .contains(&address)
    );
}

#[test]
fn connectivity_results_verify_only_directly_reached_addresses() {
    use crate::{DirectedRoute, DiscoverySource, ReachableAddress, ReachableAddressLease};

    let mut state = ApplicationState::default();
    let source = id(33);
    let destination = id(34);
    let configured = ReachableAddress::parse("up://configured.example:11451").unwrap();
    let fallback = ReachableAddress::parse("up://fallback.example:11451").unwrap();
    let reverse = ReachableAddress::parse("up://reverse.example:11451").unwrap();
    let failed = DirectedRoute {
        source: destination,
        destination: source,
    };
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id: destination,
            addresses: BTreeSet::from([configured.clone()]),
        })
        .unwrap();

    state
        .apply(Command::RecordConnectivity {
            leases: vec![
                ReachableAddressLease {
                    node_id: destination,
                    address: configured.clone(),
                    source: DiscoverySource::Node {
                        discovering_node_id: source,
                    },
                    discovered_at_ms: 10,
                    expires_at_ms: 50,
                },
                ReachableAddressLease {
                    node_id: destination,
                    address: fallback.clone(),
                    source: DiscoverySource::Node {
                        discovering_node_id: source,
                    },
                    discovered_at_ms: 10,
                    expires_at_ms: 50,
                },
                ReachableAddressLease {
                    node_id: source,
                    address: reverse.clone(),
                    source: DiscoverySource::Node {
                        discovering_node_id: destination,
                    },
                    discovered_at_ms: 10,
                    expires_at_ms: 50,
                },
            ],
            verified: Some(BTreeMap::from([(
                destination,
                BTreeSet::from([configured, fallback.clone()]),
            )])),
            checked_at_ms: 10,
            failures: BTreeSet::from([failed]),
        })
        .unwrap();

    assert_eq!(
        state.node_reachability[&destination]
            .verified_addresses()
            .get(&fallback),
        Some(&10)
    );
    let reverse_reachability = &state.node_reachability[&source];
    assert!(
        reverse_reachability
            .candidates(destination, 10)
            .contains(&reverse)
    );
    assert!(
        !reverse_reachability
            .reachable(destination, 10)
            .contains(&reverse)
    );
    assert!(state.connectivity_failures.is_empty());

    let confirmation = Command::RecordConnectivity {
        leases: vec![ReachableAddressLease {
            node_id: source,
            address: reverse.clone(),
            source: DiscoverySource::Node {
                discovering_node_id: destination,
            },
            discovered_at_ms: 20,
            expires_at_ms: 60,
        }],
        verified: Some(BTreeMap::from([(
            source,
            BTreeSet::from([reverse.clone()]),
        )])),
        checked_at_ms: 20,
        failures: BTreeSet::from([failed]),
    };
    state.apply(confirmation.clone()).unwrap();
    state.apply(confirmation).unwrap();

    assert!(
        state.node_reachability[&source]
            .reachable(destination, 20)
            .contains(&reverse)
    );
    assert_eq!(state.connectivity_failures, BTreeSet::from([failed]));
}

#[test]
fn legacy_connectivity_without_verification_evidence_verifies_all_leases() {
    use crate::{DiscoverySource, ReachableAddress, ReachableAddressLease};

    let node_id = id(38);
    let observer = id(39);
    let address = ReachableAddress::parse("up://legacy.example:11451").unwrap();
    let command = Command::RecordConnectivity {
        leases: vec![ReachableAddressLease {
            node_id,
            address: address.clone(),
            source: DiscoverySource::Node {
                discovering_node_id: observer,
            },
            discovered_at_ms: 10,
            expires_at_ms: 50,
        }],
        verified: Some(BTreeMap::new()),
        checked_at_ms: 10,
        failures: BTreeSet::new(),
    };
    let mut encoded = serde_json::to_value(command).unwrap();
    encoded["RecordConnectivity"]
        .as_object_mut()
        .unwrap()
        .remove("verified");
    let decoded: Command = serde_json::from_value(encoded).unwrap();
    let Command::RecordConnectivity { verified, .. } = &decoded else {
        panic!("the decoded command should retain its variant");
    };
    assert_eq!(verified, &None);

    let mut state = ApplicationState::default();
    state.apply(decoded).unwrap();

    assert_eq!(
        state.node_reachability[&node_id]
            .verified_addresses()
            .get(&address),
        Some(&10)
    );
}

#[test]
fn removing_a_member_clears_its_reachability_state() {
    use crate::{DirectedRoute, ReachableAddress};

    let mut state = ApplicationState::default();
    let removed = id(35);
    let retained = id(36);
    let removed_route = DirectedRoute {
        source: retained,
        destination: removed,
    };
    let retained_route = DirectedRoute {
        source: retained,
        destination: id(37),
    };
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id: removed,
            addresses: BTreeSet::from([
                ReachableAddress::parse("up://removed.example:11451").unwrap()
            ]),
        })
        .unwrap();
    for _ in 0..3 {
        state
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(BTreeMap::new()),
                checked_at_ms: 10,
                failures: BTreeSet::from([removed_route, retained_route]),
            })
            .unwrap();
    }

    assert!(state.retain_member_reachability(&BTreeSet::from([retained, id(37),])));
    assert!(!state.node_reachability.contains_key(&removed));
    assert_eq!(
        state.connectivity_failures,
        BTreeSet::from([retained_route])
    );
    assert_eq!(
        state.connectivity_failure_counts,
        BTreeMap::from([(retained_route, 3)])
    );
}
