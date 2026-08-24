use super::*;
use crate::domain::{Command, NotificationChannel, NotificationChannelId, NotificationChannelKind};
use crate::{DiscoverySource, ReachableAddress};

fn route() -> DirectedRoute {
    DirectedRoute {
        source: Uuid::from_u128(1),
        destination: Uuid::from_u128(2),
    }
}

fn configure_default_channel(state: &mut ApplicationState) -> NotificationChannelId {
    let channel_id = NotificationChannelId(Uuid::from_u128(3));
    state
        .apply(Command::CreateNotificationChannel {
            channel: NotificationChannel {
                id: channel_id,
                name: "Connectivity".to_owned(),
                kind: NotificationChannelKind::Webhook {
                    url: Url::parse("https://example.com/connectivity").unwrap(),
                    headers: Default::default(),
                },
            },
            generated_secret: None,
            is_default: true,
        })
        .unwrap();
    channel_id
}

fn record(state: &mut ApplicationState, checked_at_ms: u64, failed: bool) {
    let failures = if failed {
        BTreeSet::from([route()])
    } else {
        BTreeSet::new()
    };
    record_failures(state, checked_at_ms, failures);
}

fn record_failures(
    state: &mut ApplicationState,
    checked_at_ms: u64,
    failures: BTreeSet<DirectedRoute>,
) {
    state
        .apply(Command::RecordConnectivity {
            leases: Vec::new(),
            verified: Some(BTreeMap::new()),
            checked_at_ms,
            failures,
        })
        .unwrap();
}

#[test]
fn reverse_observation_after_a_lease_gap_does_not_revive_verification() {
    let mut state = ApplicationState::default();
    let node_id = Uuid::from_u128(10);
    let direct_observer = Uuid::from_u128(11);
    let reverse_observer = Uuid::from_u128(12);
    let address = ReachableAddress::parse("up://observed.example:11451").unwrap();
    state
        .apply(Command::RecordConnectivity {
            leases: vec![ReachableAddressLease {
                node_id,
                address: address.clone(),
                source: DiscoverySource::Node {
                    discovering_node_id: direct_observer,
                },
                discovered_at_ms: 10,
                expires_at_ms: 20,
            }],
            verified: Some(BTreeMap::from([(
                node_id,
                BTreeSet::from([address.clone()]),
            )])),
            checked_at_ms: 10,
            failures: BTreeSet::new(),
        })
        .unwrap();
    assert!(
        state.node_reachability[&node_id]
            .reachable(direct_observer, 10)
            .contains(&address)
    );

    state
        .apply(Command::RecordConnectivity {
            leases: vec![ReachableAddressLease {
                node_id,
                address: address.clone(),
                source: DiscoverySource::Node {
                    discovering_node_id: reverse_observer,
                },
                discovered_at_ms: 20,
                expires_at_ms: 40,
            }],
            verified: Some(BTreeMap::new()),
            checked_at_ms: 20,
            failures: BTreeSet::new(),
        })
        .unwrap();

    let reachability = &state.node_reachability[&node_id];
    assert!(
        reachability
            .candidates(reverse_observer, 20)
            .contains(&address)
    );
    assert!(
        !reachability
            .reachable(reverse_observer, 20)
            .contains(&address)
    );
    assert!(!reachability.verified_addresses().contains_key(&address));
}

#[test]
fn expired_reachability_lease_requires_a_cleanup_record() {
    let mut state = ApplicationState::default();
    let node_id = Uuid::from_u128(10);
    state
        .apply(Command::RenewReachabilityLeases(vec![
            ReachableAddressLease {
                node_id,
                address: ReachableAddress::parse("up://expired.example:11451").unwrap(),
                source: DiscoverySource::Service {
                    url: "https://discovery.example/nodes".to_owned(),
                },
                discovered_at_ms: 10,
                expires_at_ms: 20,
            },
        ]))
        .unwrap();

    assert!(!state.connectivity_scan_requires_record(&BTreeSet::new(), 19));
    assert!(state.connectivity_scan_requires_record(&BTreeSet::new(), 20));
    record(&mut state, 20, false);
    assert!(!state.connectivity_scan_requires_record(&BTreeSet::new(), 20));
}

#[test]
fn repeated_route_loss_and_recovery_deliver_to_a_non_default_channel() {
    let mut state = ApplicationState::default();
    let channel_id = configure_default_channel(&mut state);
    state.default_notification_channels.clear();

    record(&mut state, 1_000, true);
    record(&mut state, 2_000, true);
    assert!(state.connectivity_failures.is_empty());
    assert!(state.alerts.is_empty());

    record(&mut state, 3_000, true);
    assert_eq!(state.connectivity_failures, BTreeSet::from([route()]));
    assert_eq!(state.availability_transitions.len(), 1);
    assert_eq!(state.alerts.len(), 1);
    let down = state.alerts.keys().next().unwrap();
    assert_eq!(down.channel_id, channel_id);
    assert_eq!(down.kind, AlertKind::Down);
    assert_eq!(down.evaluation_scheduled_at_ms, 3_000);

    record(&mut state, 4_000, false);
    record(&mut state, 5_000, false);
    assert_eq!(state.connectivity_failures, BTreeSet::from([route()]));
    assert_eq!(state.availability_transitions.len(), 1);

    record(&mut state, 6_000, false);
    assert!(state.connectivity_failures.is_empty());
    assert_eq!(state.availability_transitions.len(), 2);
    assert_eq!(state.alerts.len(), 2);
    let recovered = state
        .alerts
        .keys()
        .find(|id| id.channel_id == channel_id && id.kind == AlertKind::Recovered)
        .unwrap();
    assert_eq!(recovered.evaluation_scheduled_at_ms, 6_000);
    assert!(state.alerts.keys().all(|id| id.channel_id == channel_id));
}

#[test]
fn removing_the_failed_member_recovers_after_three_successful_scans() {
    let mut state = ApplicationState::default();
    let channel_id = configure_default_channel(&mut state);

    record(&mut state, 1_000, true);
    record(&mut state, 2_000, true);
    record(&mut state, 3_000, true);
    assert!(state.connectivity_degraded());

    assert!(state.retain_member_reachability(&BTreeSet::from([route().source])));
    assert!(state.connectivity_failures.is_empty());
    assert!(state.connectivity_degraded());
    assert!(state.connectivity_scan_requires_record(&BTreeSet::new(), 4_000));

    record(&mut state, 4_000, false);
    record(&mut state, 5_000, false);
    assert!(state.connectivity_degraded());
    assert_eq!(state.availability_transitions.len(), 1);

    record(&mut state, 6_000, false);
    assert!(!state.connectivity_degraded());
    assert_eq!(state.availability_transitions.len(), 2);
    assert!(state.alerts.keys().any(|id| {
        id.channel_id == channel_id
            && id.kind == AlertKind::Recovered
            && id.evaluation_scheduled_at_ms == 6_000
    }));
}

#[test]
fn same_millisecond_loss_and_recovery_keep_both_alerts() {
    let mut state = ApplicationState::default();
    let channel_id = configure_default_channel(&mut state);

    record(&mut state, 1_000, true);
    record(&mut state, 1_000, true);
    record(&mut state, 1_000, true);
    record(&mut state, 1_000, false);
    record(&mut state, 1_000, false);
    record(&mut state, 1_000, false);

    assert_eq!(state.availability_transitions.len(), 2);
    assert_eq!(state.alerts.len(), 2);
    assert_eq!(
        state
            .availability_transitions
            .keys()
            .map(|id| id.scheduled_at_ms)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([1_000, 1_001])
    );
    assert_eq!(
        state
            .availability_transitions
            .values()
            .map(|transition| transition.kind)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([AlertKind::Down, AlertKind::Recovered])
    );
    assert!(
        state
            .availability_transitions
            .iter()
            .all(|(id, transition)| { transition.evaluation.recorded_at_ms == id.scheduled_at_ms })
    );
    assert!(state.alerts.keys().all(|id| id.channel_id == channel_id));
    assert_eq!(
        state
            .alerts
            .keys()
            .map(|id| id.kind)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([AlertKind::Down, AlertKind::Recovered])
    );
}

#[test]
fn clock_rollback_keeps_connectivity_transitions_in_state_order() {
    let mut state = ApplicationState::default();

    record(&mut state, 2_000, true);
    record(&mut state, 2_000, true);
    record(&mut state, 2_000, true);
    record(&mut state, 1_000, false);
    record(&mut state, 1_000, false);
    record(&mut state, 1_000, false);

    assert_eq!(
        state
            .availability_transitions
            .iter()
            .map(|(id, transition)| (id.scheduled_at_ms, transition.kind))
            .collect::<Vec<_>>(),
        vec![(2_000, AlertKind::Down), (2_001, AlertKind::Recovered)]
    );
}

#[test]
fn connectivity_availability_transitions_are_recorded_without_channels() {
    let mut state = ApplicationState::default();

    record(&mut state, 1_000, true);
    record(&mut state, 2_000, true);
    record(&mut state, 3_000, true);
    record(&mut state, 4_000, false);
    record(&mut state, 5_000, false);
    record(&mut state, 6_000, false);

    assert!(state.connectivity_failures.is_empty());
    assert!(state.alerts.is_empty());
    assert_eq!(state.availability_transitions.len(), 2);
    assert!(
        state
            .availability_transitions
            .values()
            .any(|transition| transition.kind == AlertKind::Down)
    );
    assert!(
        state
            .availability_transitions
            .values()
            .any(|transition| transition.kind == AlertKind::Recovered)
    );
    assert!(
        state
            .availability_transitions
            .values()
            .all(|transition| transition.target_name == "Cluster connectivity")
    );
}

#[test]
fn a_changed_failure_set_restarts_stabilization() {
    let mut state = ApplicationState::default();
    let other = DirectedRoute {
        source: Uuid::from_u128(1),
        destination: Uuid::from_u128(3),
    };

    record(&mut state, 1_000, true);
    record(&mut state, 2_000, true);
    record_failures(&mut state, 3_000, BTreeSet::from([route(), other]));
    record(&mut state, 4_000, true);
    record(&mut state, 5_000, true);
    assert!(state.connectivity_failures.is_empty());

    record(&mut state, 6_000, true);
    assert_eq!(state.connectivity_failures, BTreeSet::from([route()]));
}

#[test]
fn isolated_route_failures_do_not_degrade_the_cluster() {
    let mut state = ApplicationState::default();

    record(&mut state, 1_000, true);
    record(&mut state, 2_000, false);
    record(&mut state, 3_000, true);

    assert!(state.connectivity_failures.is_empty());
    assert!(state.alerts.is_empty());
}
