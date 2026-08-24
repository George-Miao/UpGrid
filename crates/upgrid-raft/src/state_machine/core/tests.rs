use std::collections::{BTreeMap, BTreeSet};

use openraft::alias::StoredMembershipOf;
use serde::Serialize;
use uuid::Uuid;

use super::{StateMachineData, apply_membership, apply_request, migrate_legacy_membership};
use crate::domain::{ApplicationState, Command, CommandResult, DomainError};
use crate::raft::{NodeIdentity, Req, TC};
use crate::{DirectedRoute, NodeReachability, ReachableAddress};

#[derive(Serialize)]
struct LegacyAddress<'a> {
    host: &'a str,
    port: u16,
}

#[test]
fn legacy_membership_seeds_reachability_then_becomes_identity_only() {
    let node_id = Uuid::from_u128(7);
    let encoded = postcard::to_stdvec(&LegacyAddress {
        host: "legacy.example",
        port: 11451,
    })
    .unwrap();
    let node = postcard::from_bytes::<NodeIdentity>(&encoded).unwrap();
    let mut membership = StoredMembershipOf::<TC>::new(
        None,
        openraft::Membership::from(BTreeMap::from([(node_id, node)])),
    );
    let mut application = ApplicationState::default();

    assert!(migrate_legacy_membership(&mut application, &mut membership));
    assert_eq!(
        membership.get_node(&node_id),
        Some(&NodeIdentity::default())
    );
    assert!(
        application
            .node_reachability
            .get(&node_id)
            .unwrap()
            .configured_reachable_addresses()
            .contains(&ReachableAddress::parse("up://legacy.example:11451").unwrap())
    );
}

#[test]
fn membership_application_removes_absent_node_reachability() {
    let removed = Uuid::from_u128(8);
    let retained = Uuid::from_u128(9);
    let destination = Uuid::from_u128(10);
    let removed_route = DirectedRoute {
        source: retained,
        destination: removed,
    };
    let retained_route = DirectedRoute {
        source: retained,
        destination,
    };
    let mut state = StateMachineData::default();
    state
        .application
        .node_reachability
        .insert(removed, NodeReachability::default());
    for checked_at_ms in [10, 20] {
        state
            .application
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(BTreeMap::new()),
                checked_at_ms,
                failures: BTreeSet::from([removed_route, retained_route]),
            })
            .unwrap();
    }
    let membership = StoredMembershipOf::<TC>::new(
        None,
        openraft::Membership::from(BTreeMap::from([
            (retained, NodeIdentity::default()),
            (destination, NodeIdentity::default()),
        ])),
    );

    apply_membership(&mut state, membership);

    assert!(!state.application.node_reachability.contains_key(&removed));
    assert_eq!(
        state.application.connectivity_route_state(retained),
        (BTreeSet::new(), BTreeMap::new())
    );
}

#[test]
fn membership_addition_restarts_connectivity_stabilization() {
    let first = Uuid::from_u128(20);
    let second = Uuid::from_u128(21);
    let added = Uuid::from_u128(22);
    let route = DirectedRoute {
        source: first,
        destination: second,
    };
    let mut state = StateMachineData::default();
    let initial = StoredMembershipOf::<TC>::new(
        None,
        openraft::Membership::from(BTreeMap::from([
            (first, NodeIdentity::default()),
            (second, NodeIdentity::default()),
        ])),
    );
    apply_membership(&mut state, initial);
    for checked_at_ms in 1..=3 {
        state
            .application
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(BTreeMap::new()),
                checked_at_ms,
                failures: BTreeSet::from([route]),
            })
            .unwrap();
    }
    for checked_at_ms in 4..=5 {
        state
            .application
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(BTreeMap::new()),
                checked_at_ms,
                failures: BTreeSet::new(),
            })
            .unwrap();
    }
    assert_eq!(
        state.application.connectivity_route_state(first).0,
        BTreeSet::from([route])
    );

    let expanded = StoredMembershipOf::<TC>::new(
        None,
        openraft::Membership::from(BTreeMap::from([
            (first, NodeIdentity::default()),
            (second, NodeIdentity::default()),
            (added, NodeIdentity::default()),
        ])),
    );
    apply_membership(&mut state, expanded);
    for checked_at_ms in 6..=7 {
        state
            .application
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(BTreeMap::new()),
                checked_at_ms,
                failures: BTreeSet::new(),
            })
            .unwrap();
    }
    assert_eq!(
        state.application.connectivity_route_state(first).0,
        BTreeSet::from([route])
    );

    state
        .application
        .apply(Command::RecordConnectivity {
            leases: Vec::new(),
            verified: Some(BTreeMap::new()),
            checked_at_ms: 8,
            failures: BTreeSet::new(),
        })
        .unwrap();
    assert!(
        state
            .application
            .connectivity_route_state(first)
            .0
            .is_empty()
    );
}

#[test]
fn reachability_replays_keep_the_first_validated_result() {
    let node_id = Uuid::from_u128(30);
    let address = ReachableAddress::parse("up://node.example:11451").unwrap();
    let command = Command::ReplaceConfiguredReachableAddresses {
        node_id,
        addresses: BTreeSet::from([address]),
    };
    let rejected = Req {
        operation_id: Uuid::from_u128(31),
        submitted_at_ms: 1,
        command: command.clone(),
    };
    let mut state = StateMachineData::default();
    assert_eq!(
        apply_request(&mut state, rejected.clone()),
        Err(DomainError::NodeNotInMembership(node_id))
    );
    let membership = StoredMembershipOf::<TC>::new(
        None,
        openraft::Membership::from(BTreeMap::from([(node_id, NodeIdentity::default())])),
    );
    apply_membership(&mut state, membership);
    assert_eq!(
        apply_request(&mut state, rejected),
        Err(DomainError::NodeNotInMembership(node_id))
    );

    let accepted = Req {
        operation_id: Uuid::from_u128(32),
        submitted_at_ms: 2,
        command,
    };
    assert_eq!(
        apply_request(&mut state, accepted.clone()),
        Ok(CommandResult::ConfiguredReachableAddressesReplaced(node_id))
    );
    apply_membership(
        &mut state,
        StoredMembershipOf::<TC>::new(None, openraft::Membership::from(BTreeMap::new())),
    );
    assert_eq!(
        apply_request(&mut state, accepted),
        Ok(CommandResult::ConfiguredReachableAddressesReplaced(node_id))
    );
}
