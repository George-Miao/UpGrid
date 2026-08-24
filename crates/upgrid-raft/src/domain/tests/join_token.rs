use super::evaluation::id;
use super::*;
use crate::{DirectedRoute, NodeReachability, ReachableAddress};

#[test]
fn join_token_is_reusable_until_expiry_or_revocation() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([7; 32]);
    state
        .apply(Command::PutJoinToken {
            hash,
            expires_at_ms: 2_000,
        })
        .unwrap();

    assert_eq!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 1_000,
            })
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert_eq!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 1_001,
            })
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 2_001,
            })
            .is_err()
    );
    assert_eq!(
        state.apply(Command::RevokeJoinToken(hash)).unwrap(),
        CommandResult::JoinTokenRevoked
    );
    assert!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 1_000,
            })
            .is_err()
    );
}

#[test]
fn revocation_stops_a_reserved_token_from_being_restored() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([14; 32]);
    let reservation_id = id(18);
    state
        .apply(Command::PutLimitedJoinToken {
            hash,
            expires_at_ms: 2_000,
            uses: 1,
        })
        .unwrap();
    state
        .apply(Command::ReserveJoinToken {
            hash,
            reservation_id,
            reservation_operation_id: Uuid::nil(),
            reserved_at_ms: 1_000,
            readmission: false,
        })
        .unwrap();

    assert_eq!(
        state.apply(Command::RevokeJoinToken(hash)).unwrap(),
        CommandResult::JoinTokenRevoked
    );
    state
        .apply(Command::CompleteJoinTokenReservation {
            reservation_id,
            reservation_operation_id: Uuid::nil(),
            accepted: false,
            completed_at_ms: 1_100,
        })
        .unwrap();

    assert!(!state.join_tokens.contains_key(&hash));
    assert!(!state.join_token_uses.contains_key(&hash));
    assert!(!state.join_token_reservations.contains_key(&reservation_id));
}

#[test]
fn reusable_join_token_authorizes_different_nodes() {
    let mut state = ApplicationState::default();
    let token = "reusable-join-token";
    let hash = crate::token::hash_join_token(token);
    let first_node = id(1);
    let second_node = id(2);
    state
        .apply(Command::PutJoinToken {
            hash,
            expires_at_ms: 2_000,
        })
        .unwrap();

    let authorize = || Command::AuthorizeJoinToken {
        hash,
        authorized_at_ms: 1_000,
    };
    let first_operation = first_node;
    assert_eq!(
        state
            .apply_operation(first_operation, 1_000, authorize())
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert_eq!(
        state
            .apply_operation(first_operation, 1_001, authorize())
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert_eq!(
        state
            .apply_operation(second_node, 1_001, authorize(),)
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
}

#[test]
fn limited_join_token_is_removed_after_its_last_use() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([8; 32]);
    state
        .apply(Command::PutLimitedJoinToken {
            hash,
            expires_at_ms: 2_000,
            uses: 2,
        })
        .unwrap();

    let authorize = || Command::AuthorizeJoinToken {
        hash,
        authorized_at_ms: 1_000,
    };
    assert_eq!(
        state.apply(authorize()).unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert_eq!(state.join_token_uses.get(&hash), Some(&1));
    assert_eq!(
        state.apply(authorize()).unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert!(!state.join_tokens.contains_key(&hash));
    assert!(state.apply(authorize()).is_err());
}

#[test]
fn join_token_reservation_follows_admission_outcome() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([9; 32]);
    let reservation_id = id(9);
    state
        .apply(Command::PutLimitedJoinToken {
            hash,
            expires_at_ms: 2_000,
            uses: 1,
        })
        .unwrap();

    let reserve = || Command::ReserveJoinToken {
        hash,
        reservation_id,
        reservation_operation_id: Uuid::nil(),
        reserved_at_ms: 1_000,
        readmission: false,
    };
    assert_eq!(
        state.apply(reserve()).unwrap(),
        CommandResult::JoinTokenReserved
    );
    assert!(!state.join_tokens.contains_key(&hash));
    assert_eq!(
        state
            .apply(Command::CompleteJoinTokenReservation {
                reservation_id,
                reservation_operation_id: Uuid::nil(),
                accepted: false,
                completed_at_ms: 1_100,
            })
            .unwrap(),
        CommandResult::JoinTokenReservationCompleted
    );
    assert_eq!(state.join_tokens.get(&hash), Some(&2_000));
    assert_eq!(state.join_token_uses.get(&hash), Some(&1));

    state.apply(reserve()).unwrap();
    state
        .apply(Command::CompleteJoinTokenReservation {
            reservation_id,
            reservation_operation_id: Uuid::nil(),
            accepted: true,
            completed_at_ms: 1_200,
        })
        .unwrap();
    assert!(!state.join_tokens.contains_key(&hash));
    assert!(!state.join_token_reservations.contains_key(&reservation_id));
}

#[test]
fn aborted_pending_join_removes_staged_reachability() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([10; 32]);
    let pending_node = id(10);
    let existing_node = id(11);
    let other_node = id(12);
    let address = ReachableAddress::parse("up://pending.example:11451").unwrap();
    state
        .apply(Command::PutLimitedJoinToken {
            hash,
            expires_at_ms: 2_000,
            uses: 1,
        })
        .unwrap();
    state
        .apply(Command::ReserveJoinToken {
            hash,
            reservation_id: pending_node,
            reservation_operation_id: Uuid::nil(),
            reserved_at_ms: 1_000,
            readmission: false,
        })
        .unwrap();
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id: pending_node,
            addresses: BTreeSet::from([address]),
        })
        .unwrap();
    let unrelated_failure = DirectedRoute {
        source: existing_node,
        destination: other_node,
    };
    let failures = BTreeSet::from([
        DirectedRoute {
            source: existing_node,
            destination: pending_node,
        },
        unrelated_failure,
    ]);
    for checked_at_ms in [1_050, 1_060, 1_070] {
        state
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(BTreeMap::new()),
                checked_at_ms,
                failures: failures.clone(),
            })
            .unwrap();
    }

    assert_eq!(
        state
            .apply(Command::AbortPendingJoin {
                reservation_id: pending_node,
                reservation_operation_id: Uuid::nil(),
                completed_at_ms: 1_100,
            })
            .unwrap(),
        CommandResult::JoinTokenReservationCompleted
    );

    assert!(!state.node_reachability.contains_key(&pending_node));
    assert_eq!(
        state.connectivity_failures,
        BTreeSet::from([unrelated_failure])
    );
    assert_eq!(state.join_tokens.get(&hash), Some(&2_000));
    assert_eq!(state.join_token_uses.get(&hash), Some(&1));
    assert!(!state.join_token_reservations.contains_key(&pending_node));
}

#[test]
fn aborted_readmission_restores_reachability_without_reverting_connectivity() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([13; 32]);
    let existing_node = id(15);
    let other_node = id(16);
    let unrelated_source = id(17);
    let old_reachability = NodeReachability::configured(BTreeSet::from([ReachableAddress::parse(
        "up://old.example:11451",
    )
    .unwrap()]));
    let old_failure = DirectedRoute {
        source: existing_node,
        destination: other_node,
    };
    let unrelated_failure = DirectedRoute {
        source: unrelated_source,
        destination: other_node,
    };
    state
        .node_reachability
        .insert(existing_node, old_reachability.clone());
    state.connectivity_failures.insert(old_failure);
    state.connectivity_failure_counts.insert(old_failure, 2);
    state
        .apply(Command::PutLimitedJoinToken {
            hash,
            expires_at_ms: 2_000,
            uses: 1,
        })
        .unwrap();
    state
        .apply(Command::ReserveJoinToken {
            hash,
            reservation_id: existing_node,
            reservation_operation_id: Uuid::nil(),
            reserved_at_ms: 1_000,
            readmission: true,
        })
        .unwrap();
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id: existing_node,
            addresses: BTreeSet::from([
                ReachableAddress::parse("up://unreachable.example:11451").unwrap()
            ]),
        })
        .unwrap();
    state
        .apply(Command::RecordConnectivity {
            leases: Vec::new(),
            verified: Some(BTreeMap::new()),
            checked_at_ms: 1_050,
            failures: BTreeSet::from([
                DirectedRoute {
                    source: other_node,
                    destination: existing_node,
                },
                unrelated_failure,
            ]),
        })
        .unwrap();
    state
        .apply(Command::RecordConnectivity {
            leases: Vec::new(),
            verified: Some(BTreeMap::new()),
            checked_at_ms: 1_060,
            failures: BTreeSet::from([
                DirectedRoute {
                    source: other_node,
                    destination: existing_node,
                },
                unrelated_failure,
            ]),
        })
        .unwrap();
    state
        .apply(Command::RecordConnectivity {
            leases: Vec::new(),
            verified: Some(BTreeMap::new()),
            checked_at_ms: 1_070,
            failures: BTreeSet::from([
                DirectedRoute {
                    source: other_node,
                    destination: existing_node,
                },
                unrelated_failure,
            ]),
        })
        .unwrap();

    state
        .apply(Command::AbortPendingReadmission {
            reservation_id: existing_node,
            reservation_operation_id: Uuid::nil(),
            completed_at_ms: 1_100,
        })
        .unwrap();

    assert_eq!(
        state.node_reachability.get(&existing_node),
        Some(&old_reachability)
    );
    let replacement_failure = DirectedRoute {
        source: other_node,
        destination: existing_node,
    };
    assert_eq!(
        state.connectivity_failures,
        BTreeSet::from([replacement_failure, unrelated_failure])
    );
    assert_eq!(
        state.connectivity_failure_counts,
        BTreeMap::from([(replacement_failure, 3), (unrelated_failure, 3)])
    );
    assert_eq!(state.join_token_uses.get(&hash), Some(&1));
    assert!(!state.join_token_reservations.contains_key(&existing_node));
}

#[test]
fn abandoned_reservation_does_not_outlive_its_token_or_block_a_new_one() {
    let mut state = ApplicationState::default();
    let expired_hash = JoinTokenHash([11; 32]);
    let replacement_hash = JoinTokenHash([12; 32]);
    let reservation_id = id(13);
    let replacement_reservation_id = id(14);
    state
        .apply(Command::PutLimitedJoinToken {
            hash: expired_hash,
            expires_at_ms: 2_000,
            uses: 1,
        })
        .unwrap();
    state
        .apply(Command::ReserveJoinToken {
            hash: expired_hash,
            reservation_id,
            reservation_operation_id: Uuid::nil(),
            reserved_at_ms: 1_000,
            readmission: false,
        })
        .unwrap();
    assert!(
        state
            .apply(Command::ReserveJoinToken {
                hash: expired_hash,
                reservation_id,
                reservation_operation_id: Uuid::nil(),
                reserved_at_ms: 2_001,
                readmission: false,
            })
            .is_err()
    );
    assert!(!state.join_token_reservations.contains_key(&reservation_id));
    state
        .apply(Command::AbortPendingJoin {
            reservation_id,
            reservation_operation_id: Uuid::nil(),
            completed_at_ms: 2_001,
        })
        .unwrap();

    state
        .apply(Command::PutLimitedJoinToken {
            hash: replacement_hash,
            expires_at_ms: 100_000,
            uses: 1,
        })
        .unwrap();
    state
        .apply(Command::ReserveJoinToken {
            hash: replacement_hash,
            reservation_id,
            reservation_operation_id: Uuid::nil(),
            reserved_at_ms: 3_000,
            readmission: false,
        })
        .unwrap();
    assert_eq!(
        state.apply(Command::ReserveJoinToken {
            hash: replacement_hash,
            reservation_id: replacement_reservation_id,
            reservation_operation_id: Uuid::nil(),
            reserved_at_ms: 33_001,
            readmission: false,
        }),
        Err(DomainError::InvalidJoinToken)
    );
    assert!(state.join_token_reservations.contains_key(&reservation_id));
    state
        .apply(Command::AbortPendingJoin {
            reservation_id,
            reservation_operation_id: Uuid::nil(),
            completed_at_ms: 33_001,
        })
        .unwrap();
    assert_eq!(
        state
            .apply(Command::ReserveJoinToken {
                hash: replacement_hash,
                reservation_id: replacement_reservation_id,
                reservation_operation_id: Uuid::nil(),
                reserved_at_ms: 33_002,
                readmission: false,
            })
            .unwrap(),
        CommandResult::JoinTokenReserved
    );
}
