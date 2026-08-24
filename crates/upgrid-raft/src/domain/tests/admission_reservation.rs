use super::evaluation::id;
use super::*;
use crate::{NodeReachability, ReachableAddress};

#[test]
fn stale_cleanup_does_not_settle_a_replacement_reservation() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([41; 32]);
    let reservation_id = id(42);
    let first_operation = id(43);
    let replacement_operation = id(44);
    state
        .apply(Command::PutLimitedJoinToken {
            hash,
            expires_at_ms: 100_000,
            uses: 2,
        })
        .unwrap();
    state
        .apply_operation(
            first_operation,
            1_000,
            Command::ReserveJoinToken {
                hash,
                reservation_id,
                reservation_operation_id: first_operation,
                reserved_at_ms: 1_000,
                readmission: false,
            },
        )
        .unwrap();
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id: reservation_id,
            addresses: BTreeSet::from([
                ReachableAddress::parse("up://staged.example:11451").unwrap()
            ]),
        })
        .unwrap();
    state
        .apply_operation(
            replacement_operation,
            31_001,
            Command::ReserveJoinToken {
                hash,
                reservation_id,
                reservation_operation_id: replacement_operation,
                reserved_at_ms: 31_001,
                readmission: false,
            },
        )
        .unwrap();
    assert!(!state.node_reachability.contains_key(&reservation_id));

    state
        .apply_operation(
            id(45),
            31_002,
            Command::AbortPendingJoin {
                reservation_id,
                reservation_operation_id: first_operation,
                completed_at_ms: 31_002,
            },
        )
        .unwrap();

    assert_eq!(
        state
            .join_token_reservations
            .get(&reservation_id)
            .map(|reservation| reservation.operation_id),
        Some(replacement_operation)
    );
    assert_eq!(state.join_token_uses.get(&hash), Some(&1));

    state
        .apply_operation(
            id(46),
            31_003,
            Command::AbortPendingJoin {
                reservation_id,
                reservation_operation_id: replacement_operation,
                completed_at_ms: 31_003,
            },
        )
        .unwrap();

    assert!(!state.join_token_reservations.contains_key(&reservation_id));
    assert_eq!(state.join_token_uses.get(&hash), Some(&2));
}

#[test]
fn replacement_readmission_inherits_the_original_rollback() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([47; 32]);
    let reservation_id = id(48);
    let first_operation = id(49);
    let replacement_operation = id(50);
    let old_address = ReachableAddress::parse("up://old.example:11451").unwrap();
    let staged_address = ReachableAddress::parse("up://staged.example:11451").unwrap();
    let second_address = ReachableAddress::parse("up://second.example:11451").unwrap();
    let old_reachability = NodeReachability::configured(BTreeSet::from([old_address.clone()]));
    state
        .node_reachability
        .insert(reservation_id, old_reachability.clone());
    state
        .apply(Command::PutLimitedJoinToken {
            hash,
            expires_at_ms: 100_000,
            uses: 2,
        })
        .unwrap();
    state
        .apply_operation(
            first_operation,
            1_000,
            Command::ReserveJoinToken {
                hash,
                reservation_id,
                reservation_operation_id: first_operation,
                reserved_at_ms: 1_000,
                readmission: true,
            },
        )
        .unwrap();
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id: reservation_id,
            addresses: BTreeSet::from([staged_address.clone()]),
        })
        .unwrap();
    state
        .apply_operation(
            replacement_operation,
            31_001,
            Command::ReserveJoinToken {
                hash,
                reservation_id,
                reservation_operation_id: replacement_operation,
                reserved_at_ms: 31_001,
                readmission: true,
            },
        )
        .unwrap();
    assert_eq!(
        state.node_reachability[&reservation_id].configured_reachable_addresses(),
        &BTreeSet::from([old_address.clone()])
    );
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id: reservation_id,
            addresses: BTreeSet::from([second_address]),
        })
        .unwrap();

    state
        .apply_operation(
            id(51),
            31_002,
            Command::AbortPendingReadmission {
                reservation_id,
                reservation_operation_id: replacement_operation,
                completed_at_ms: 31_002,
            },
        )
        .unwrap();

    assert_eq!(
        state.node_reachability[&reservation_id].configured_reachable_addresses(),
        &BTreeSet::from([old_address])
    );
}
#[test]
fn expired_reservation_ids_exclude_the_live_boundary() {
    let mut state = ApplicationState::default();
    let first = id(91);
    let second = id(92);
    for (hash, reservation_id, reserved_at_ms) in [
        (JoinTokenHash([1; 32]), first, 100),
        (JoinTokenHash([2; 32]), second, 200),
    ] {
        state
            .apply(Command::PutJoinToken {
                hash,
                expires_at_ms: 100_000,
            })
            .unwrap();
        state
            .apply(Command::ReserveJoinToken {
                hash,
                reservation_id,
                reservation_operation_id: Uuid::nil(),
                reserved_at_ms,
                readmission: false,
            })
            .unwrap();
    }

    assert!(state.expired_join_reservations(30_100).is_empty());
    assert_eq!(
        state
            .expired_join_reservations(30_101)
            .into_iter()
            .map(|reservation| reservation.node_id)
            .collect::<Vec<_>>(),
        vec![first]
    );
}

#[test]
fn readmission_flag_round_trips_through_json() {
    let command = Command::ReserveJoinToken {
        hash: JoinTokenHash([62; 32]),
        reservation_id: id(63),
        reservation_operation_id: Uuid::nil(),
        reserved_at_ms: 1_000,
        readmission: true,
    };

    let encoded = serde_json::to_vec(&command).unwrap();
    let decoded = serde_json::from_slice::<Command>(&encoded).unwrap();

    assert_eq!(decoded, command);
}
