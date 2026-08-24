use super::evaluation::id;
use super::*;
use crate::ReachableAddress;
#[test]
fn retrying_an_admission_extends_its_active_reservation() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([13; 32]);
    let reservation_id = id(15);
    let operation_id = id(16);
    state
        .apply(Command::PutJoinToken {
            hash,
            expires_at_ms: 100_000,
        })
        .unwrap();
    let reserve = |reserved_at_ms| Command::ReserveJoinToken {
        hash,
        reservation_id,
        reservation_operation_id: operation_id,
        reserved_at_ms,
        readmission: false,
    };

    state.apply(reserve(1_000)).unwrap();
    assert_eq!(
        state.active_join_reservation(reservation_id, operation_id, 40_000),
        None,
    );

    state.apply(reserve(20_000)).unwrap();
    assert_eq!(
        state.active_join_reservation(reservation_id, operation_id, 40_000),
        Some(false),
    );
}

#[test]
fn promoted_fresh_reservation_captures_rollback_on_retry() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([14; 32]);
    let node_id = id(17);
    let operation_id = id(18);
    state
        .apply(Command::PutJoinToken {
            hash,
            expires_at_ms: 100_000,
        })
        .unwrap();
    state
        .apply(Command::ReserveJoinToken {
            hash,
            reservation_id: node_id,
            reservation_operation_id: operation_id,
            reserved_at_ms: 1_000,
            readmission: false,
        })
        .unwrap();
    let working = ReachableAddress::parse("up://working.example:11451").unwrap();
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id,
            addresses: BTreeSet::from([working.clone()]),
        })
        .unwrap();

    state
        .apply(Command::ReserveJoinToken {
            hash,
            reservation_id: node_id,
            reservation_operation_id: operation_id,
            reserved_at_ms: 2_000,
            readmission: true,
        })
        .unwrap();
    state
        .apply(Command::ReplaceConfiguredReachableAddresses {
            node_id,
            addresses: BTreeSet::from([
                ReachableAddress::parse("up://failed-retry.example:11451").unwrap()
            ]),
        })
        .unwrap();
    state
        .apply(Command::AbortPendingReadmission {
            reservation_id: node_id,
            reservation_operation_id: operation_id,
            completed_at_ms: 3_000,
        })
        .unwrap();

    assert_eq!(
        state.node_reachability[&node_id].configured_reachable_addresses(),
        &BTreeSet::from([working])
    );
    assert!(!state.join_token_reservations.contains_key(&node_id));
}
