use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use compio::runtime::{JoinHandle, spawn};
use compio::time::sleep;
use openraft::ChangeMembers;
use openraft::async_runtime::watch::WatchReceiver as _;
use tracing::debug;

use crate::domain::{Command, ExpiredJoinReservation};
use crate::raft::{Raft, Req};
use crate::rpc::Rpc;
use crate::state_machine::StateMachine;

pub(super) fn watch_expired_admissions(
    node_id: uuid::Uuid,
    raft: Raft,
    rpc: Rpc,
    state_machine: Rc<StateMachine>,
) -> JoinHandle<()> {
    spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            if raft.current_leader().await != Some(node_id) {
                continue;
            }
            let now_ms = upgrid_config::now_ms();
            let expired = state_machine.expired_join_reservations(now_ms);
            for reservation in expired {
                let reservation_id = reservation.node_id;
                let _membership_change = rpc.membership_changes().lock().await;
                if raft.current_leader().await != Some(node_id)
                    || !state_machine
                        .expired_join_reservations(now_ms)
                        .contains(&reservation)
                {
                    continue;
                }
                let metrics = raft.metrics();
                let current = metrics.borrow_watched();
                let is_member = current
                    .membership_config
                    .nodes()
                    .any(|(member_id, _)| *member_id == reservation_id);
                let is_voter = current
                    .membership_config
                    .membership()
                    .voter_ids()
                    .any(|voter_id| voter_id == reservation_id);
                drop(current);
                if is_member && !is_voter {
                    let change = ChangeMembers::RemoveNodes(BTreeSet::from([reservation_id]));
                    if let Err(error) = raft.change_membership(change, false).await {
                        debug!(%reservation_id, %error, "could not remove expired admission learner");
                        continue;
                    }
                }
                let result = rpc
                    .write_to_leader(
                        Req::new(reconciliation_command(reservation, is_voter, now_ms)),
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await;
                if let Err(error) = result {
                    debug!(%reservation_id, %error, "could not reconcile expired admission");
                }
            }
        }
    })
}

fn reconciliation_command(
    reservation: ExpiredJoinReservation,
    is_voter: bool,
    completed_at_ms: u64,
) -> Command {
    let reservation_id = reservation.node_id;
    let reservation_operation_id = reservation.operation_id;
    if reservation.readmission && is_voter {
        Command::AbortPendingReadmission {
            reservation_id,
            reservation_operation_id,
            completed_at_ms,
        }
    } else if is_voter {
        Command::CompleteJoinTokenReservation {
            reservation_id,
            reservation_operation_id,
            accepted: true,
            completed_at_ms,
        }
    } else {
        Command::AbortPendingJoin {
            reservation_id,
            reservation_operation_id,
            completed_at_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::reconciliation_command;
    use crate::domain::{Command, ExpiredJoinReservation};

    #[test]
    fn expired_readmission_restores_voter_state() {
        let reservation_id = Uuid::from_u128(7);
        let reservation = ExpiredJoinReservation {
            node_id: reservation_id,
            operation_id: Uuid::from_u128(8),
            readmission: true,
        };
        assert!(matches!(
            reconciliation_command(reservation, true, 42),
            Command::AbortPendingReadmission {
                reservation_id: id,
                reservation_operation_id,
                completed_at_ms: 42,
            } if id == reservation_id && reservation_operation_id == reservation.operation_id
        ));
        assert!(matches!(
            reconciliation_command(reservation, false, 42),
            Command::AbortPendingJoin {
                reservation_id: id,
                reservation_operation_id,
                completed_at_ms: 42,
            } if id == reservation_id && reservation_operation_id == reservation.operation_id
        ));
        assert!(matches!(
            reconciliation_command(
                ExpiredJoinReservation {
                    readmission: false,
                    ..reservation
                },
                true,
                42
            ),
            Command::CompleteJoinTokenReservation {
                reservation_id: id,
                reservation_operation_id,
                accepted: true,
                completed_at_ms: 42,
            } if id == reservation_id && reservation_operation_id == reservation.operation_id
        ));
    }
}
