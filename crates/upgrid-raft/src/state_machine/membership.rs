use openraft::alias::StoredMembershipOf;

use crate::domain::{ApplicationState, Command, DomainError};
use crate::raft::TC;

pub(super) fn validate_reachability_command(
    command: &Command,
    submitted_at_ms: u64,
    membership: &StoredMembershipOf<TC>,
    application: &ApplicationState,
) -> Result<(), DomainError> {
    let ensure_member = |node_id| {
        membership
            .get_node(&node_id)
            .map(|_| ())
            .ok_or(DomainError::NodeNotInMembership(node_id))
    };
    let ensure_admission = |node_id, operation_id| {
        let Some(readmission) =
            application.active_join_reservation(node_id, operation_id, submitted_at_ms)
        else {
            return Err(DomainError::JoinAlreadyPending(node_id));
        };
        if readmission {
            ensure_member(node_id)
        } else {
            Ok(())
        }
    };
    match command {
        Command::ReplaceConfiguredReachableAddresses { node_id, .. }
        | Command::VerifyReachableAddress { node_id, .. } => ensure_member(*node_id),
        Command::RenewReachabilityLeases(leases) => leases
            .iter()
            .try_for_each(|lease| ensure_member(lease.node_id)),
        Command::ReplaceAdmissionConfiguredReachableAddresses {
            node_id,
            reservation_operation_id,
            ..
        }
        | Command::VerifyAdmissionReachableAddress {
            node_id,
            reservation_operation_id,
            ..
        } => ensure_admission(*node_id, *reservation_operation_id),
        Command::RenewAdmissionReachabilityLeases {
            reservation_id,
            reservation_operation_id,
            leases,
        } => {
            ensure_admission(*reservation_id, *reservation_operation_id)?;
            leases.iter().try_for_each(|lease| {
                if lease.node_id == *reservation_id {
                    Ok(())
                } else {
                    Err(DomainError::NodeNotInMembership(lease.node_id))
                }
            })
        }
        Command::RecordConnectivity {
            leases,
            verified,
            failures,
            ..
        } => {
            leases
                .iter()
                .try_for_each(|lease| ensure_member(lease.node_id))?;
            if let Some(verified) = verified {
                verified
                    .keys()
                    .try_for_each(|node_id| ensure_member(*node_id))?;
            }
            failures.iter().try_for_each(|route| {
                ensure_member(route.source)?;
                ensure_member(route.destination)
            })
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use openraft::alias::StoredMembershipOf;
    use uuid::Uuid;

    use super::validate_reachability_command;
    use crate::domain::{ApplicationState, Command, DomainError, JoinTokenHash};
    use crate::raft::{NodeIdentity, TC};

    #[test]
    fn staged_reachability_requires_an_active_reservation() {
        let member_id = Uuid::from_u128(1);
        let pending_id = Uuid::from_u128(2);
        let operation_id = Uuid::from_u128(3);
        let membership = StoredMembershipOf::<TC>::new(
            None,
            openraft::Membership::from(BTreeMap::from([(member_id, NodeIdentity::default())])),
        );
        let mut application = ApplicationState::default();
        let command = Command::ReplaceAdmissionConfiguredReachableAddresses {
            node_id: pending_id,
            addresses: BTreeSet::new(),
            reservation_operation_id: operation_id,
        };

        assert_eq!(
            validate_reachability_command(&command, 100, &membership, &application),
            Err(DomainError::JoinAlreadyPending(pending_id))
        );
        let hash = JoinTokenHash([7; 32]);
        application
            .apply(Command::PutJoinToken {
                hash,
                expires_at_ms: 100_000,
            })
            .unwrap();
        application
            .apply(Command::ReserveJoinToken {
                hash,
                reservation_id: pending_id,
                reservation_operation_id: operation_id,
                reserved_at_ms: 100,
                readmission: false,
            })
            .unwrap();
        assert!(validate_reachability_command(&command, 100, &membership, &application).is_ok());
        assert_eq!(
            validate_reachability_command(&command, 30_101, &membership, &application),
            Err(DomainError::JoinAlreadyPending(pending_id))
        );
    }
}
