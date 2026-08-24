use uuid::Uuid;

use super::super::state::JoinTokenReservation;
use super::super::{
    ApplicationState, CommandResult, DomainError, JoinTokenHash, ReadmissionRollback,
};

const JOIN_RESERVATION_TTL_MS: u64 = 30_000;

impl ApplicationState {
    pub(super) fn reserve_join_token(
        &mut self,
        hash: JoinTokenHash,
        reservation_id: Uuid,
        operation_id: Uuid,
        reserved_at_ms: u64,
        readmission: bool,
    ) -> Result<CommandResult, DomainError> {
        if let Some(reservation) = self.join_token_reservations.get_mut(&reservation_id)
            && reserved_at_ms <= reservation.reserved_until_ms
        {
            if reservation.hash != hash || reservation.operation_id != operation_id {
                return Err(DomainError::JoinAlreadyPending(reservation_id));
            }
            if readmission && reservation.readmission.is_none() {
                reservation.readmission = Some(ReadmissionRollback {
                    reachability: self.node_reachability.get(&reservation_id).cloned(),
                });
            }
            reservation.reserved_until_ms = reservation
                .reserved_until_ms
                .max(reserved_at_ms.saturating_add(JOIN_RESERVATION_TTL_MS))
                .min(reservation.expires_at_ms);
            return Ok(CommandResult::JoinTokenReserved);
        }
        if let Some(mut expired) = self.join_token_reservations.remove(&reservation_id) {
            match expired.readmission.take() {
                Some(rollback) => self.restore_readmission_state(reservation_id, rollback),
                None => self.remove_pending_join_state(reservation_id),
            }
            self.restore_join_token_reservation(expired, reserved_at_ms);
        }
        let Some(expires_at_ms) = self.join_tokens.get(&hash).copied() else {
            return Err(DomainError::InvalidJoinToken);
        };
        if reserved_at_ms > expires_at_ms {
            return Err(DomainError::InvalidJoinToken);
        }
        let rollback = readmission.then(|| ReadmissionRollback {
            reachability: self.node_reachability.get(&reservation_id).cloned(),
        });
        let limited = self.join_token_uses.contains_key(&hash);
        if let Some(uses) = self.join_token_uses.get(&hash).copied() {
            if uses <= 1 {
                self.join_token_uses.remove(&hash);
                self.join_tokens.remove(&hash);
            } else {
                self.join_token_uses.insert(hash, uses - 1);
            }
        }
        self.join_token_reservations.insert(
            reservation_id,
            JoinTokenReservation {
                hash,
                expires_at_ms,
                reserved_until_ms: reserved_at_ms
                    .saturating_add(JOIN_RESERVATION_TTL_MS)
                    .min(expires_at_ms),
                limited,
                operation_id,
                readmission: rollback,
            },
        );
        Ok(CommandResult::JoinTokenReserved)
    }

    pub(super) fn complete_join_token_reservation(
        &mut self,
        reservation_id: Uuid,
        operation_id: Uuid,
        accepted: bool,
        completed_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        let Some(reservation) = self.take_join_token_reservation(reservation_id, operation_id)
        else {
            return Ok(CommandResult::JoinTokenReservationCompleted);
        };
        if accepted {
            Ok(CommandResult::AdmissionAccepted(reservation_id))
        } else {
            self.restore_join_token_reservation(reservation, completed_at_ms);
            Ok(CommandResult::JoinTokenReservationCompleted)
        }
    }

    fn restore_join_token_reservation(
        &mut self,
        reservation: JoinTokenReservation,
        restored_at_ms: u64,
    ) {
        if reservation.expires_at_ms == 0
            || restored_at_ms > reservation.expires_at_ms
            || !reservation.limited
        {
            return;
        }
        if let Some(uses) = self.join_token_uses.get_mut(&reservation.hash) {
            *uses = uses.saturating_add(1);
        } else if let std::collections::btree_map::Entry::Vacant(entry) =
            self.join_tokens.entry(reservation.hash)
        {
            entry.insert(reservation.expires_at_ms);
            self.join_token_uses.insert(reservation.hash, 1);
        }
    }

    pub(super) fn abort_pending_join(
        &mut self,
        reservation_id: Uuid,
        operation_id: Uuid,
        completed_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        let Some(reservation) = self.take_join_token_reservation(reservation_id, operation_id)
        else {
            return Ok(CommandResult::JoinTokenReservationCompleted);
        };
        self.remove_pending_join_state(reservation_id);
        self.restore_join_token_reservation(reservation, completed_at_ms);
        Ok(CommandResult::JoinTokenReservationCompleted)
    }

    fn remove_pending_join_state(&mut self, reservation_id: Uuid) {
        let degraded = self.connectivity_degraded();
        self.node_reachability.remove(&reservation_id);
        self.connectivity_failures
            .retain(|route| route.source != reservation_id && route.destination != reservation_id);
        self.connectivity_failure_counts.retain(|route, _| {
            route.source != reservation_id && route.destination != reservation_id
        });
        self.connectivity_degraded = Some(degraded);
        self.connectivity_success_count = 0;
    }

    pub(super) fn abort_pending_readmission(
        &mut self,
        reservation_id: Uuid,
        operation_id: Uuid,
        completed_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        let matches = self
            .join_token_reservations
            .get(&reservation_id)
            .is_some_and(|reservation| {
                reservation.operation_id == operation_id && reservation.readmission.is_some()
            });
        if !matches {
            return Ok(CommandResult::JoinTokenReservationCompleted);
        }
        let mut reservation = self
            .join_token_reservations
            .remove(&reservation_id)
            .expect("the matching reservation exists");
        let rollback = reservation
            .readmission
            .take()
            .expect("a readmission reservation has rollback state");
        self.restore_readmission_state(reservation_id, rollback);
        self.restore_join_token_reservation(reservation, completed_at_ms);
        Ok(CommandResult::JoinTokenReservationCompleted)
    }

    fn restore_readmission_state(&mut self, reservation_id: Uuid, rollback: ReadmissionRollback) {
        match rollback.reachability {
            Some(reachability) => {
                self.node_reachability.insert(reservation_id, reachability);
            }
            None => {
                self.node_reachability.remove(&reservation_id);
            }
        }
    }

    pub(super) fn ensure_join_reservation(
        &self,
        reservation_id: Uuid,
        operation_id: Uuid,
    ) -> Result<(), DomainError> {
        if self
            .join_token_reservations
            .get(&reservation_id)
            .is_some_and(|reservation| reservation.operation_id == operation_id)
        {
            Ok(())
        } else {
            Err(DomainError::JoinAlreadyPending(reservation_id))
        }
    }

    fn take_join_token_reservation(
        &mut self,
        reservation_id: Uuid,
        operation_id: Uuid,
    ) -> Option<JoinTokenReservation> {
        (self
            .join_token_reservations
            .get(&reservation_id)?
            .operation_id
            == operation_id)
            .then(|| {
                self.join_token_reservations
                    .remove(&reservation_id)
                    .expect("the matching reservation exists")
            })
    }
}
