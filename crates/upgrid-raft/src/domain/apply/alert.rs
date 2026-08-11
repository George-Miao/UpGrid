use super::*;
use crate::domain::{AlertId, MAX_DIAGNOSTIC_BYTES};

impl ApplicationState {
    pub(super) fn acknowledge_alert(
        &mut self,
        alert_id: AlertId,
        acknowledged_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        if !self.alerts.contains_key(&alert_id) {
            return Err(DomainError::AlertNotFound(alert_id));
        }
        self.alert_acknowledgements
            .entry(alert_id)
            .or_insert(acknowledged_at_ms);
        Ok(CommandResult::AlertUpdated(alert_id))
    }

    pub(super) fn retry_alert(
        &mut self,
        alert_id: AlertId,
        retry_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        let alert = self
            .alerts
            .get_mut(&alert_id)
            .ok_or(DomainError::AlertNotFound(alert_id))?;
        let attempts = match alert.delivery {
            AlertDelivery::Pending { attempts, .. } => attempts,
            AlertDelivery::Failed { .. } => 0,
            AlertDelivery::Delivered { .. } => {
                return Err(DomainError::InvalidAlert(
                    "delivered alerts cannot be retried".to_owned(),
                ));
            }
        };
        alert.delivery = AlertDelivery::Pending {
            attempts,
            next_attempt_at_ms: retry_at_ms,
        };
        Ok(CommandResult::AlertUpdated(alert_id))
    }

    pub(super) fn record_alert_failure(
        &mut self,
        alert_id: AlertId,
        attempted_at_ms: u64,
        retry_at_ms: Option<u64>,
        diagnostic: String,
    ) -> Result<CommandResult, DomainError> {
        if diagnostic.len() > MAX_DIAGNOSTIC_BYTES {
            return Err(DomainError::InvalidAlert(format!(
                "diagnostic exceeds {MAX_DIAGNOSTIC_BYTES} bytes"
            )));
        }
        let alert = self
            .alerts
            .get_mut(&alert_id)
            .ok_or(DomainError::AlertNotFound(alert_id))?;
        if matches!(&alert.delivery, AlertDelivery::Delivered { .. }) {
            return Ok(CommandResult::AlertUpdated(alert_id));
        }
        let attempts = match &alert.delivery {
            AlertDelivery::Pending { attempts, .. } => (*attempts).saturating_add(1),
            AlertDelivery::Delivered { .. } | AlertDelivery::Failed { .. } => 1,
        };
        alert.delivery = match retry_at_ms {
            Some(next_attempt_at_ms) => AlertDelivery::Pending {
                attempts,
                next_attempt_at_ms,
            },
            None => AlertDelivery::Failed {
                failed_at_ms: attempted_at_ms,
                diagnostic,
            },
        };
        Ok(CommandResult::AlertUpdated(alert_id))
    }
}
