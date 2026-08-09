impl ApplicationState {
    pub fn apply_operation(
        &mut self,
        operation_id: Uuid,
        submitted_at_ms: u64,
        command: Command,
    ) -> Result<CommandResult, DomainError> {
        if let Some(processed) = self.processed_operations.get(&operation_id) {
            return processed.result.clone();
        }

        let result = self.apply(command);
        self.latest_operation_at_ms = self.latest_operation_at_ms.max(submitted_at_ms);
        let cutoff = self
            .latest_operation_at_ms
            .saturating_sub(DEFAULT_OPERATION_RETENTION_MS);
        self.processed_operations
            .retain(|_, item| item.submitted_at_ms >= cutoff);
        self.processed_operations.insert(
            operation_id,
            ProcessedOperation {
                submitted_at_ms,
                result: result.clone(),
            },
        );
        result
    }

    pub fn apply(&mut self, command: Command) -> Result<CommandResult, DomainError> {
        match command {
            Command::PutSecret(secret) => self.put_secret(secret),
            Command::PutNotificationChannel(channel) => self.put_notification_channel(channel),
            Command::CreateTarget(target) => self.create_target(target),
            Command::UpdateTarget(target) => self.update_target(target),
            Command::DeleteTarget(target_id) => {
                self.targets
                    .remove(&target_id)
                    .ok_or(DomainError::TargetNotFound(target_id))?;
                self.assignments
                    .retain(|evaluation_id, _| evaluation_id.target_id != target_id);
                Ok(CommandResult::TargetDeleted(target_id))
            }
            Command::AssignEvaluation(assignment) => self.assign_evaluation(assignment),
            Command::AssignEvaluations(assignments) => self.assign_evaluations(assignments),
            Command::SetHistoryRetention { retention_ms } => {
                if retention_ms == 0 {
                    return Err(DomainError::InvalidEvaluation(
                        "history retention must be greater than zero".to_owned(),
                    ));
                }
                self.history_retention_ms = retention_ms;
                Ok(CommandResult::HistoryRetentionSet(retention_ms))
            }
            Command::SetTargetPaused { target_id, paused } => {
                let target = self
                    .targets
                    .get_mut(&target_id)
                    .ok_or(DomainError::TargetNotFound(target_id))?;
                target.paused = paused;
                if paused {
                    self.assignments
                        .retain(|evaluation_id, _| evaluation_id.target_id != target_id);
                }
                Ok(CommandResult::TargetPauseSet { target_id, paused })
            }
            Command::DeleteNotificationChannel(channel_id) => {
                if self
                    .targets
                    .values()
                    .any(|target| target.target.notification_channels.contains(&channel_id))
                {
                    return Err(DomainError::InvalidNotificationChannel(
                        "notification channel is still referenced by a Target".to_owned(),
                    ));
                }
                self.notification_channels
                    .remove(&channel_id)
                    .ok_or(DomainError::NotificationChannelNotFound(channel_id))?;
                Ok(CommandResult::NotificationChannelDeleted(channel_id))
            }
            Command::DeleteSecret(secret_id) => {
                let target_reference = self
                    .targets
                    .values()
                    .any(|target| target.target.http.secret_ids().any(|id| id == secret_id));
                let channel_reference = self
                    .notification_channels
                    .values()
                    .any(|channel| channel.secret_ids().contains(&secret_id));
                if target_reference || channel_reference {
                    return Err(DomainError::InvalidSecret(
                        "secret is still referenced by a Target or Notification Channel".to_owned(),
                    ));
                }
                self.secrets
                    .remove(&secret_id)
                    .ok_or(DomainError::SecretNotFound(secret_id))?;
                Ok(CommandResult::SecretDeleted(secret_id))
            }
            Command::PutJoinToken {
                hash,
                expires_at_ms,
            } => {
                if expires_at_ms == 0 {
                    return Err(DomainError::InvalidJoinToken);
                }
                self.join_tokens.insert(hash, expires_at_ms);
                self.join_token_uses.remove(&hash);
                Ok(CommandResult::JoinTokenStored)
            }
            Command::PutLimitedJoinToken {
                hash,
                expires_at_ms,
                uses,
            } => {
                if expires_at_ms == 0 || uses == 0 {
                    return Err(DomainError::InvalidJoinToken);
                }
                self.join_tokens.insert(hash, expires_at_ms);
                self.join_token_uses.insert(hash, uses);
                Ok(CommandResult::JoinTokenStored)
            }
            Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms,
            } => {
                let Some(expires_at_ms) = self.join_tokens.get(&hash).copied() else {
                    return Err(DomainError::InvalidJoinToken);
                };
                if authorized_at_ms > expires_at_ms {
                    return Err(DomainError::InvalidJoinToken);
                }
                if let Some(uses) = self.join_token_uses.get_mut(&hash) {
                    if *uses == 1 {
                        self.join_token_uses.remove(&hash);
                        self.join_tokens.remove(&hash);
                    } else {
                        *uses = uses.saturating_sub(1);
                    }
                }
                Ok(CommandResult::JoinTokenAuthorized)
            }
            Command::RevokeJoinToken(hash) => {
                self.join_tokens
                    .remove(&hash)
                    .ok_or(DomainError::InvalidJoinToken)?;
                self.join_token_uses.remove(&hash);
                Ok(CommandResult::JoinTokenRevoked)
            }
            Command::SetNodeName { node_id, name } => {
                let name = name.trim();
                if name.is_empty() || name.len() > 64 || name.chars().any(char::is_control) {
                    return Err(DomainError::InvalidNodeName(
                        "node name must contain 1 to 64 printable characters".to_owned(),
                    ));
                }
                self.node_names.insert(node_id, name.to_owned());
                Ok(CommandResult::NodeNameSet(node_id))
            }
            Command::RecordEvaluation(evaluation) => self.record_evaluation(evaluation),
            Command::MarkAlertDelivered {
                alert_id,
                delivered_at_ms,
            } => {
                let alert = self
                    .alerts
                    .get_mut(&alert_id)
                    .ok_or(DomainError::AlertNotFound(alert_id))?;
                if !matches!(&alert.delivery, AlertDelivery::Delivered { .. }) {
                    alert.delivery = AlertDelivery::Delivered { delivered_at_ms };
                }
                Ok(CommandResult::AlertUpdated(alert_id))
            }
            Command::RecordAlertFailure {
                alert_id,
                attempted_at_ms,
                retry_at_ms,
                diagnostic,
            } => self.record_alert_failure(alert_id, attempted_at_ms, retry_at_ms, diagnostic),
        }
    }

    fn put_secret(&mut self, secret: Secret) -> Result<CommandResult, DomainError> {
        secret.validate()?;
        let id = secret.id;
        self.secrets.insert(id, secret);
        Ok(CommandResult::SecretStored(id))
    }

    fn put_notification_channel(
        &mut self,
        channel: NotificationChannel,
    ) -> Result<CommandResult, DomainError> {
        channel.validate()?;
        for secret_id in channel.secret_ids() {
            if !self.secrets.contains_key(&secret_id) {
                return Err(DomainError::SecretNotFound(secret_id));
            }
        }
        let id = channel.id;
        self.notification_channels.insert(id, channel);
        Ok(CommandResult::NotificationChannelStored(id))
    }

    fn create_target(&mut self, target: Target) -> Result<CommandResult, DomainError> {
        target.validate()?;
        if self.targets.contains_key(&target.id) {
            return Err(DomainError::TargetAlreadyExists(target.id));
        }
        self.validate_target_references(&target)?;
        let id = target.id;
        self.targets.insert(id, TargetState::new(target));
        Ok(CommandResult::TargetCreated(id))
    }

    fn update_target(&mut self, target: Target) -> Result<CommandResult, DomainError> {
        target.validate()?;
        self.validate_target_references(&target)?;
        let target_state = self
            .targets
            .get_mut(&target.id)
            .ok_or(DomainError::TargetNotFound(target.id))?;
        let id = target.id;
        target_state.target = target;
        Ok(CommandResult::TargetUpdated(id))
    }

    fn validate_target_references(&self, target: &Target) -> Result<(), DomainError> {
        for channel_id in &target.notification_channels {
            if !self.notification_channels.contains_key(channel_id) {
                return Err(DomainError::NotificationChannelNotFound(*channel_id));
            }
        }
        for secret_id in target.http.secret_ids() {
            if !self.secrets.contains_key(&secret_id) {
                return Err(DomainError::SecretNotFound(secret_id));
            }
        }
        Ok(())
    }

    fn assign_evaluation(
        &mut self,
        assignment: EvaluationAssignment,
    ) -> Result<CommandResult, DomainError> {
        assignment.validate()?;
        let Some(target) = self.targets.get(&assignment.id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if target.paused {
            return Ok(CommandResult::EvaluationDiscarded);
        }
        if target
            .latest_evaluation
            .as_ref()
            .is_some_and(|latest| latest.id.scheduled_at_ms >= assignment.id.scheduled_at_ms)
            || target.history.contains_key(&assignment.id.scheduled_at_ms)
            || self
                .assignments
                .get(&assignment.id)
                .is_some_and(|current| current.attempt >= assignment.attempt)
            || self
                .assignments
                .keys()
                .any(|id| id.target_id == assignment.id.target_id && *id != assignment.id)
        {
            return Ok(CommandResult::EvaluationDiscarded);
        }
        let id = assignment.id;
        self.assignments.insert(id, assignment);
        Ok(CommandResult::EvaluationAssigned(id))
    }

    fn assign_evaluations(
        &mut self,
        assignments: Vec<EvaluationAssignment>,
    ) -> Result<CommandResult, DomainError> {
        for assignment in &assignments {
            assignment.validate()?;
        }
        for assignment in assignments {
            self.assign_evaluation(assignment)?;
        }
        Ok(CommandResult::Noop)
    }

    fn record_evaluation(&mut self, evaluation: Evaluation) -> Result<CommandResult, DomainError> {
        evaluation.validate()?;
        self.assignments.remove(&evaluation.id);
        let Some(target_state) = self.targets.get_mut(&evaluation.id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if target_state.paused {
            return Ok(CommandResult::EvaluationDiscarded);
        }
        if target_state
            .history
            .contains_key(&evaluation.id.scheduled_at_ms)
            || target_state
                .latest_evaluation
                .as_ref()
                .is_some_and(|latest| latest.id.scheduled_at_ms >= evaluation.id.scheduled_at_ms)
        {
            return Ok(CommandResult::EvaluationDiscarded);
        }

        let previous_availability = target_state.availability;
        if evaluation.succeeded {
            target_state.consecutive_failures = 0;
            target_state.availability = AvailabilityState::Up;
        } else {
            target_state.consecutive_failures = target_state.consecutive_failures.saturating_add(1);
            if target_state.consecutive_failures >= target_state.target.policy.failure_threshold {
                target_state.availability = AvailabilityState::Down;
            }
        }

        let transition = match (previous_availability, target_state.availability) {
            (AvailabilityState::Down, AvailabilityState::Up) => Some(AlertKind::Recovered),
            (previous, AvailabilityState::Down) if previous != AvailabilityState::Down => {
                Some(AlertKind::Down)
            }
            _ => None,
        };

        let channel_ids = target_state
            .target
            .notification_channels
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let target_name = target_state.target.name.clone();
        let target_url = target_state.target.http.url.clone();
        target_state.latest_evaluation = Some(evaluation.clone());
        target_state
            .history
            .insert(evaluation.id.scheduled_at_ms, evaluation.clone());
        let cutoff = evaluation
            .recorded_at_ms
            .saturating_sub(self.history_retention_ms);
        target_state
            .history
            .retain(|_, item| item.recorded_at_ms >= cutoff);
        self.transitions
            .retain(|_, item| item.evaluation.recorded_at_ms >= cutoff);
        let availability = target_state.availability;

        let mut alert_ids = Vec::new();
        if let Some(kind) = transition {
            self.transitions
                .entry(evaluation.id)
                .or_insert_with(|| AvailabilityTransition {
                    kind,
                    target_name: target_name.clone(),
                    target_url: target_url.clone(),
                    evaluation: evaluation.clone(),
                });
            for channel_id in channel_ids {
                let id = AlertId {
                    target_id: evaluation.id.target_id,
                    channel_id,
                    evaluation_scheduled_at_ms: evaluation.id.scheduled_at_ms,
                    kind,
                };
                self.alerts.entry(id).or_insert_with(|| Alert {
                    id,
                    target_name: target_name.clone(),
                    target_url: target_url.clone(),
                    evaluation: evaluation.clone(),
                    delivery: AlertDelivery::Pending {
                        attempts: 0,
                        next_attempt_at_ms: evaluation.recorded_at_ms,
                    },
                });
                alert_ids.push(id);
            }
        }

        Ok(CommandResult::EvaluationAccepted {
            availability,
            alerts: alert_ids,
        })
    }

    fn record_alert_failure(
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
use uuid::Uuid;

use super::{
    Alert, AlertDelivery, AlertId, AlertKind, ApplicationState, AvailabilityState,
    AvailabilityTransition, Command, CommandResult, DEFAULT_OPERATION_RETENTION_MS, DomainError,
    Evaluation, EvaluationAssignment, MAX_DIAGNOSTIC_BYTES, NotificationChannel,
    ProcessedOperation, Secret, Target, TargetState,
};
