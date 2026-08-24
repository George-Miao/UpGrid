use uuid::Uuid;

use super::state::ProcessedOperation;
use super::{
    AlertDelivery, ApplicationState, Command, CommandResult, DEFAULT_OPERATION_RETENTION_MS,
    DomainError, Secret, TargetId,
};

mod alert;
mod evaluation;
mod join_token;
mod resource;
mod secret;
mod trash;

impl ApplicationState {
    pub(crate) fn processed_operation_result(
        &self,
        operation_id: Uuid,
    ) -> Option<Result<CommandResult, DomainError>> {
        self.processed_operations
            .get(&operation_id)
            .map(|processed| processed.result.clone())
    }

    pub fn apply_operation(
        &mut self,
        operation_id: Uuid,
        submitted_at_ms: u64,
        command: Command,
    ) -> Result<CommandResult, DomainError> {
        if let Some(processed) = self.processed_operations.get(&operation_id) {
            return processed.result.clone();
        }

        let result = self.apply_with_operation(command);
        self.cache_operation_result(operation_id, submitted_at_ms, result)
    }

    pub(crate) fn cache_operation_result(
        &mut self,
        operation_id: Uuid,
        submitted_at_ms: u64,
        result: Result<CommandResult, DomainError>,
    ) -> Result<CommandResult, DomainError> {
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
        self.apply_with_operation(command)
    }

    fn apply_with_operation(&mut self, command: Command) -> Result<CommandResult, DomainError> {
        match command {
            Command::CreateIdentity(identity) => self.create_identity(identity),
            Command::UpdateIdentity(identity) => self.update_identity(identity),
            Command::DeleteIdentity(identity_id) => self.delete_identity(identity_id),
            Command::CreateApiToken(token) => self.create_api_token(token),
            Command::RevokeApiToken(token_id) => {
                self.api_tokens
                    .remove(&token_id)
                    .ok_or(DomainError::ApiTokenNotFound(token_id))?;
                Ok(CommandResult::ApiTokenRevoked(token_id))
            }
            Command::PutSecret(secret) => self.put_secret(secret),
            Command::CreateNotificationChannel {
                channel,
                generated_secret,
                is_default,
            } => self.create_notification_channel(channel, generated_secret, is_default),
            Command::UpdateNotificationChannel {
                channel,
                generated_secret,
                is_default,
            } => self.update_notification_channel(channel, generated_secret, is_default),
            Command::CreateTarget {
                target,
                use_default_notifications,
            } => self.create_target(target, use_default_notifications),
            Command::UpdateTarget {
                target,
                use_default_notifications,
            } => self.update_target(target, use_default_notifications),
            Command::CreateTargetWithLocations {
                target,
                use_default_notifications,
                locations,
            } => self.create_target_with_locations(target, use_default_notifications, locations),
            Command::UpdateTargetWithLocations {
                target,
                use_default_notifications,
                locations,
            } => self.update_target_with_locations(target, use_default_notifications, locations),
            Command::DeleteTarget(target_id) => self.hard_delete_target(target_id),
            Command::TrashTarget {
                target_id,
                deleted_at_ms,
            } => self.trash_target(target_id, deleted_at_ms),
            Command::RestoreTarget {
                target_id,
                restored_at_ms,
            } => self.restore_target(target_id, restored_at_ms),
            Command::PurgeTarget(target_id) => self.purge_target(target_id),
            Command::PruneTargetTrash { now_ms } => self.prune_target_trash(now_ms),
            Command::SetTargetTrashRetention {
                retention_ms,
                now_ms,
            } => self.set_target_trash_retention(retention_ms, now_ms),
            Command::AssignEvaluation(assignment) => self.assign_one_evaluation(assignment),
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
            Command::SetHistoryRollupRetention { retention_ms } => {
                if retention_ms == 0 {
                    return Err(DomainError::InvalidEvaluation(
                        "history rollup retention must be greater than zero".to_owned(),
                    ));
                }
                self.history_rollup_retention_ms = retention_ms;
                Ok(CommandResult::HistoryRollupRetentionSet(retention_ms))
            }
            Command::SetPublicStatusEnabled { enabled } => {
                self.public_status_enabled = enabled;
                Ok(CommandResult::PublicStatusEnabledSet(enabled))
            }
            Command::SetTargetPaused { target_id, paused } => {
                let target = self
                    .targets
                    .get_mut(&target_id)
                    .ok_or(DomainError::TargetNotFound(target_id))?;
                target.paused = paused;
                if paused {
                    self.assignments
                        .retain(|key, _| key.id.target_id != target_id);
                    self.evaluation_batches
                        .retain(|id, _| id.target_id != target_id);
                }
                Ok(CommandResult::TargetPauseSet { target_id, paused })
            }
            Command::DeleteNotificationChannel(channel_id) => {
                if self
                    .targets
                    .values()
                    .any(|target| target.target.notification_channels.contains(&channel_id))
                    || self.trashed_targets.values().any(|target| {
                        target
                            .state
                            .target
                            .notification_channels
                            .contains(&channel_id)
                    })
                {
                    return Err(DomainError::InvalidNotificationChannel(
                        "notification channel is still referenced by a Target".to_owned(),
                    ));
                }
                self.notification_channels
                    .remove(&channel_id)
                    .ok_or(DomainError::NotificationChannelNotFound(channel_id))?;
                self.default_notification_channels.remove(&channel_id);
                Ok(CommandResult::NotificationChannelDeleted(channel_id))
            }
            Command::DeleteSecret(secret_id) => self.delete_secret(secret_id),
            Command::DeleteUnreferencedSecrets => self.delete_unreferenced_secrets(),
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
                let removed = self.join_tokens.remove(&hash).is_some();
                let mut reserved = false;
                for reservation in self.join_token_reservations.values_mut() {
                    if reservation.hash == hash {
                        reservation.expires_at_ms = 0;
                        reserved = true;
                    }
                }
                if !removed && !reserved {
                    return Err(DomainError::InvalidJoinToken);
                }
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
                if let Some(target) = self.node_targets.get_mut(&TargetId(node_id)) {
                    target.target.name = name.to_owned();
                }
                Ok(CommandResult::NodeNameSet(node_id))
            }
            Command::SetNodeDraining {
                node_id,
                draining,
                force,
            } => {
                if draining {
                    self.draining_nodes.insert(node_id);
                    if force {
                        self.assignments
                            .retain(|_, assignment| assignment.executor_node_id != node_id);
                    }
                } else {
                    self.draining_nodes.remove(&node_id);
                }
                Ok(CommandResult::NodeDrainSet { node_id, draining })
            }
            Command::SetNotificationChannelDefault {
                channel_id,
                is_default,
            } => {
                if !self.notification_channels.contains_key(&channel_id) {
                    return Err(DomainError::NotificationChannelNotFound(channel_id));
                }
                if is_default {
                    self.default_notification_channels.insert(channel_id);
                } else {
                    self.default_notification_channels.remove(&channel_id);
                }
                Ok(CommandResult::NotificationChannelDefaultSet(channel_id))
            }
            Command::SyncNodeTargets(targets) => self.sync_node_targets(targets),
            Command::RecordEvaluation(evaluation) => self.record_evaluation(evaluation),
            Command::RecordNodeEvaluation(evaluation) => self.record_node_evaluation(evaluation),
            Command::MarkAlertDelivered {
                alert_id,
                delivered_at_ms,
            } => {
                let resolved = self
                    .resolve_alert_id(alert_id)
                    .ok_or(DomainError::AlertNotFound(alert_id))?;
                let alert = self
                    .alerts
                    .get_mut(&resolved)
                    .expect("resolved alert exists");
                if !matches!(&alert.delivery, AlertDelivery::Delivered { .. }) {
                    alert.delivery = AlertDelivery::Delivered { delivered_at_ms };
                }
                Ok(CommandResult::AlertUpdated(resolved))
            }
            Command::RecordAlertFailure {
                alert_id,
                attempted_at_ms,
                retry_at_ms,
                diagnostic,
            } => self.record_alert_failure(alert_id, attempted_at_ms, retry_at_ms, diagnostic),
            Command::AcknowledgeAlert {
                alert_id,
                acknowledged_at_ms,
            } => self.acknowledge_alert(alert_id, acknowledged_at_ms),
            Command::RetryAlert {
                alert_id,
                retry_at_ms,
            } => self.retry_alert(alert_id, retry_at_ms),
            Command::ReplaceConfiguredReachableAddresses { node_id, addresses } => {
                self.replace_configured_reachable_addresses(node_id, addresses)
            }
            Command::RenewReachabilityLeases(leases) => self.renew_reachability_leases(leases),
            Command::VerifyReachableAddress {
                node_id,
                address,
                verified_at_ms,
            } => self.verify_reachable_address(node_id, address, verified_at_ms),
            Command::ReplaceAdmissionConfiguredReachableAddresses {
                node_id,
                addresses,
                reservation_operation_id,
            } => {
                self.ensure_join_reservation(node_id, reservation_operation_id)?;
                self.replace_configured_reachable_addresses(node_id, addresses)
            }
            Command::RenewAdmissionReachabilityLeases {
                reservation_id,
                reservation_operation_id,
                leases,
            } => {
                self.ensure_join_reservation(reservation_id, reservation_operation_id)?;
                if leases.iter().any(|lease| lease.node_id != reservation_id) {
                    return Err(DomainError::NodeNotInMembership(reservation_id));
                }
                self.renew_reachability_leases(leases)
            }
            Command::VerifyAdmissionReachableAddress {
                node_id,
                address,
                verified_at_ms,
                reservation_operation_id,
            } => {
                self.ensure_join_reservation(node_id, reservation_operation_id)?;
                self.verify_reachable_address(node_id, address, verified_at_ms)
            }
            Command::RecordConnectivity {
                leases,
                verified,
                checked_at_ms,
                failures,
            } => self.record_connectivity(leases, verified, checked_at_ms, failures),
            Command::ReserveJoinToken {
                hash,
                reservation_id,
                reserved_at_ms,
                readmission,
                reservation_operation_id,
            } => self.reserve_join_token(
                hash,
                reservation_id,
                reservation_operation_id,
                reserved_at_ms,
                readmission,
            ),
            Command::CompleteJoinTokenReservation {
                reservation_id,
                reservation_operation_id,
                accepted,
                completed_at_ms,
            } => self.complete_join_token_reservation(
                reservation_id,
                reservation_operation_id,
                accepted,
                completed_at_ms,
            ),
            Command::AbortPendingJoin {
                reservation_id,
                reservation_operation_id,
                completed_at_ms,
            } => self.abort_pending_join(reservation_id, reservation_operation_id, completed_at_ms),
            Command::AbortPendingReadmission {
                reservation_id,
                reservation_operation_id,
                completed_at_ms,
            } => self.abort_pending_readmission(
                reservation_id,
                reservation_operation_id,
                completed_at_ms,
            ),
        }
    }

    fn replace_configured_reachable_addresses(
        &mut self,
        node_id: Uuid,
        addresses: std::collections::BTreeSet<crate::ReachableAddress>,
    ) -> Result<CommandResult, DomainError> {
        self.node_reachability
            .entry(node_id)
            .or_default()
            .replace_configured(addresses);
        Ok(CommandResult::ConfiguredReachableAddressesReplaced(node_id))
    }

    fn renew_reachability_leases(
        &mut self,
        leases: Vec<crate::ReachableAddressLease>,
    ) -> Result<CommandResult, DomainError> {
        for lease in leases {
            let reachability = self.node_reachability.entry(lease.node_id).or_default();
            reachability.expire(lease.discovered_at_ms);
            reachability.renew(
                lease.address,
                lease.source,
                lease.discovered_at_ms,
                lease.expires_at_ms,
            );
        }
        Ok(CommandResult::ReachabilityLeasesRenewed)
    }

    fn verify_reachable_address(
        &mut self,
        node_id: Uuid,
        address: crate::ReachableAddress,
        verified_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        self.node_reachability
            .entry(node_id)
            .or_default()
            .verify(&address, verified_at_ms);
        Ok(CommandResult::ReachableAddressVerified(node_id))
    }

    fn put_secret(&mut self, secret: Secret) -> Result<CommandResult, DomainError> {
        secret.validate()?;
        let id = secret.id;
        self.secrets.insert(id, secret);
        Ok(CommandResult::SecretStored(id))
    }
}
