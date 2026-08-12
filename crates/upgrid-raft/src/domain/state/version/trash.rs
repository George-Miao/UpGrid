use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreTrashApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    node_targets: BTreeMap<TargetId, NodeTargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    default_notification_channels: BTreeSet<NotificationChannelId>,
    default_notifications_disabled: BTreeSet<TargetId>,
    alerts: BTreeMap<AlertId, Alert>,
    alert_acknowledgements: BTreeMap<AlertId, u64>,
    transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    history_retention_ms: u64,
    history_rollup_retention_ms: u64,
    history_rollups: BTreeMap<TargetId, BTreeMap<u64, EvaluationRollup>>,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationAssignmentKey, EvaluationAssignment>,
    evaluation_batches: BTreeMap<EvaluationId, EvaluationBatch>,
    target_locations: BTreeMap<TargetId, u16>,
    join_tokens: BTreeMap<JoinTokenHash, u64>,
    join_token_uses: BTreeMap<JoinTokenHash, u64>,
    node_names: BTreeMap<Uuid, String>,
    draining_nodes: BTreeSet<Uuid>,
    identities: BTreeMap<IdentityId, OperatorIdentity>,
    api_tokens: BTreeMap<ApiTokenId, ApiToken>,
}

impl From<PreTrashApplicationState> for ApplicationState {
    fn from(previous: PreTrashApplicationState) -> Self {
        Self {
            targets: previous.targets,
            node_targets: previous.node_targets,
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: previous.default_notification_channels,
            default_notifications_disabled: previous.default_notifications_disabled,
            alerts: previous.alerts,
            alert_acknowledgements: previous.alert_acknowledgements,
            transitions: previous.transitions,
            history_retention_ms: previous.history_retention_ms,
            history_rollup_retention_ms: previous.history_rollup_retention_ms,
            history_rollups: previous.history_rollups,
            target_trash_retention_ms: DEFAULT_TARGET_TRASH_RETENTION_MS,
            trashed_targets: BTreeMap::new(),
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            evaluation_batches: previous.evaluation_batches,
            target_locations: previous.target_locations,
            join_tokens: previous.join_tokens,
            join_token_uses: previous.join_token_uses,
            node_names: previous.node_names,
            draining_nodes: previous.draining_nodes,
            identities: previous.identities,
            api_tokens: previous.api_tokens,
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for PreTrashApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            node_targets: current.node_targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            default_notification_channels: current.default_notification_channels,
            default_notifications_disabled: current.default_notifications_disabled,
            alerts: current.alerts,
            alert_acknowledgements: current.alert_acknowledgements,
            transitions: current.transitions,
            history_retention_ms: current.history_retention_ms,
            history_rollup_retention_ms: current.history_rollup_retention_ms,
            history_rollups: current.history_rollups,
            processed_operations: current.processed_operations,
            latest_operation_at_ms: current.latest_operation_at_ms,
            assignments: current.assignments,
            evaluation_batches: current.evaluation_batches,
            target_locations: current.target_locations,
            join_tokens: current.join_tokens,
            join_token_uses: current.join_token_uses,
            node_names: current.node_names,
            draining_nodes: current.draining_nodes,
            identities: current.identities,
            api_tokens: current.api_tokens,
        }
    }
}
