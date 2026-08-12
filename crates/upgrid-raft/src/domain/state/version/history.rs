use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreRollupApplicationState {
    pub(super) targets: BTreeMap<TargetId, TargetState>,
    pub(super) node_targets: BTreeMap<TargetId, NodeTargetState>,
    pub(super) secrets: BTreeMap<SecretId, Secret>,
    pub(super) notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    pub(super) default_notification_channels: BTreeSet<NotificationChannelId>,
    pub(super) default_notifications_disabled: BTreeSet<TargetId>,
    pub(super) alerts: BTreeMap<AlertId, Alert>,
    pub(super) alert_acknowledgements: BTreeMap<AlertId, u64>,
    pub(super) transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    pub(super) history_retention_ms: u64,
    pub(super) processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    pub(super) latest_operation_at_ms: u64,
    pub(super) assignments: BTreeMap<EvaluationAssignmentKey, EvaluationAssignment>,
    pub(super) evaluation_batches: BTreeMap<EvaluationId, EvaluationBatch>,
    pub(super) target_locations: BTreeMap<TargetId, u16>,
    pub(super) join_tokens: BTreeMap<JoinTokenHash, u64>,
    pub(super) join_token_uses: BTreeMap<JoinTokenHash, u64>,
    pub(super) node_names: BTreeMap<Uuid, String>,
    pub(super) draining_nodes: BTreeSet<Uuid>,
    pub(super) identities: BTreeMap<IdentityId, OperatorIdentity>,
    pub(super) api_tokens: BTreeMap<ApiTokenId, ApiToken>,
}

impl From<PreRollupApplicationState> for ApplicationState {
    fn from(previous: PreRollupApplicationState) -> Self {
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
            history_rollup_retention_ms: DEFAULT_HISTORY_ROLLUP_RETENTION_MS,
            history_rollups: BTreeMap::new(),
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
impl From<ApplicationState> for PreRollupApplicationState {
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
