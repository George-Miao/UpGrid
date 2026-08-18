#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationState {
    pub targets: BTreeMap<TargetId, TargetState>,
    pub node_targets: BTreeMap<TargetId, NodeTargetState>,
    pub secrets: BTreeMap<SecretId, Secret>,
    pub notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    pub default_notification_channels: BTreeSet<NotificationChannelId>,
    pub default_notifications_disabled: BTreeSet<TargetId>,
    pub alerts: BTreeMap<AlertId, Alert>,
    pub alert_acknowledgements: BTreeMap<AlertId, u64>,
    pub transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    pub history_retention_ms: u64,
    pub history_rollup_retention_ms: u64,
    pub history_rollups: BTreeMap<TargetId, BTreeMap<u64, EvaluationRollup>>,
    pub target_trash_retention_ms: u64,
    pub trashed_targets: BTreeMap<TargetId, TrashedTarget>,
    pub(super) processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    pub(super) latest_operation_at_ms: u64,
    pub assignments: BTreeMap<EvaluationAssignmentKey, EvaluationAssignment>,
    pub(super) evaluation_batches: BTreeMap<EvaluationId, EvaluationBatch>,
    pub(super) target_locations: BTreeMap<TargetId, u16>,
    #[serde(default)]
    pub join_tokens: BTreeMap<JoinTokenHash, u64>,
    #[serde(default)]
    pub join_token_uses: BTreeMap<JoinTokenHash, u64>,
    #[serde(default)]
    pub node_names: BTreeMap<Uuid, String>,
    #[serde(default)]
    pub draining_nodes: BTreeSet<Uuid>,
    #[serde(default)]
    pub identities: BTreeMap<IdentityId, OperatorIdentity>,
    #[serde(default)]
    pub api_tokens: BTreeMap<ApiTokenId, ApiToken>,
    #[serde(default)]
    pub public_status_enabled: bool,
}

impl ApplicationState {
    pub fn target_location_count(&self, target_id: TargetId) -> u16 {
        self.target_locations.get(&target_id).copied().unwrap_or(1)
    }

    pub fn has_evaluation_assignment(&self, evaluation_id: EvaluationId) -> bool {
        self.assignments.keys().any(|key| key.id == evaluation_id)
    }

    pub fn expected_evaluation_results(&self, evaluation_id: EvaluationId) -> Option<u16> {
        self.evaluation_batches
            .get(&evaluation_id)
            .map(|batch| batch.expected_results)
    }

    pub fn evaluation_results(
        &self,
        evaluation_id: EvaluationId,
    ) -> Option<&BTreeMap<Uuid, Evaluation>> {
        self.evaluation_batches
            .get(&evaluation_id)
            .map(|batch| &batch.results)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ProcessedOperation {
    pub(super) submitted_at_ms: u64,
    pub(super) result: Result<CommandResult, DomainError>,
}

impl Default for ApplicationState {
    fn default() -> Self {
        Self {
            targets: BTreeMap::new(),
            node_targets: BTreeMap::new(),
            secrets: BTreeMap::new(),
            notification_channels: BTreeMap::new(),
            default_notification_channels: BTreeSet::new(),
            default_notifications_disabled: BTreeSet::new(),
            alerts: BTreeMap::new(),
            alert_acknowledgements: BTreeMap::new(),
            transitions: BTreeMap::new(),
            assignments: BTreeMap::new(),
            evaluation_batches: BTreeMap::new(),
            history_rollup_retention_ms: DEFAULT_HISTORY_ROLLUP_RETENTION_MS,
            history_rollups: BTreeMap::new(),
            target_trash_retention_ms: DEFAULT_TARGET_TRASH_RETENTION_MS,
            trashed_targets: BTreeMap::new(),
            target_locations: BTreeMap::new(),
            join_tokens: BTreeMap::new(),
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            identities: BTreeMap::new(),
            api_tokens: BTreeMap::new(),
            public_status_enabled: false,
            history_retention_ms: DEFAULT_HISTORY_RETENTION_MS,
            processed_operations: BTreeMap::new(),
            latest_operation_at_ms: 0,
        }
    }
}
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    Alert, AlertId, ApiToken, ApiTokenId, AvailabilityTransition, CommandResult,
    DEFAULT_HISTORY_RETENTION_MS, DEFAULT_HISTORY_ROLLUP_RETENTION_MS,
    DEFAULT_TARGET_TRASH_RETENTION_MS, DomainError, Evaluation, EvaluationAssignment,
    EvaluationAssignmentKey, EvaluationBatch, EvaluationId, EvaluationRollup, IdentityId,
    JoinTokenHash, NodeTargetState, NotificationChannel, NotificationChannelId, OperatorIdentity,
    Secret, SecretId, TargetId, TargetState, TrashedTarget,
};
