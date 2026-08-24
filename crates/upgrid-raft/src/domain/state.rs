use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::connectivity::CONNECTIVITY_FAILURE_THRESHOLD;
use super::{
    Alert, AlertId, AlertKind, ApiToken, ApiTokenId, AvailabilityState, AvailabilityTransition,
    CommandResult, DEFAULT_HISTORY_RETENTION_MS, DEFAULT_HISTORY_ROLLUP_RETENTION_MS,
    DEFAULT_TARGET_TRASH_RETENTION_MS, DomainError, Evaluation, EvaluationAssignment,
    EvaluationAssignmentKey, EvaluationBatch, EvaluationId, EvaluationRollup, IdentityId,
    JoinTokenHash, NodeTargetState, NotificationChannel, NotificationChannelId, OperatorIdentity,
    Secret, SecretId, TargetId, TargetState, TrashedTarget, map_as_entries,
};
use crate::{DirectedRoute, NodeReachability, ReachableAddress};

mod migration;
mod v2026_08_19_connectivity_alerts;
mod v2026_08_19_reachability;
pub(crate) use migration::{
    ApplicationStateV20260812, decode_v2026_08_12_application_state,
    decode_v2026_08_19_application_state,
};
#[cfg(test)]
pub(crate) use migration::{
    encode_v2026_08_12_application_state, encode_v2026_08_19_application_state,
};
pub(crate) use v2026_08_19_connectivity_alerts::decode as decode_v2026_08_19_connectivity_alerts_application_state;
#[cfg(test)]
pub(crate) use v2026_08_19_connectivity_alerts::encode as encode_v2026_08_19_connectivity_alerts_application_state;

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
    pub availability_transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
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
    #[serde(default)]
    pub node_reachability: BTreeMap<Uuid, NodeReachability>,
    #[serde(default)]
    pub connectivity_failures: BTreeSet<DirectedRoute>,
    #[serde(default)]
    pub(super) connectivity_failure_counts: BTreeMap<DirectedRoute, u8>,
    #[serde(default)]
    pub(super) connectivity_success_count: u8,
    #[serde(default)]
    pub(super) connectivity_degraded: Option<bool>,
    #[serde(default)]
    pub(super) join_token_reservations: BTreeMap<Uuid, JoinTokenReservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct JoinTokenReservation {
    pub(super) hash: JoinTokenHash,
    pub(super) expires_at_ms: u64,
    pub(super) reserved_until_ms: u64,
    pub(super) limited: bool,
    #[serde(default)]
    pub(super) operation_id: Uuid,
    #[serde(default)]
    pub(super) readmission: Option<ReadmissionRollback>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadmissionRollback {
    pub reachability: Option<NodeReachability>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExpiredJoinReservation {
    pub(crate) node_id: Uuid,
    pub(crate) operation_id: Uuid,
    pub(crate) readmission: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ApplicationState")]
struct ApplicationStateJson {
    targets: BTreeMap<TargetId, TargetState>,
    node_targets: BTreeMap<TargetId, NodeTargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    default_notification_channels: BTreeSet<NotificationChannelId>,
    default_notifications_disabled: BTreeSet<TargetId>,
    #[serde(alias = "alert_deliveries", with = "map_as_entries")]
    alerts: BTreeMap<AlertId, Alert>,
    #[serde(with = "map_as_entries")]
    alert_acknowledgements: BTreeMap<AlertId, u64>,
    #[serde(alias = "alert_events", alias = "transitions", with = "map_as_entries")]
    availability_transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    history_retention_ms: u64,
    history_rollup_retention_ms: u64,
    history_rollups: BTreeMap<TargetId, BTreeMap<u64, EvaluationRollup>>,
    target_trash_retention_ms: u64,
    trashed_targets: BTreeMap<TargetId, TrashedTarget>,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    #[serde(with = "map_as_entries")]
    assignments: BTreeMap<EvaluationAssignmentKey, EvaluationAssignment>,
    #[serde(with = "map_as_entries")]
    evaluation_batches: BTreeMap<EvaluationId, EvaluationBatch>,
    target_locations: BTreeMap<TargetId, u16>,
    #[serde(default, with = "map_as_entries")]
    join_tokens: BTreeMap<JoinTokenHash, u64>,
    #[serde(default, with = "map_as_entries")]
    join_token_uses: BTreeMap<JoinTokenHash, u64>,
    #[serde(default)]
    node_names: BTreeMap<Uuid, String>,
    #[serde(default)]
    draining_nodes: BTreeSet<Uuid>,
    #[serde(default)]
    identities: BTreeMap<IdentityId, OperatorIdentity>,
    #[serde(default)]
    api_tokens: BTreeMap<ApiTokenId, ApiToken>,
    #[serde(default)]
    public_status_enabled: bool,
    #[serde(default)]
    node_reachability: BTreeMap<Uuid, NodeReachability>,
    #[serde(default)]
    connectivity_failures: BTreeSet<DirectedRoute>,
    #[serde(default, with = "map_as_entries")]
    connectivity_failure_counts: BTreeMap<DirectedRoute, u8>,
    #[serde(default)]
    connectivity_success_count: u8,
    #[serde(default)]
    connectivity_degraded: Option<bool>,
    #[serde(default)]
    join_token_reservations: BTreeMap<Uuid, JoinTokenReservation>,
}

impl ApplicationState {
    pub fn target_location_count(&self, target_id: TargetId) -> u16 {
        self.target_locations.get(&target_id).copied().unwrap_or(1)
    }

    pub fn preferred_reachable_address(
        &self,
        node_id: Uuid,
        source_node_id: Uuid,
        now_ms: u64,
    ) -> Option<&ReachableAddress> {
        self.node_reachability
            .get(&node_id)
            .and_then(|reachability| {
                reachability.preferred_reachable_address(source_node_id, now_ms)
            })
    }

    pub fn preferred_published_address(
        &self,
        node_id: Uuid,
        now_ms: u64,
    ) -> Option<&ReachableAddress> {
        self.node_reachability
            .get(&node_id)
            .and_then(|reachability| reachability.preferred_published_address(now_ms))
    }

    pub fn connectivity_degraded(&self) -> bool {
        self.connectivity_degraded.unwrap_or(false) || !self.connectivity_failures.is_empty()
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

    pub(crate) fn expired_join_reservations(&self, now_ms: u64) -> Vec<ExpiredJoinReservation> {
        self.join_token_reservations
            .iter()
            .filter_map(|(node_id, reservation)| {
                (now_ms > reservation.reserved_until_ms).then_some(ExpiredJoinReservation {
                    node_id: *node_id,
                    operation_id: reservation.operation_id,
                    readmission: reservation.readmission.is_some(),
                })
            })
            .collect()
    }

    pub(crate) fn active_join_reservation(
        &self,
        node_id: Uuid,
        operation_id: Uuid,
        submitted_at_ms: u64,
    ) -> Option<bool> {
        self.join_token_reservations
            .get(&node_id)
            .filter(|reservation| {
                reservation.operation_id == operation_id
                    && submitted_at_ms <= reservation.reserved_until_ms
            })
            .map(|reservation| reservation.readmission.is_some())
    }

    pub(crate) fn retain_member_reachability(&mut self, members: &BTreeSet<Uuid>) -> bool {
        let degraded = self.connectivity_degraded();
        let reachability_count = self.node_reachability.len();
        let failure_count = self.connectivity_failures.len();
        let pending_failure_count = self.connectivity_failure_counts.len();
        self.node_reachability
            .retain(|node_id, _| members.contains(node_id));
        self.connectivity_failures.retain(|route| {
            members.contains(&route.source) && members.contains(&route.destination)
        });
        self.connectivity_failure_counts.retain(|route, _| {
            members.contains(&route.source) && members.contains(&route.destination)
        });
        let changed = reachability_count != self.node_reachability.len()
            || failure_count != self.connectivity_failures.len()
            || pending_failure_count != self.connectivity_failure_counts.len();
        if changed {
            self.connectivity_degraded = Some(degraded);
            self.connectivity_success_count = 0;
        }
        changed
    }

    pub(crate) fn serialize_database_json<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        ApplicationStateJson::serialize(self, serializer)
    }

    pub(crate) fn deserialize_database_json<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut state = ApplicationStateJson::deserialize(deserializer)?;
        state.normalize_connectivity_state();
        state.normalize_alert_ids();
        Ok(state)
    }

    pub(crate) fn normalize_connectivity_state(&mut self) {
        self.normalize_connectivity_failure_counts();
        if self.connectivity_degraded.is_some() {
            return;
        }
        let latest_transition = self
            .availability_transitions
            .iter()
            .rev()
            .find(|(id, _)| id.target_id == TargetId(Uuid::nil()));
        let degraded = !self.connectivity_failures.is_empty()
            || latest_transition.is_some_and(|(_, transition)| transition.kind == AlertKind::Down);
        self.connectivity_degraded = Some(degraded);
    }

    fn normalize_connectivity_failure_counts(&mut self) {
        for route in &self.connectivity_failures {
            self.connectivity_failure_counts
                .entry(*route)
                .and_modify(|count| *count = (*count).max(CONNECTIVITY_FAILURE_THRESHOLD))
                .or_insert(CONNECTIVITY_FAILURE_THRESHOLD);
        }
    }

    pub(crate) fn normalize_alert_ids(&mut self) {
        let transition_kinds = self
            .availability_transitions
            .iter()
            .map(|(id, transition)| (*id, transition.kind))
            .collect::<BTreeMap<_, _>>();
        let mut alert_kinds = BTreeMap::new();
        let mut alerts = BTreeMap::new();
        for (_, mut alert) in std::mem::take(&mut self.alerts) {
            let kind = transition_kinds
                .get(&alert.evaluation.id)
                .copied()
                .unwrap_or(if alert.evaluation.succeeded {
                    AlertKind::Recovered
                } else {
                    AlertKind::Down
                });
            alert.id.kind = kind;
            alert_kinds.insert(
                (
                    alert.id.target_id,
                    alert.id.channel_id,
                    alert.id.evaluation_scheduled_at_ms,
                ),
                kind,
            );
            alerts.insert(alert.id, alert);
        }
        self.alerts = alerts;

        let infer_kind = |id: &AlertId| {
            transition_kinds
                .get(&EvaluationId {
                    target_id: id.target_id,
                    scheduled_at_ms: id.evaluation_scheduled_at_ms,
                })
                .or_else(|| {
                    alert_kinds.get(&(id.target_id, id.channel_id, id.evaluation_scheduled_at_ms))
                })
                .copied()
                .unwrap_or(id.kind)
        };
        self.alert_acknowledgements = std::mem::take(&mut self.alert_acknowledgements)
            .into_iter()
            .map(|(mut id, acknowledged_at_ms)| {
                id.kind = infer_kind(&id);
                (id, acknowledged_at_ms)
            })
            .collect();
        for operation in self.processed_operations.values_mut() {
            match &mut operation.result {
                Ok(CommandResult::EvaluationAccepted {
                    availability,
                    alerts,
                })
                | Ok(CommandResult::NodeEvaluationAccepted {
                    availability,
                    alerts,
                }) => {
                    let kind = if *availability == AvailabilityState::Up {
                        AlertKind::Recovered
                    } else {
                        AlertKind::Down
                    };
                    for id in alerts {
                        id.kind = kind;
                    }
                }
                Ok(CommandResult::AlertUpdated(id)) => id.kind = infer_kind(id),
                _ => {}
            }
        }
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
            availability_transitions: BTreeMap::new(),
            assignments: BTreeMap::new(),
            evaluation_batches: BTreeMap::new(),
            history_rollup_retention_ms: DEFAULT_HISTORY_ROLLUP_RETENTION_MS,
            history_rollups: BTreeMap::new(),
            target_trash_retention_ms: DEFAULT_TARGET_TRASH_RETENTION_MS,
            trashed_targets: BTreeMap::new(),
            target_locations: BTreeMap::new(),
            join_tokens: BTreeMap::new(),
            join_token_uses: BTreeMap::new(),
            join_token_reservations: BTreeMap::new(),
            node_names: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            identities: BTreeMap::new(),
            api_tokens: BTreeMap::new(),
            public_status_enabled: false,
            node_reachability: BTreeMap::new(),
            connectivity_failures: BTreeSet::new(),
            connectivity_failure_counts: BTreeMap::new(),
            connectivity_success_count: 0,
            connectivity_degraded: Some(false),
            history_retention_ms: DEFAULT_HISTORY_RETENTION_MS,
            processed_operations: BTreeMap::new(),
            latest_operation_at_ms: 0,
        }
    }
}
