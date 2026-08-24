use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ApplicationState, JoinTokenReservation, ProcessedOperation};
use crate::DirectedRoute;
use crate::domain::connectivity::CONNECTIVITY_FAILURE_THRESHOLD;
use crate::domain::{
    Alert, AlertId, ApiToken, ApiTokenId, AvailabilityTransition, EvaluationAssignment,
    EvaluationAssignmentKey, EvaluationBatch, EvaluationId, EvaluationRollup, IdentityId,
    JoinTokenHash, NodeTargetState, NotificationChannel, NotificationChannelId, OperatorIdentity,
    Secret, SecretId, TargetId, TargetState, TrashedTarget,
};
use crate::reachability::NodeReachabilityV20260819;

#[derive(Serialize, Deserialize)]
struct JoinTokenReservationV20260819 {
    hash: JoinTokenHash,
    expires_at_ms: u64,
    reserved_until_ms: u64,
    limited: bool,
}

impl From<JoinTokenReservationV20260819> for JoinTokenReservation {
    fn from(value: JoinTokenReservationV20260819) -> Self {
        Self {
            hash: value.hash,
            expires_at_ms: value.expires_at_ms,
            reserved_until_ms: value.reserved_until_ms,
            limited: value.limited,
            operation_id: Uuid::nil(),
            readmission: None,
        }
    }
}

#[cfg(test)]
impl From<JoinTokenReservation> for JoinTokenReservationV20260819 {
    fn from(value: JoinTokenReservation) -> Self {
        Self {
            hash: value.hash,
            expires_at_ms: value.expires_at_ms,
            reserved_until_ms: value.reserved_until_ms,
            limited: value.limited,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ApplicationStateV20260819 {
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
    target_trash_retention_ms: u64,
    trashed_targets: BTreeMap<TargetId, TrashedTarget>,
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
    public_status_enabled: bool,
    node_reachability: BTreeMap<Uuid, NodeReachabilityV20260819>,
    connectivity_failures: BTreeSet<DirectedRoute>,
    join_token_reservations: BTreeMap<Uuid, JoinTokenReservationV20260819>,
}

impl From<ApplicationStateV20260819> for ApplicationState {
    fn from(state: ApplicationStateV20260819) -> Self {
        let ApplicationStateV20260819 {
            targets,
            node_targets,
            secrets,
            notification_channels,
            default_notification_channels,
            default_notifications_disabled,
            alerts,
            alert_acknowledgements,
            transitions: availability_transitions,
            history_retention_ms,
            history_rollup_retention_ms,
            history_rollups,
            target_trash_retention_ms,
            trashed_targets,
            processed_operations,
            latest_operation_at_ms,
            assignments,
            evaluation_batches,
            target_locations,
            join_tokens,
            join_token_uses,
            node_names,
            draining_nodes,
            identities,
            api_tokens,
            public_status_enabled,
            node_reachability,
            connectivity_failures,
            join_token_reservations,
        } = state;
        let connectivity_failure_counts = connectivity_failures
            .iter()
            .map(|route| (*route, CONNECTIVITY_FAILURE_THRESHOLD))
            .collect();
        let node_reachability = node_reachability
            .into_iter()
            .map(|(node_id, reachability)| (node_id, reachability.into()))
            .collect();
        let join_token_reservations = join_token_reservations
            .into_iter()
            .map(|(node_id, reservation)| (node_id, reservation.into()))
            .collect();
        Self {
            targets,
            node_targets,
            secrets,
            notification_channels,
            default_notification_channels,
            default_notifications_disabled,
            alerts,
            alert_acknowledgements,
            availability_transitions,
            history_retention_ms,
            history_rollup_retention_ms,
            history_rollups,
            target_trash_retention_ms,
            trashed_targets,
            processed_operations,
            latest_operation_at_ms,
            assignments,
            evaluation_batches,
            target_locations,
            join_tokens,
            join_token_uses,
            node_names,
            draining_nodes,
            identities,
            api_tokens,
            public_status_enabled,
            node_reachability,
            connectivity_failures,
            connectivity_failure_counts,
            connectivity_success_count: 0,
            connectivity_degraded: None,
            join_token_reservations,
        }
    }
}

pub(super) fn decode(payload: &[u8]) -> Result<ApplicationState, postcard::Error> {
    postcard::from_bytes::<ApplicationStateV20260819>(payload).map(Into::into)
}

#[cfg(test)]
impl From<ApplicationState> for ApplicationStateV20260819 {
    fn from(state: ApplicationState) -> Self {
        let ApplicationState {
            targets,
            node_targets,
            secrets,
            notification_channels,
            default_notification_channels,
            default_notifications_disabled,
            alerts,
            alert_acknowledgements,
            availability_transitions: transitions,
            history_retention_ms,
            history_rollup_retention_ms,
            history_rollups,
            target_trash_retention_ms,
            trashed_targets,
            processed_operations,
            latest_operation_at_ms,
            assignments,
            evaluation_batches,
            target_locations,
            join_tokens,
            join_token_uses,
            node_names,
            draining_nodes,
            identities,
            api_tokens,
            public_status_enabled,
            node_reachability,
            connectivity_failures,
            join_token_reservations,
            ..
        } = state;
        let node_reachability = node_reachability
            .into_iter()
            .map(|(node_id, reachability)| (node_id, reachability.into()))
            .collect();
        let join_token_reservations = join_token_reservations
            .into_iter()
            .map(|(node_id, reservation)| (node_id, reservation.into()))
            .collect();
        Self {
            targets,
            node_targets,
            secrets,
            notification_channels,
            default_notification_channels,
            default_notifications_disabled,
            alerts,
            alert_acknowledgements,
            transitions,
            history_retention_ms,
            history_rollup_retention_ms,
            history_rollups,
            target_trash_retention_ms,
            trashed_targets,
            processed_operations,
            latest_operation_at_ms,
            assignments,
            evaluation_batches,
            target_locations,
            join_tokens,
            join_token_uses,
            node_names,
            draining_nodes,
            identities,
            api_tokens,
            public_status_enabled,
            node_reachability,
            connectivity_failures,
            join_token_reservations,
        }
    }
}

#[cfg(test)]
pub(super) fn encode(state: ApplicationState) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_extend(&ApplicationStateV20260819::from(state), Vec::new())
}
