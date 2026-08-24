use super::*;

#[derive(Serialize, Deserialize)]
pub(crate) struct ApplicationStateV20260812 {
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
    #[serde(default)]
    join_tokens: BTreeMap<JoinTokenHash, u64>,
    #[serde(default)]
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
}

impl Default for ApplicationStateV20260812 {
    fn default() -> Self {
        ApplicationState::default().into()
    }
}

impl From<ApplicationStateV20260812> for ApplicationState {
    fn from(state: ApplicationStateV20260812) -> Self {
        let ApplicationStateV20260812 {
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
        } = state;
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
            node_reachability: BTreeMap::new(),
            connectivity_failures: BTreeSet::new(),
            connectivity_failure_counts: BTreeMap::new(),
            connectivity_success_count: 0,
            connectivity_degraded: None,
            join_token_reservations: BTreeMap::new(),
        }
    }
}

pub(crate) fn decode_v2026_08_12_application_state(
    payload: &[u8],
) -> Result<ApplicationState, postcard::Error> {
    postcard::from_bytes::<ApplicationStateV20260812>(payload).map(|state| {
        let mut state = ApplicationState::from(state);
        state.normalize_connectivity_state();
        state.normalize_alert_ids();
        state
    })
}

pub(crate) fn decode_v2026_08_19_application_state(
    payload: &[u8],
) -> Result<ApplicationState, postcard::Error> {
    v2026_08_19_reachability::decode(payload).map(|mut state| {
        state.normalize_connectivity_state();
        state.normalize_alert_ids();
        state
    })
}

#[cfg(test)]
pub(crate) fn encode_v2026_08_19_application_state(
    state: ApplicationState,
) -> Result<Vec<u8>, postcard::Error> {
    v2026_08_19_reachability::encode(state)
}

impl From<ApplicationState> for ApplicationStateV20260812 {
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
            ..
        } = state;
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
        }
    }
}

#[cfg(test)]
pub(crate) fn encode_v2026_08_12_application_state(
    state: ApplicationState,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_extend(&ApplicationStateV20260812::from(state), Vec::new())
}
