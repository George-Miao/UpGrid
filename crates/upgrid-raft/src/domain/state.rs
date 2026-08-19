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

#[derive(Serialize, Deserialize)]
#[serde(remote = "ApplicationState")]
struct ApplicationStateJson {
    targets: BTreeMap<TargetId, TargetState>,
    node_targets: BTreeMap<TargetId, NodeTargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    default_notification_channels: BTreeSet<NotificationChannelId>,
    default_notifications_disabled: BTreeSet<TargetId>,
    #[serde(with = "map_as_entries")]
    alerts: BTreeMap<AlertId, Alert>,
    #[serde(with = "map_as_entries")]
    alert_acknowledgements: BTreeMap<AlertId, u64>,
    #[serde(with = "map_as_entries")]
    transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
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
        ApplicationStateJson::deserialize(deserializer)
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

mod map_as_entries {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::marker::PhantomData;

    use serde::de::{SeqAccess, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Serialize,
        V: Serialize,
        S: Serializer,
    {
        serializer.collect_seq(map)
    }

    pub(super) fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(MapVisitor(PhantomData))
    }

    struct MapVisitor<K, V>(PhantomData<(K, V)>);

    impl<'de, K, V> Visitor<'de> for MapVisitor<K, V>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
    {
        type Value = BTreeMap<K, V>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a sequence of key-value entries")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut map = BTreeMap::new();
            while let Some((key, value)) = sequence.next_element()? {
                map.insert(key, value);
            }
            Ok(map)
        }
    }
}
