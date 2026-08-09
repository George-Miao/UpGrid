#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationState {
    pub targets: BTreeMap<TargetId, TargetState>,
    pub secrets: BTreeMap<SecretId, Secret>,
    pub notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    pub alerts: BTreeMap<AlertId, Alert>,
    pub transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    pub history_retention_ms: u64,
    pub(super) processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    pub(super) latest_operation_at_ms: u64,
    #[serde(default)]
    pub assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
    #[serde(default)]
    pub join_tokens: BTreeMap<JoinTokenHash, u64>,
    #[serde(default)]
    pub join_token_uses: BTreeMap<JoinTokenHash, u64>,
    #[serde(default)]
    pub node_names: BTreeMap<Uuid, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ProcessedOperation {
    pub(super) submitted_at_ms: u64,
    pub(super) result: Result<CommandResult, DomainError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LegacyApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    alerts: BTreeMap<AlertId, Alert>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreviousApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    alerts: BTreeMap<AlertId, Alert>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TokenApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    alerts: BTreeMap<AlertId, Alert>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
    join_tokens: BTreeMap<JoinTokenHash, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NamedApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    alerts: BTreeMap<AlertId, Alert>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
    join_tokens: BTreeMap<JoinTokenHash, u64>,
    join_token_uses: BTreeMap<JoinTokenHash, u64>,
    node_names: BTreeMap<Uuid, String>,
}

#[cfg(test)]
impl From<ApplicationState> for LegacyApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            alerts: current.alerts,
            history_retention_ms: current.history_retention_ms,
            processed_operations: current.processed_operations,
            latest_operation_at_ms: current.latest_operation_at_ms,
        }
    }
}

impl From<LegacyApplicationState> for ApplicationState {
    fn from(legacy: LegacyApplicationState) -> Self {
        Self {
            targets: legacy.targets,
            secrets: legacy.secrets,
            notification_channels: legacy.notification_channels,
            alerts: legacy.alerts,
            transitions: BTreeMap::new(),
            history_retention_ms: legacy.history_retention_ms,
            processed_operations: legacy.processed_operations,
            latest_operation_at_ms: legacy.latest_operation_at_ms,
            assignments: BTreeMap::new(),
            join_tokens: BTreeMap::new(),
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
        }
    }
}

impl From<PreviousApplicationState> for ApplicationState {
    fn from(previous: PreviousApplicationState) -> Self {
        Self {
            targets: previous.targets,
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            alerts: previous.alerts,
            transitions: BTreeMap::new(),
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: BTreeMap::new(),
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
        }
    }
}

impl From<TokenApplicationState> for ApplicationState {
    fn from(previous: TokenApplicationState) -> Self {
        Self {
            targets: previous.targets,
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            alerts: previous.alerts,
            transitions: BTreeMap::new(),
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
        }
    }
}

impl From<NamedApplicationState> for ApplicationState {
    fn from(previous: NamedApplicationState) -> Self {
        Self {
            targets: previous.targets,
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            alerts: previous.alerts,
            transitions: BTreeMap::new(),
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: previous.join_token_uses,
            node_names: previous.node_names,
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for NamedApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            alerts: current.alerts,
            history_retention_ms: current.history_retention_ms,
            processed_operations: current.processed_operations,
            latest_operation_at_ms: current.latest_operation_at_ms,
            assignments: current.assignments,
            join_tokens: current.join_tokens,
            join_token_uses: current.join_token_uses,
            node_names: current.node_names,
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for TokenApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            alerts: current.alerts,
            history_retention_ms: current.history_retention_ms,
            processed_operations: current.processed_operations,
            latest_operation_at_ms: current.latest_operation_at_ms,
            assignments: current.assignments,
            join_tokens: current.join_tokens,
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for PreviousApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            alerts: current.alerts,
            history_retention_ms: current.history_retention_ms,
            processed_operations: current.processed_operations,
            latest_operation_at_ms: current.latest_operation_at_ms,
            assignments: current.assignments,
        }
    }
}

impl Default for ApplicationState {
    fn default() -> Self {
        Self {
            targets: BTreeMap::new(),
            secrets: BTreeMap::new(),
            notification_channels: BTreeMap::new(),
            alerts: BTreeMap::new(),
            transitions: BTreeMap::new(),
            assignments: BTreeMap::new(),
            join_tokens: BTreeMap::new(),
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
            history_retention_ms: DEFAULT_HISTORY_RETENTION_MS,
            processed_operations: BTreeMap::new(),
            latest_operation_at_ms: 0,
        }
    }
}
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    Alert, AlertId, AvailabilityTransition, CommandResult, DEFAULT_HISTORY_RETENTION_MS,
    DomainError, EvaluationAssignment, EvaluationId, JoinTokenHash, NotificationChannel,
    NotificationChannelId, Secret, SecretId, TargetId, TargetState,
};
