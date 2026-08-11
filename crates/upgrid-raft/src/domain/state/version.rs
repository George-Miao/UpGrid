use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::*;

mod assertion;
pub(crate) use assertion::*;
mod acknowledgement;
pub(crate) use acknowledgement::*;
mod drain;
pub(crate) use drain::*;
mod tls;
pub(crate) use tls::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreAuthApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    node_targets: BTreeMap<TargetId, NodeTargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    default_notification_channels: BTreeSet<NotificationChannelId>,
    default_notifications_disabled: BTreeSet<TargetId>,
    alerts: BTreeMap<AlertId, Alert>,
    transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
    join_tokens: BTreeMap<JoinTokenHash, u64>,
    join_token_uses: BTreeMap<JoinTokenHash, u64>,
    node_names: BTreeMap<Uuid, String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TransitionApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    alerts: BTreeMap<AlertId, Alert>,
    transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
    join_tokens: BTreeMap<JoinTokenHash, u64>,
    join_token_uses: BTreeMap<JoinTokenHash, u64>,
    node_names: BTreeMap<Uuid, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DefaultChannelApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    default_notification_channels: BTreeSet<NotificationChannelId>,
    default_notifications_disabled: BTreeSet<TargetId>,
    alerts: BTreeMap<AlertId, Alert>,
    transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
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
            node_targets: BTreeMap::new(),
            secrets: legacy.secrets,
            notification_channels: legacy.notification_channels,
            default_notification_channels: BTreeSet::new(),
            default_notifications_disabled: BTreeSet::new(),
            alerts: legacy.alerts,
            alert_acknowledgements: BTreeMap::new(),
            transitions: BTreeMap::new(),
            history_retention_ms: legacy.history_retention_ms,
            processed_operations: legacy.processed_operations,
            latest_operation_at_ms: legacy.latest_operation_at_ms,
            assignments: BTreeMap::new(),
            join_tokens: BTreeMap::new(),
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
            identities: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            api_tokens: BTreeMap::new(),
        }
    }
}

impl From<PreviousApplicationState> for ApplicationState {
    fn from(previous: PreviousApplicationState) -> Self {
        Self {
            targets: previous.targets,
            node_targets: BTreeMap::new(),
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: BTreeSet::new(),
            default_notifications_disabled: BTreeSet::new(),
            alerts: previous.alerts,
            alert_acknowledgements: BTreeMap::new(),
            transitions: BTreeMap::new(),
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: BTreeMap::new(),
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
            identities: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            api_tokens: BTreeMap::new(),
        }
    }
}

impl From<TokenApplicationState> for ApplicationState {
    fn from(previous: TokenApplicationState) -> Self {
        Self {
            targets: previous.targets,
            node_targets: BTreeMap::new(),
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: BTreeSet::new(),
            default_notifications_disabled: BTreeSet::new(),
            alerts: previous.alerts,
            alert_acknowledgements: BTreeMap::new(),
            transitions: BTreeMap::new(),
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: BTreeMap::new(),
            node_names: BTreeMap::new(),
            identities: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            api_tokens: BTreeMap::new(),
        }
    }
}

impl From<NamedApplicationState> for ApplicationState {
    fn from(previous: NamedApplicationState) -> Self {
        Self {
            targets: previous.targets,
            node_targets: BTreeMap::new(),
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: BTreeSet::new(),
            default_notifications_disabled: BTreeSet::new(),
            alerts: previous.alerts,
            alert_acknowledgements: BTreeMap::new(),
            transitions: BTreeMap::new(),
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: previous.join_token_uses,
            node_names: previous.node_names,
            identities: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            api_tokens: BTreeMap::new(),
        }
    }
}

impl From<TransitionApplicationState> for ApplicationState {
    fn from(previous: TransitionApplicationState) -> Self {
        Self {
            targets: previous.targets,
            node_targets: BTreeMap::new(),
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: BTreeSet::new(),
            default_notifications_disabled: BTreeSet::new(),
            alerts: previous.alerts,
            alert_acknowledgements: BTreeMap::new(),
            transitions: previous.transitions,
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: previous.join_token_uses,
            node_names: previous.node_names,
            identities: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            api_tokens: BTreeMap::new(),
        }
    }
}

impl From<DefaultChannelApplicationState> for ApplicationState {
    fn from(previous: DefaultChannelApplicationState) -> Self {
        Self {
            targets: previous.targets,
            node_targets: BTreeMap::new(),
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: previous.default_notification_channels,
            default_notifications_disabled: previous.default_notifications_disabled,
            alerts: previous.alerts,
            alert_acknowledgements: BTreeMap::new(),
            transitions: previous.transitions,
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: previous.join_token_uses,
            node_names: previous.node_names,
            identities: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            api_tokens: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for DefaultChannelApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            default_notification_channels: current.default_notification_channels,
            default_notifications_disabled: current.default_notifications_disabled,
            alerts: current.alerts,
            transitions: current.transitions,
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
impl From<ApplicationState> for TransitionApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            alerts: current.alerts,
            transitions: current.transitions,
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

impl From<PreAuthApplicationState> for ApplicationState {
    fn from(previous: PreAuthApplicationState) -> Self {
        Self {
            targets: previous.targets,
            node_targets: previous.node_targets,
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: previous.default_notification_channels,
            default_notifications_disabled: previous.default_notifications_disabled,
            alerts: previous.alerts,
            alert_acknowledgements: BTreeMap::new(),
            transitions: previous.transitions,
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: previous.join_token_uses,
            node_names: previous.node_names,
            identities: BTreeMap::new(),
            draining_nodes: BTreeSet::new(),
            api_tokens: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for PreAuthApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            node_targets: current.node_targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            default_notification_channels: current.default_notification_channels,
            default_notifications_disabled: current.default_notifications_disabled,
            alerts: current.alerts,
            transitions: current.transitions,
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
