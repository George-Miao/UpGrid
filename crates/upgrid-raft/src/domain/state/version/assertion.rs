use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use super::*;
use crate::domain::{
    AvailabilityState, ConfigValue, Evaluation, EvaluationPolicy, HttpAssertion, HttpTarget,
    StatusRange, Target,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreAssertionHttpTarget {
    url: Url,
    method: String,
    headers: BTreeMap<String, ConfigValue>,
    body: Option<ConfigValue>,
    accepted_statuses: Vec<StatusRange>,
    follow_redirects: bool,
    max_redirects: u8,
    body_contains: Option<String>,
    skip_tls_verification: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreAssertionTarget {
    id: TargetId,
    name: String,
    http: PreAssertionHttpTarget,
    policy: EvaluationPolicy,
    notification_channels: BTreeSet<NotificationChannelId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreAssertionTargetState {
    target: PreAssertionTarget,
    availability: AvailabilityState,
    consecutive_failures: u32,
    latest_evaluation: Option<Evaluation>,
    history: BTreeMap<u64, Evaluation>,
    paused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreAssertionApplicationState {
    targets: BTreeMap<TargetId, PreAssertionTargetState>,
    node_targets: BTreeMap<TargetId, NodeTargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    default_notification_channels: BTreeSet<NotificationChannelId>,
    default_notifications_disabled: BTreeSet<TargetId>,
    alerts: BTreeMap<AlertId, Alert>,
    alert_acknowledgements: BTreeMap<AlertId, u64>,
    transitions: BTreeMap<EvaluationId, AvailabilityTransition>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
    join_tokens: BTreeMap<JoinTokenHash, u64>,
    join_token_uses: BTreeMap<JoinTokenHash, u64>,
    node_names: BTreeMap<Uuid, String>,
    draining_nodes: BTreeSet<Uuid>,
    identities: BTreeMap<IdentityId, OperatorIdentity>,
    api_tokens: BTreeMap<ApiTokenId, ApiToken>,
}

impl From<PreAssertionApplicationState> for ApplicationState {
    fn from(previous: PreAssertionApplicationState) -> Self {
        Self {
            targets: previous
                .targets
                .into_iter()
                .map(|(id, state)| (id, state.into()))
                .collect(),
            node_targets: previous.node_targets,
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            default_notification_channels: previous.default_notification_channels,
            default_notifications_disabled: previous.default_notifications_disabled,
            alerts: previous.alerts,
            alert_acknowledgements: previous.alert_acknowledgements,
            transitions: previous.transitions,
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: previous.join_tokens,
            join_token_uses: previous.join_token_uses,
            node_names: previous.node_names,
            draining_nodes: previous.draining_nodes,
            identities: previous.identities,
            api_tokens: previous.api_tokens,
        }
    }
}

impl From<PreAssertionTargetState> for TargetState {
    fn from(previous: PreAssertionTargetState) -> Self {
        Self {
            target: previous.target.into(),
            availability: previous.availability,
            consecutive_failures: previous.consecutive_failures,
            latest_evaluation: previous.latest_evaluation,
            history: previous.history,
            paused: previous.paused,
        }
    }
}

impl From<PreAssertionTarget> for Target {
    fn from(previous: PreAssertionTarget) -> Self {
        let assertions = previous
            .http
            .body_contains
            .into_iter()
            .map(|value| HttpAssertion::BodyContains { value })
            .collect();
        Self {
            id: previous.id,
            name: previous.name,
            http: HttpTarget {
                url: previous.http.url,
                method: previous.http.method,
                headers: previous.http.headers,
                body: previous.http.body,
                accepted_statuses: previous.http.accepted_statuses,
                follow_redirects: previous.http.follow_redirects,
                max_redirects: previous.http.max_redirects,
                assertions,
                skip_tls_verification: previous.http.skip_tls_verification,
            },
            policy: previous.policy,
            notification_channels: previous.notification_channels,
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for PreAssertionApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current
                .targets
                .into_iter()
                .map(|(id, state)| (id, state.into()))
                .collect(),
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
            join_tokens: current.join_tokens,
            join_token_uses: current.join_token_uses,
            node_names: current.node_names,
            draining_nodes: current.draining_nodes,
            identities: current.identities,
            api_tokens: current.api_tokens,
        }
    }
}

#[cfg(test)]
impl From<TargetState> for PreAssertionTargetState {
    fn from(current: TargetState) -> Self {
        Self {
            target: current.target.into(),
            availability: current.availability,
            consecutive_failures: current.consecutive_failures,
            latest_evaluation: current.latest_evaluation,
            history: current.history,
            paused: current.paused,
        }
    }
}

#[cfg(test)]
impl From<Target> for PreAssertionTarget {
    fn from(current: Target) -> Self {
        let body_contains =
            current
                .http
                .assertions
                .into_iter()
                .find_map(|assertion| match assertion {
                    HttpAssertion::BodyContains { value } => Some(value),
                    _ => None,
                });
        Self {
            id: current.id,
            name: current.name,
            http: PreAssertionHttpTarget {
                url: current.http.url,
                method: current.http.method,
                headers: current.http.headers,
                body: current.http.body,
                accepted_statuses: current.http.accepted_statuses,
                follow_redirects: current.http.follow_redirects,
                max_redirects: current.http.max_redirects,
                body_contains,
                skip_tls_verification: current.http.skip_tls_verification,
            },
            policy: current.policy,
            notification_channels: current.notification_channels,
        }
    }
}
