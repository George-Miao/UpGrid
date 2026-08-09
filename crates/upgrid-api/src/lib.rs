//! Authenticated Cluster HTTP interface and embedded WebUI.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use upgrid_config::{AppResult, Cipher, Config, JoinLink, generate_join_token, now_ms};
use upgrid_raft::domain::{
    AlertDelivery, AlertKind, AvailabilityState, Command, ConfigValue, DomainError,
    EvaluationPolicy, HttpTarget, NotificationChannel, NotificationChannelId,
    NotificationChannelKind, Secret, SecretId, StatusRange, Target, TargetId, TargetState,
};
use upgrid_raft::{ClusterError, Handle, hash_join_token};
use url::Url;
use utoipa::ToSchema;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityRequirement, SecurityScheme};
use utoipa::openapi::{Components, Info};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use uuid::Uuid;

mod assets;
mod resources;
mod server;
mod setup;
mod targets;

#[cfg(test)]
#[path = "tests.rs"]
mod api_tests;

pub use server::{openapi_json, start};
pub use setup::wait_for_join;

#[derive(Clone)]
struct WebState {
    cluster: Handle,
    cipher: Cipher,
    raft_url: String,
    username: String,
    password: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorBody {
    error: String,
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        }
    }

    fn unavailable(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: error.to_string(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl From<ClusterError> for ApiError {
    fn from(error: ClusterError) -> Self {
        let status = match &error {
            ClusterError::Domain(DomainError::TargetNotFound(_)) => StatusCode::NOT_FOUND,
            ClusterError::Domain(DomainError::TargetAlreadyExists(_)) => StatusCode::CONFLICT,
            ClusterError::Domain(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ClusterError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
struct StatusRangeInput {
    start: u16,
    end: u16,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(untagged)]
enum ConfigValueInput {
    Literal(String),
    Secret { secret_id: Uuid },
}

impl From<ConfigValueInput> for ConfigValue {
    fn from(value: ConfigValueInput) -> Self {
        match value {
            ConfigValueInput::Literal(value) => Self::Literal(value),
            ConfigValueInput::Secret { secret_id } => Self::Secret(SecretId(secret_id)),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ConfigValueView {
    Literal { value: String },
    Secret { secret_id: Uuid },
}

impl From<&ConfigValue> for ConfigValueView {
    fn from(value: &ConfigValue) -> Self {
        match value {
            ConfigValue::Literal(value) => Self::Literal {
                value: value.clone(),
            },
            ConfigValue::Secret(secret_id) => Self::Secret {
                secret_id: secret_id.0,
            },
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct PutTargetRequest {
    name: String,
    url: String,
    #[serde(default = "default_method")]
    method: String,
    #[serde(default)]
    headers: BTreeMap<String, ConfigValueInput>,
    #[serde(default)]
    body: Option<ConfigValueInput>,
    #[serde(default = "default_statuses")]
    accepted_statuses: Vec<StatusRangeInput>,
    #[serde(default = "default_true")]
    follow_redirects: bool,
    #[serde(default = "default_redirects")]
    max_redirects: u8,
    #[serde(default)]
    body_contains: Option<String>,
    #[serde(default)]
    skip_tls_verification: bool,
    #[serde(default = "default_interval_seconds")]
    interval_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    timeout_seconds: u64,
    #[serde(default = "default_failure_threshold")]
    failure_threshold: u32,
    #[serde(default)]
    notification_channel_ids: BTreeSet<Uuid>,
}

fn default_method() -> String {
    "GET".to_owned()
}
fn default_statuses() -> Vec<StatusRangeInput> {
    vec![StatusRangeInput {
        start: 200,
        end: 299,
    }]
}
fn default_true() -> bool {
    true
}
fn default_redirects() -> u8 {
    5
}
fn default_interval_seconds() -> u64 {
    60
}
fn default_timeout_seconds() -> u64 {
    10
}
fn default_failure_threshold() -> u32 {
    3
}

#[derive(Debug, Serialize, ToSchema)]
struct EvaluationView {
    scheduled_at_ms: u64,
    recorded_at_ms: u64,
    executor_node_id: Uuid,
    succeeded: bool,
    status_code: Option<u16>,
    latency_ms: u64,
    diagnostic: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct TargetView {
    id: Uuid,
    name: String,
    url: String,
    method: String,
    headers: BTreeMap<String, ConfigValueView>,
    body: Option<ConfigValueView>,
    accepted_statuses: Vec<StatusRangeInput>,
    follow_redirects: bool,
    max_redirects: u8,
    body_contains: Option<String>,
    skip_tls_verification: bool,
    interval_seconds: u64,
    timeout_seconds: u64,
    failure_threshold: u32,
    notification_channel_ids: BTreeSet<Uuid>,
    availability: String,
    consecutive_failures: u32,
    latest_evaluation: Option<EvaluationView>,
    history: Vec<EvaluationView>,
    paused: bool,
}

impl From<&TargetState> for TargetView {
    fn from(state: &TargetState) -> Self {
        let target = &state.target;
        Self {
            id: target.id.0,
            name: target.name.clone(),
            url: target.http.url.to_string(),
            method: target.http.method.clone(),
            headers: target
                .http
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), ConfigValueView::from(value)))
                .collect(),
            body: target.http.body.as_ref().map(ConfigValueView::from),
            accepted_statuses: target
                .http
                .accepted_statuses
                .iter()
                .map(|range| StatusRangeInput {
                    start: range.start,
                    end: range.end,
                })
                .collect(),
            follow_redirects: target.http.follow_redirects,
            max_redirects: target.http.max_redirects,
            body_contains: target.http.body_contains.clone(),
            skip_tls_verification: target.http.skip_tls_verification,
            interval_seconds: target.policy.interval_ms / 1_000,
            timeout_seconds: target.policy.timeout_ms / 1_000,
            failure_threshold: target.policy.failure_threshold,
            notification_channel_ids: target.notification_channels.iter().map(|id| id.0).collect(),
            availability: availability_name(state.availability).to_owned(),
            consecutive_failures: state.consecutive_failures,
            latest_evaluation: state.latest_evaluation.as_ref().map(EvaluationView::from),
            history: state
                .history
                .values()
                .rev()
                .take(100)
                .map(EvaluationView::from)
                .collect(),
            paused: state.paused,
        }
    }
}

impl From<&upgrid_raft::domain::Evaluation> for EvaluationView {
    fn from(value: &upgrid_raft::domain::Evaluation) -> Self {
        Self {
            scheduled_at_ms: value.id.scheduled_at_ms,
            recorded_at_ms: value.recorded_at_ms,
            executor_node_id: value.executor_node_id,
            succeeded: value.succeeded,
            status_code: value.http.status_code,
            latency_ms: value.http.latency_ms,
            diagnostic: value.diagnostic.clone(),
        }
    }
}

fn availability_name(value: AvailabilityState) -> &'static str {
    match value {
        AvailabilityState::Unknown => "unknown",
        AvailabilityState::Up => "up",
        AvailabilityState::Down => "down",
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PutChannelRequest {
    Telegram {
        name: String,
        bot_token: String,
        chat_id: String,
    },
    Webhook {
        name: String,
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, ConfigValueInput>,
    },
}

#[derive(Debug, Serialize, ToSchema)]
struct ChannelView {
    id: Uuid,
    name: String,
    kind: String,
    destination: String,
}

impl From<&NotificationChannel> for ChannelView {
    fn from(channel: &NotificationChannel) -> Self {
        let (kind, destination) = match &channel.kind {
            NotificationChannelKind::Telegram { chat_id, .. } => ("telegram", chat_id.clone()),
            NotificationChannelKind::Webhook { url, .. } => ("webhook", url.to_string()),
        };
        Self {
            id: channel.id.0,
            name: channel.name.clone(),
            kind: kind.to_owned(),
            destination,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct PutSecretRequest {
    name: String,
    value: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct SecretView {
    id: Uuid,
    name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateJoinTokenRequest {
    #[serde(default = "default_join_link_lifetime")]
    expires_in_seconds: u64,
}

fn default_join_link_lifetime() -> u64 {
    600
}

#[derive(Debug, Serialize, ToSchema)]
struct CreatedJoinTokenView {
    id: String,
    url: String,
    expires_at_ms: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct JoinTokenView {
    id: String,
    expires_at_ms: u64,
}

impl From<&Secret> for SecretView {
    fn from(secret: &Secret) -> Self {
        Self {
            id: secret.id.0,
            name: secret.name.clone(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
struct AlertView {
    target_id: Uuid,
    channel_id: Uuid,
    kind: String,
    target_name: String,
    scheduled_at_ms: u64,
    delivery: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ClusterMemberView {
    id: Uuid,
    raft_url: String,
    leader: bool,
    local: bool,
}

#[derive(Debug, Serialize, ToSchema)]
struct ClusterView {
    leader_node_id: Option<Uuid>,
    local_node_id: Uuid,
    members: Vec<ClusterMemberView>,
}
