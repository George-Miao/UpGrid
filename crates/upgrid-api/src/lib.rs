//! Authenticated Cluster HTTP interface and embedded WebUI.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use upgrid_config::{Cipher, Config, JoinLink, Oobe, OobePhase, generate_join_token, now_ms};
use upgrid_raft::domain::{
    AlertDelivery, AlertKind, ApplicationState, AvailabilityState, Command, ConfigValue,
    DomainError, EvaluationPolicy, HttpAssertion, HttpTarget, NodeTargetState, NotificationChannel,
    NotificationChannelId, NotificationChannelKind, Secret, SecretId, SmtpSecurity, StatusRange,
    Target, TargetId, TargetKind, TargetState, TrashedTarget,
};
use upgrid_raft::{ClusterError, Handle, MembershipError, hash_join_token};
use url::Url;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityRequirement, SecurityScheme};
use utoipa::openapi::{Components, Info};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use uuid::Uuid;

mod assets;
mod auth;
mod channels;
mod error;
mod history;
mod identities;
mod join;
mod model;
mod nodes;
mod resources;
mod server;
mod setup;
mod targets;

#[cfg(test)]
#[path = "tests.rs"]
mod api_tests;

pub use error::{Error, Result};
use model::*;
pub use server::{openapi_json, start};
pub use setup::{OobeChoice, wait_for_oobe};

#[derive(Clone)]
struct WebState {
    cluster: Handle,
    cipher: Cipher,
    notifications: upgrid_notification::Tester,
    raft_url: String,
    node_name: String,
    oobe: Oobe,
    startup_warning: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorBody {
    error: String,
}

#[derive(Debug)]
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

    fn unprocessable(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: error.to_string(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "authentication required".to_owned(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }
}

impl From<ClusterError> for ApiError {
    fn from(error: ClusterError) -> Self {
        let status = match &error {
            ClusterError::Domain {
                source:
                    DomainError::TargetNotFound(_)
                    | DomainError::TrashedTargetNotFound(_)
                    | DomainError::NotificationChannelNotFound(_)
                    | DomainError::AlertNotFound(_)
                    | DomainError::IdentityNotFound(_)
                    | DomainError::ApiTokenNotFound(_),
            } => StatusCode::NOT_FOUND,
            ClusterError::Domain {
                source:
                    DomainError::TargetAlreadyExists(_)
                    | DomainError::IdentityAlreadyExists(_)
                    | DomainError::ApiTokenAlreadyExists(_),
            } => StatusCode::CONFLICT,
            ClusterError::Domain { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            ClusterError::RuntimeStopped { .. }
            | ClusterError::RuntimeResponse { .. }
            | ClusterError::Operation { .. } => StatusCode::SERVICE_UNAVAILABLE,
            ClusterError::Membership {
                source: MembershipError::NodeNotFound(_),
            } => StatusCode::NOT_FOUND,
            ClusterError::Membership {
                source: MembershipError::LastVoter,
            } => StatusCode::CONFLICT,
            ClusterError::Membership { .. } => StatusCode::SERVICE_UNAVAILABLE,
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
    #[serde(default)]
    max_uses: Option<u64>,
}

fn default_join_link_lifetime() -> u64 {
    24 * 60 * 60
}

#[derive(Debug, Serialize, ToSchema)]
struct CreatedJoinTokenView {
    id: String,
    url: String,
    expires_at_ms: u64,
    remaining_uses: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct JoinTokenView {
    id: String,
    expires_at_ms: u64,
    remaining_uses: Option<u64>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct JoinClusterRequest {
    join_link: String,
    node_name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateClusterRequest {
    node_name: String,
    admin_username: String,
    admin_password: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum SetupPhase {
    Cluster,
    Channel,
    Target,
    Complete,
}

impl From<OobePhase> for SetupPhase {
    fn from(phase: OobePhase) -> Self {
        match phase {
            OobePhase::Cluster => Self::Cluster,
            OobePhase::Channel => Self::Channel,
            OobePhase::Target => Self::Target,
            OobePhase::Complete => Self::Complete,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
struct SetupView {
    setup: bool,
    phase: SetupPhase,
    path: String,
    cluster_ready: bool,
    node_name: String,
    warning: Option<String>,
    channel_count: usize,
    target_count: usize,
}

#[derive(Debug, Serialize, ToSchema)]
struct JoinClusterView {
    status: &'static str,
}

impl From<&Secret> for SecretView {
    fn from(secret: &Secret) -> Self {
        Self {
            id: secret.id.0,
            name: secret.name.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum AlertKindParam {
    Down,
    Recovered,
}

impl From<AlertKindParam> for AlertKind {
    fn from(kind: AlertKindParam) -> Self {
        match kind {
            AlertKindParam::Down => Self::Down,
            AlertKindParam::Recovered => Self::Recovered,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum AlertDeliveryParam {
    Pending,
    Delivered,
    Failed,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct AlertFilters {
    target_id: Option<Uuid>,
    channel_id: Option<Uuid>,
    kind: Option<AlertKindParam>,
    delivery: Option<AlertDeliveryParam>,
    acknowledged: Option<bool>,
    from_ms: Option<u64>,
    to_ms: Option<u64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct AlertActionRequest {
    target_id: Uuid,
    channel_id: Uuid,
    scheduled_at_ms: u64,
    kind: AlertKindParam,
}

impl AlertActionRequest {
    fn alert_id(&self) -> upgrid_raft::domain::AlertId {
        upgrid_raft::domain::AlertId {
            target_id: TargetId(self.target_id),
            channel_id: NotificationChannelId(self.channel_id),
            evaluation_scheduled_at_ms: self.scheduled_at_ms,
            kind: self.kind.into(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
struct AlertView {
    target_id: Uuid,
    channel_id: Uuid,
    kind: String,
    target_name: String,
    channel_name: String,
    scheduled_at_ms: u64,
    delivery: String,
    attempts: u32,
    next_attempt_at_ms: Option<u64>,
    completed_at_ms: Option<u64>,
    diagnostic: Option<String>,
    acknowledged_at_ms: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ClusterMemberView {
    id: Uuid,
    name: String,
    raft_url: String,
    leader: bool,
    local: bool,
    draining: bool,
    active_assignments: usize,
}

#[derive(Debug, Serialize, ToSchema)]
struct ClusterView {
    leader_node_id: Option<Uuid>,
    local_node_id: Uuid,
    members: Vec<ClusterMemberView>,
}
