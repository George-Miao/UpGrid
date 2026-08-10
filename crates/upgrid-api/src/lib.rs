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
use upgrid_config::{Cipher, Config, JoinLink, Oobe, OobePhase, generate_join_token, now_ms};
use upgrid_raft::domain::{
    AlertDelivery, AlertKind, ApplicationState, AvailabilityState, Command, ConfigValue,
    DomainError, EvaluationPolicy, HttpTarget, NodeTargetState, NotificationChannel,
    NotificationChannelId, NotificationChannelKind, Secret, SecretId, StatusRange, Target,
    TargetId, TargetState,
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
mod error;
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
    username: String,
    password: String,
    node_name: String,
    oobe: Oobe,
    startup_warning: Option<String>,
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
}

impl From<ClusterError> for ApiError {
    fn from(error: ClusterError) -> Self {
        let status = match &error {
            ClusterError::Domain(DomainError::TargetNotFound(_)) => StatusCode::NOT_FOUND,
            ClusterError::Domain(DomainError::NotificationChannelNotFound(_)) => {
                StatusCode::NOT_FOUND
            }
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
    name: String,
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
