use axum::Json;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use upgrid_config::now_ms;
use upgrid_raft::domain::{
    ApiToken, ApiTokenId, Command, IdentityId, OperatorIdentity, PasswordVerifier,
    generate_api_token,
};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::auth::Principal;
use crate::{ApiError, ErrorBody, WebState};

const MAX_TOKEN_TTL_SECONDS: u64 = 365 * 24 * 60 * 60;

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct IdentityView {
    id: Uuid,
    username: String,
    created_at_ms: u64,
}

impl From<&OperatorIdentity> for IdentityView {
    fn from(identity: &OperatorIdentity) -> Self {
        Self {
            id: identity.id.0,
            username: identity.username.clone(),
            created_at_ms: identity.created_at_ms,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct CreateIdentityRequest {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct UpdateIdentityRequest {
    username: String,
    password: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ApiTokenView {
    id: Uuid,
    identity_id: Uuid,
    name: String,
    created_at_ms: u64,
    expires_at_ms: Option<u64>,
}

impl From<&ApiToken> for ApiTokenView {
    fn from(token: &ApiToken) -> Self {
        Self {
            id: token.id.0,
            identity_id: token.identity_id.0,
            name: token.name.clone(),
            created_at_ms: token.created_at_ms,
            expires_at_ms: token.expires_at_ms,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct CreateApiTokenRequest {
    name: String,
    expires_in_seconds: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct CreatedApiTokenView {
    #[serde(flatten)]
    token: ApiTokenView,
    value: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/identities",
    responses(
        (status = 200, body = Vec<IdentityView>),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody)
    )
)]
pub(super) async fn list_identities(
    State(state): State<WebState>,
) -> Result<Json<Vec<IdentityView>>, ApiError> {
    let application = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        application
            .identities
            .values()
            .map(IdentityView::from)
            .collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/identities",
    request_body = CreateIdentityRequest,
    responses(
        (status = 201, body = IdentityView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 409, body = ErrorBody),
        (status = 503, body = ErrorBody)
    )
)]
pub(super) async fn create_identity(
    State(state): State<WebState>,
    Json(input): Json<CreateIdentityRequest>,
) -> Result<(StatusCode, Json<IdentityView>), ApiError> {
    let password = password_verifier(input.password).await?;
    let identity = OperatorIdentity {
        id: IdentityId(Uuid::now_v7()),
        username: input.username,
        password,
        auth_version: 1,
        created_at_ms: now_ms(),
    };
    state
        .cluster
        .apply(Command::CreateIdentity(identity.clone()))
        .await?;
    Ok((StatusCode::CREATED, Json(IdentityView::from(&identity))))
}

#[utoipa::path(
    put,
    path = "/api/v1/identities/{id}",
    params(("id" = Uuid, Path, description = "Operator identity ID")),
    request_body = UpdateIdentityRequest,
    responses(
        (status = 200, body = IdentityView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 409, body = ErrorBody),
        (status = 503, body = ErrorBody)
    )
)]
pub(super) async fn update_identity(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Json(input): Json<UpdateIdentityRequest>,
) -> Result<Json<IdentityView>, ApiError> {
    let application = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let mut identity = application
        .identities
        .get(&IdentityId(id))
        .cloned()
        .ok_or_else(|| ApiError::not_found("identity not found"))?;
    identity.username = input.username;
    if let Some(password) = input.password {
        identity.password = password_verifier(password).await?;
        identity.auth_version = identity.auth_version.saturating_add(1);
    }
    state
        .cluster
        .apply(Command::UpdateIdentity(identity.clone()))
        .await?;
    Ok(Json(IdentityView::from(&identity)))
}

#[utoipa::path(
    delete,
    path = "/api/v1/identities/{id}",
    params(("id" = Uuid, Path, description = "Operator identity ID")),
    responses(
        (status = 204),
        (status = 401, body = ErrorBody),
        (status = 403, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 409, body = ErrorBody),
        (status = 503, body = ErrorBody)
    )
)]
pub(super) async fn delete_identity(
    State(state): State<WebState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let id = IdentityId(id);
    if id == principal.identity_id {
        return Err(ApiError::forbidden(
            "the current identity cannot delete itself",
        ));
    }
    state.cluster.apply(Command::DeleteIdentity(id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/v1/api-tokens",
    responses(
        (status = 200, body = Vec<ApiTokenView>),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody)
    )
)]
pub(super) async fn list_api_tokens(
    State(state): State<WebState>,
) -> Result<Json<Vec<ApiTokenView>>, ApiError> {
    let application = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        application
            .api_tokens
            .values()
            .map(ApiTokenView::from)
            .collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/api-tokens",
    request_body = CreateApiTokenRequest,
    responses(
        (status = 201, body = CreatedApiTokenView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody)
    )
)]
pub(super) async fn create_api_token(
    State(state): State<WebState>,
    Extension(principal): Extension<Principal>,
    Json(input): Json<CreateApiTokenRequest>,
) -> Result<(StatusCode, Json<CreatedApiTokenView>), ApiError> {
    if input
        .expires_in_seconds
        .is_some_and(|seconds| seconds == 0 || seconds > MAX_TOKEN_TTL_SECONDS)
    {
        return Err(ApiError::bad_request(
            "API token expiry must be between one second and one year",
        ));
    }
    let created_at_ms = now_ms();
    let expires_at_ms = input
        .expires_in_seconds
        .and_then(|seconds| seconds.checked_mul(1_000))
        .and_then(|duration| created_at_ms.checked_add(duration));
    let (value, hash) = generate_api_token().map_err(ApiError::unavailable)?;
    let token = ApiToken {
        id: ApiTokenId(Uuid::now_v7()),
        identity_id: principal.identity_id,
        name: input.name,
        hash,
        created_at_ms,
        expires_at_ms,
    };
    state
        .cluster
        .apply(Command::CreateApiToken(token.clone()))
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedApiTokenView {
            token: ApiTokenView::from(&token),
            value,
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/api-tokens/{id}",
    params(("id" = Uuid, Path, description = "API token ID")),
    responses(
        (status = 204),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody)
    )
)]
pub(super) async fn revoke_api_token(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state
        .cluster
        .apply(Command::RevokeApiToken(ApiTokenId(id)))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn password_verifier(password: String) -> Result<PasswordVerifier, ApiError> {
    tokio::task::spawn_blocking(move || PasswordVerifier::create(&password))
        .await
        .map_err(|_| ApiError::unavailable("password hashing task stopped"))?
        .map_err(ApiError::bad_request)
}
