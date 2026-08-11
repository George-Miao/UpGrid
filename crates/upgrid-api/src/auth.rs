use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::hmac;
use serde::{Deserialize, Serialize};
use upgrid_config::now_ms;
use upgrid_raft::domain::{IdentityId, OperatorIdentity, hash_api_token};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{ApiError, ErrorBody, WebState};

const SESSION_COOKIE: &str = "upgrid_session";
const SESSION_TTL_MS: u64 = 15 * 60 * 1_000;
const JWT_PURPOSE: &[u8] = b"upgrid-api-session-v1";

#[derive(Debug, Clone)]
pub(super) struct Principal {
    pub(super) identity_id: IdentityId,
    pub(super) username: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct SessionView {
    identity_id: Uuid,
    username: String,
    expires_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: Uuid,
    ver: u64,
    iat: u64,
    exp: u64,
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, body = SessionView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody)
    ),
    security()
)]
pub(super) async fn login(
    State(state): State<WebState>,
    Json(input): Json<LoginRequest>,
) -> Result<Response, ApiError> {
    let application = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let Some(identity) = application
        .identities
        .values()
        .find(|identity| {
            identity
                .username
                .eq_ignore_ascii_case(input.username.trim())
        })
        .cloned()
    else {
        return Err(ApiError::unauthorized());
    };
    let password = input.password;
    let verifier = identity.password.clone();
    let valid = tokio::task::spawn_blocking(move || verifier.verify(&password))
        .await
        .map_err(|_| ApiError::unavailable("password verification task stopped"))?;
    if !valid {
        return Err(ApiError::unauthorized());
    }
    session_response(&state, &identity)
}

#[utoipa::path(
    get,
    path = "/api/v1/auth/session",
    responses(
        (status = 200, body = SessionView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody)
    ),
    security()
)]
pub(super) async fn session(
    State(state): State<WebState>,
    headers: HeaderMap,
) -> Result<Json<SessionView>, ApiError> {
    let (principal, expires_at_ms) = authenticate(&state, &headers)
        .await
        .ok_or_else(ApiError::unauthorized)?;
    Ok(Json(SessionView {
        identity_id: principal.identity_id.0,
        username: principal.username,
        expires_at_ms,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/logout",
    responses((status = 204)),
    security()
)]
pub(super) async fn logout() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static("upgrid_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"),
    );
    response
}

pub(super) async fn require_auth(
    State(state): State<WebState>,
    mut request: Request,
    next: Next,
) -> Response {
    if matches!(
        request.uri().path(),
        "/api/v1/auth/login" | "/api/v1/auth/session" | "/api/v1/auth/logout"
    ) {
        return next.run(request).await;
    }
    let Some((principal, _)) = authenticate(&state, request.headers()).await else {
        return unauthorized();
    };
    request.extensions_mut().insert(principal);
    next.run(request).await
}

async fn authenticate(state: &WebState, headers: &HeaderMap) -> Option<(Principal, u64)> {
    let token = bearer(headers).or_else(|| cookie(headers))?;
    let application = state.cluster.read().await.ok()?;
    let now = now_ms();
    if token.starts_with("upgrid_") {
        let hash = hash_api_token(token);
        let api_token = application.api_tokens.values().find(|candidate| {
            candidate.hash == hash
                && candidate
                    .expires_at_ms
                    .is_none_or(|expires_at_ms| expires_at_ms >= now)
        })?;
        let identity = application.identities.get(&api_token.identity_id)?;
        return Some((
            Principal::from(identity),
            api_token.expires_at_ms.unwrap_or(u64::MAX),
        ));
    }
    let claims = decode_jwt(state, token)?;
    if claims.exp < now {
        return None;
    }
    let identity = application.identities.get(&IdentityId(claims.sub))?;
    (identity.auth_version == claims.ver).then(|| (Principal::from(identity), claims.exp))
}

fn session_response(state: &WebState, identity: &OperatorIdentity) -> Result<Response, ApiError> {
    let now = now_ms();
    let expires_at_ms = now.saturating_add(SESSION_TTL_MS);
    let token = encode_jwt(
        state,
        &Claims {
            sub: identity.id.0,
            ver: identity.auth_version,
            iat: now,
            exp: expires_at_ms,
        },
    )?;
    let cookie = HeaderValue::try_from(format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
        SESSION_TTL_MS / 1_000
    ))
    .map_err(ApiError::unavailable)?;
    let mut response = Json(SessionView {
        identity_id: identity.id.0,
        username: identity.username.clone(),
        expires_at_ms,
    })
    .into_response();
    response.headers_mut().insert(header::SET_COOKIE, cookie);
    Ok(response)
}

fn encode_jwt(state: &WebState, claims: &Claims) -> Result<String, ApiError> {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).map_err(ApiError::unavailable)?);
    let payload = format!("{header}.{claims}");
    let key = hmac::Key::new(hmac::HMAC_SHA256, &state.cipher.derive(JWT_PURPOSE));
    let signature = URL_SAFE_NO_PAD.encode(hmac::sign(&key, payload.as_bytes()).as_ref());
    Ok(format!("{payload}.{signature}"))
}

fn decode_jwt(state: &WebState, token: &str) -> Option<Claims> {
    let mut parts = token.split('.');
    let header = parts.next()?;
    let claims = parts.next()?;
    let signature = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let payload = format!("{header}.{claims}");
    let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, &state.cipher.derive(JWT_PURPOSE));
    hmac::verify(&key, payload.as_bytes(), &signature).ok()?;
    let claims = URL_SAFE_NO_PAD.decode(claims).ok()?;
    serde_json::from_slice(&claims).ok()
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn cookie(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|value| value.strip_prefix("upgrid_session="))
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorBody {
            error: "authentication required".to_owned(),
        }),
    )
        .into_response()
}

impl From<&OperatorIdentity> for Principal {
    fn from(identity: &OperatorIdentity) -> Self {
        Self {
            identity_id: identity.id,
            username: identity.username.clone(),
        }
    }
}
