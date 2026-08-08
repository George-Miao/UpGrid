use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{
        Html, IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use url::Url;
use utoipa::ToSchema;
use utoipa::openapi::{
    Components, Info,
    security::{Http, HttpAuthScheme, SecurityRequirement, SecurityScheme},
};
use utoipa_axum::{router::OpenApiRouter, routes};
use uuid::Uuid;

use crate::{
    app::{AppResult, Config},
    cluster::{ClusterError, Handle},
    domain::{
        AlertDelivery, AlertKind, AvailabilityState, Command, ConfigValue, DomainError,
        EvaluationPolicy, HttpTarget, NotificationChannel, NotificationChannelId,
        NotificationChannelKind, Secret, SecretId, StatusRange, Target, TargetId, TargetState,
    },
    secret::Cipher,
};

#[derive(Clone)]
struct WebState {
    cluster: Handle,
    cipher: Cipher,
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
        }
    }
}

impl From<&crate::domain::Evaluation> for EvaluationView {
    fn from(value: &crate::domain::Evaluation) -> Self {
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
    #[serde(default = "default_join_token_lifetime")]
    expires_in_seconds: u64,
}

fn default_join_token_lifetime() -> u64 {
    600
}

#[derive(Debug, Serialize, ToSchema)]
struct JoinTokenView {
    token: String,
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

#[utoipa::path(get, path = "/api/v1/targets", responses((status = 200, body = [TargetView]), (status = 401, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn list_targets(State(state): State<WebState>) -> Result<Json<Vec<TargetView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot.targets.values().map(TargetView::from).collect(),
    ))
}

#[utoipa::path(get, path = "/api/v1/targets/{id}", params(("id" = Uuid, Path)), responses((status = 200, body = TargetView), (status = 401, body = ErrorBody), (status = 404, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn get_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<Json<TargetView>, ApiError> {
    let id = TargetId(id);
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    snapshot
        .targets
        .get(&id)
        .map(TargetView::from)
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("target not found: {}", id.0)))
}

#[utoipa::path(post, path = "/api/v1/targets", request_body = PutTargetRequest, responses((status = 201, body = TargetView), (status = 400, body = ErrorBody), (status = 401, body = ErrorBody), (status = 409, body = ErrorBody), (status = 422, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn create_target(
    State(state): State<WebState>,
    Json(input): Json<PutTargetRequest>,
) -> Result<(StatusCode, Json<TargetView>), ApiError> {
    let id = TargetId(Uuid::now_v7());
    let target = target_from_input(id, input)?;
    state.cluster.apply(Command::CreateTarget(target)).await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let view = snapshot
        .targets
        .get(&id)
        .map(TargetView::from)
        .expect("created target exists");
    Ok((StatusCode::CREATED, Json(view)))
}

#[utoipa::path(put, path = "/api/v1/targets/{id}", params(("id" = Uuid, Path)), request_body = PutTargetRequest, responses((status = 200, body = TargetView), (status = 400, body = ErrorBody), (status = 401, body = ErrorBody), (status = 404, body = ErrorBody), (status = 422, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn update_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Json(input): Json<PutTargetRequest>,
) -> Result<Json<TargetView>, ApiError> {
    let id = TargetId(id);
    let target = target_from_input(id, input)?;
    state.cluster.apply(Command::UpdateTarget(target)).await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot
            .targets
            .get(&id)
            .map(TargetView::from)
            .expect("updated target exists"),
    ))
}

#[utoipa::path(delete, path = "/api/v1/targets/{id}", params(("id" = Uuid, Path)), responses((status = 204), (status = 401, body = ErrorBody), (status = 404, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn delete_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state
        .cluster
        .apply(Command::DeleteTarget(TargetId(id)))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn target_from_input(id: TargetId, input: PutTargetRequest) -> Result<Target, ApiError> {
    let url = Url::parse(&input.url).map_err(ApiError::bad_request)?;
    Ok(Target {
        id,
        name: input.name,
        http: HttpTarget {
            url,
            method: input.method,
            headers: input
                .headers
                .into_iter()
                .map(|(key, value)| (key, ConfigValue::from(value)))
                .collect(),
            body: input.body.map(ConfigValue::from),
            accepted_statuses: input
                .accepted_statuses
                .into_iter()
                .map(|range| StatusRange::new(range.start, range.end))
                .collect(),
            follow_redirects: input.follow_redirects,
            max_redirects: input.max_redirects,
            body_contains: input.body_contains,
            skip_tls_verification: input.skip_tls_verification,
        },
        policy: EvaluationPolicy {
            interval_ms: input.interval_seconds.saturating_mul(1_000),
            timeout_ms: input.timeout_seconds.saturating_mul(1_000),
            failure_threshold: input.failure_threshold,
        },
        notification_channels: input
            .notification_channel_ids
            .into_iter()
            .map(NotificationChannelId)
            .collect(),
    })
}

#[utoipa::path(get, path = "/api/v1/channels", responses((status = 200, body = [ChannelView]), (status = 401, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn list_channels(State(state): State<WebState>) -> Result<Json<Vec<ChannelView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot
            .notification_channels
            .values()
            .map(ChannelView::from)
            .collect(),
    ))
}

#[utoipa::path(post, path = "/api/v1/channels", request_body = PutChannelRequest, responses((status = 201, body = ChannelView), (status = 400, body = ErrorBody), (status = 401, body = ErrorBody), (status = 422, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn create_channel(
    State(state): State<WebState>,
    Json(input): Json<PutChannelRequest>,
) -> Result<(StatusCode, Json<ChannelView>), ApiError> {
    let id = NotificationChannelId(Uuid::now_v7());
    let channel = match input {
        PutChannelRequest::Telegram {
            name,
            bot_token,
            chat_id,
        } => {
            let secret_id = SecretId(Uuid::now_v7());
            state
                .cluster
                .apply(Command::PutSecret(Secret {
                    id: secret_id,
                    name: format!("telegram-{}", id.0),
                    ciphertext: state
                        .cipher
                        .seal(bot_token.as_bytes())
                        .map_err(ApiError::bad_request)?,
                }))
                .await?;
            NotificationChannel {
                id,
                name,
                kind: NotificationChannelKind::Telegram {
                    bot_token: secret_id,
                    chat_id,
                },
            }
        }
        PutChannelRequest::Webhook { name, url, headers } => NotificationChannel {
            id,
            name,
            kind: NotificationChannelKind::Webhook {
                url: Url::parse(&url).map_err(ApiError::bad_request)?,
                headers: headers
                    .into_iter()
                    .map(|(key, value)| (key, ConfigValue::from(value)))
                    .collect(),
            },
        },
    };
    state
        .cluster
        .apply(Command::PutNotificationChannel(channel))
        .await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok((
        StatusCode::CREATED,
        Json(ChannelView::from(
            snapshot
                .notification_channels
                .get(&id)
                .expect("created channel exists"),
        )),
    ))
}

#[utoipa::path(get, path = "/api/v1/secrets", responses((status = 200, body = [SecretView]), (status = 401, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn list_secrets(State(state): State<WebState>) -> Result<Json<Vec<SecretView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot.secrets.values().map(SecretView::from).collect(),
    ))
}

#[utoipa::path(post, path = "/api/v1/secrets", request_body = PutSecretRequest, responses((status = 201, body = SecretView), (status = 400, body = ErrorBody), (status = 401, body = ErrorBody), (status = 422, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn create_secret(
    State(state): State<WebState>,
    Json(input): Json<PutSecretRequest>,
) -> Result<(StatusCode, Json<SecretView>), ApiError> {
    let id = SecretId(Uuid::now_v7());
    state
        .cluster
        .apply(Command::PutSecret(Secret {
            id,
            name: input.name,
            ciphertext: state
                .cipher
                .seal(input.value.as_bytes())
                .map_err(ApiError::bad_request)?,
        }))
        .await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok((
        StatusCode::CREATED,
        Json(SecretView::from(
            snapshot.secrets.get(&id).expect("created secret exists"),
        )),
    ))
}

#[utoipa::path(post, path = "/api/v1/join-tokens", request_body = CreateJoinTokenRequest, responses((status = 201, body = JoinTokenView), (status = 400, body = ErrorBody), (status = 401, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn create_join_token(
    State(state): State<WebState>,
    Json(input): Json<CreateJoinTokenRequest>,
) -> Result<(StatusCode, Json<JoinTokenView>), ApiError> {
    if input.expires_in_seconds == 0 || input.expires_in_seconds > 24 * 60 * 60 {
        return Err(ApiError::bad_request(
            "join token lifetime must be between 1 second and 24 hours",
        ));
    }
    let token = crate::secret::generate_join_token().map_err(ApiError::unavailable)?;
    let expires_at_ms =
        crate::app::now_ms().saturating_add(input.expires_in_seconds.saturating_mul(1_000));
    state
        .cluster
        .apply(Command::PutJoinToken {
            hash: crate::secret::hash_join_token(&token),
            expires_at_ms,
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(JoinTokenView {
            token,
            expires_at_ms,
        }),
    ))
}

#[utoipa::path(get, path = "/api/v1/alerts", responses((status = 200, body = [AlertView]), (status = 401, body = ErrorBody), (status = 503, body = ErrorBody)))]
async fn list_alerts(State(state): State<WebState>) -> Result<Json<Vec<AlertView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let alerts = snapshot
        .alerts
        .values()
        .rev()
        .map(|alert| AlertView {
            target_id: alert.id.target_id.0,
            channel_id: alert.id.channel_id.0,
            kind: match alert.id.kind {
                AlertKind::Down => "down",
                AlertKind::Recovered => "recovered",
            }
            .to_owned(),
            target_name: alert.target_name.clone(),
            scheduled_at_ms: alert.id.evaluation_scheduled_at_ms,
            delivery: match &alert.delivery {
                AlertDelivery::Pending { .. } => "pending",
                AlertDelivery::Delivered { .. } => "delivered",
                AlertDelivery::Failed { .. } => "failed",
            }
            .to_owned(),
        })
        .collect();
    Ok(Json(alerts))
}

#[utoipa::path(get, path = "/api/v1/events", responses((status = 200, description = "SSE stream of state versions", body = String, content_type = "text/event-stream"), (status = 401, body = ErrorBody)))]
async fn events(
    State(state): State<WebState>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let mut receiver = state.cluster.subscribe();
    let initial = state.cluster.version();
    let stream = async_stream::stream! {
        yield Ok(Event::default().event("state").data(initial.to_string()));
        while receiver.changed().await.is_ok() {
            let version = *receiver.borrow_and_update();
            yield Ok(Event::default().event("state").data(version.to_string()));
        }
    };
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn index() -> Html<&'static str> {
    Html(include_str!("webui.html"))
}

async fn require_auth(State(state): State<WebState>, request: Request, next: Next) -> Response {
    let authenticated = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .and_then(|value| STANDARD.decode(value).ok())
        .and_then(|value| String::from_utf8(value).ok())
        .is_some_and(|value| value == format!("{}:{}", state.username, state.password));
    if authenticated {
        return next.run(request).await;
    }
    let mut response = (
        StatusCode::UNAUTHORIZED,
        Json(ErrorBody {
            error: "authentication required".to_owned(),
        }),
    )
        .into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"UpGrid\""),
    );
    response
}

pub fn start(config: Config, cluster: Handle, cipher: Cipher) -> AppResult<()> {
    let listener = std::net::TcpListener::bind(&config.bind)?;
    listener.set_nonblocking(true)?;
    let bind = config.bind.clone();
    std::thread::Builder::new()
        .name("upgrid-web".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("upgrid-web-worker")
                .build()
                .expect("could not create web runtime");
            if let Err(error) = runtime.block_on(serve(listener, config, cluster, cipher)) {
                tracing::error!(%error, "web API stopped");
            }
        })?;
    tracing::debug!(%bind, "web API ready");
    Ok(())
}

async fn serve(
    listener: std::net::TcpListener,
    config: Config,
    cluster: Handle,
    cipher: Cipher,
) -> AppResult<()> {
    let state = WebState {
        cluster,
        cipher,
        username: config.username,
        password: config.password,
    };
    let (api, mut openapi) = api_routes().with_state(state.clone()).split_for_parts();
    configure_openapi(&mut openapi);
    let specification = std::sync::Arc::new(openapi);
    let protected = Router::new()
        .merge(api)
        .route("/", get(index))
        .route(
            "/openapi.json",
            get({
                let specification = specification.clone();
                move || {
                    let specification = specification.clone();
                    async move { Json(specification.as_ref().clone()) }
                }
            }),
        )
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));
    let app = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"status": "ok"})) }),
        )
        .merge(protected);
    let listener = tokio::net::TcpListener::from_std(listener)?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn api_routes() -> OpenApiRouter<WebState> {
    OpenApiRouter::new()
        .routes(routes!(list_targets, create_target))
        .routes(routes!(get_target, update_target, delete_target))
        .routes(routes!(list_channels, create_channel))
        .routes(routes!(list_secrets, create_secret))
        .routes(routes!(create_join_token))
        .routes(routes!(list_alerts))
        .routes(routes!(events))
}

pub fn openapi_json() -> serde_json::Result<String> {
    let (_, mut openapi) = api_routes().split_for_parts();
    configure_openapi(&mut openapi);
    serde_json::to_string_pretty(&openapi).map(|mut json| {
        json.push('\n');
        json
    })
}

fn configure_openapi(openapi: &mut utoipa::openapi::OpenApi) {
    openapi.info = Info::new("UpGrid Cluster API", env!("CARGO_PKG_VERSION"));
    openapi
        .components
        .get_or_insert_with(Components::new)
        .add_security_scheme(
            "basicAuth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Basic)),
        );
    openapi.security = Some(vec![SecurityRequirement::new(
        "basicAuth",
        std::iter::empty::<String>(),
    )]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_view_never_serializes_ciphertext() {
        let secret = Secret {
            id: SecretId(Uuid::from_u128(1)),
            name: "Telegram".to_owned(),
            ciphertext: b"must-not-leak".to_vec(),
        };

        let json = serde_json::to_string(&SecretView::from(&secret)).unwrap();

        assert!(json.contains("Telegram"));
        assert!(!json.contains("must-not-leak"));
        assert!(!json.contains("ciphertext"));
    }

    #[test]
    fn target_input_preserves_extension_method_case() {
        let input = PutTargetRequest {
            name: "Example".to_owned(),
            url: "https://example.com".to_owned(),
            method: "Example-Method".to_owned(),
            headers: BTreeMap::new(),
            body: None,
            accepted_statuses: vec![StatusRangeInput {
                start: 200,
                end: 299,
            }],
            follow_redirects: true,
            max_redirects: 5,
            body_contains: None,
            skip_tls_verification: false,
            interval_seconds: 60,
            timeout_seconds: 10,
            failure_threshold: 3,
            notification_channel_ids: BTreeSet::new(),
        };

        let Ok(target) = target_from_input(TargetId(Uuid::from_u128(1)), input) else {
            panic!("valid extension method should be accepted");
        };

        assert_eq!(target.http.method, "Example-Method");
    }

    #[test]
    fn published_openapi_matches_routes() {
        assert_eq!(
            openapi_json().unwrap(),
            include_str!("../docs/openapi.json")
        );
    }
}
