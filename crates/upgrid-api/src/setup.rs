use std::sync::{Arc, Mutex};

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use upgrid_config::{AppResult, Config, JoinLink};

use crate::{ApiError, ErrorBody};

#[derive(Clone)]
struct SetupState {
    username: String,
    password: String,
    result: Arc<Mutex<Option<JoinLink>>>,
    accepted: Arc<Notify>,
}

#[derive(Deserialize)]
struct JoinRequest {
    join_link: String,
}

#[derive(Serialize)]
struct JoinResponse {
    status: &'static str,
}

pub fn wait_for_join(config: &Config) -> AppResult<JoinLink> {
    let listener = std::net::TcpListener::bind(&config.bind)?;
    listener.set_nonblocking(true)?;
    let result = Arc::new(Mutex::new(None));
    let accepted = Arc::new(Notify::new());
    let state = SetupState {
        username: config.username.clone(),
        password: config.password.clone(),
        result: result.clone(),
        accepted: accepted.clone(),
    };
    let protected = Router::new()
        .route("/", get(index))
        .route("/setup/join", post(join))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));
    let app = Router::new()
        .route("/assets/setup.js", get(script))
        .route("/assets/state.js", get(shared_script))
        .route("/favicon.svg", get(favicon))
        .merge(protected)
        .with_state(state);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let tls_cert = config.tls_cert.clone();
    let tls_key = config.tls_key.clone();
    tracing::info!(bind = %config.bind, "WebUI ready for Cluster setup");
    runtime.block_on(async move {
        if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
            let handle = axum_server::Handle::new();
            let shutdown = handle.clone();
            tokio::spawn(async move {
                accepted.notified().await;
                shutdown.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
            });
            axum_server::from_tcp_rustls(listener, tls)?
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        } else {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { accepted.notified().await })
                .await?;
        }
        result
            .lock()
            .map_err(|_| std::io::Error::other("Cluster setup state was poisoned"))?
            .take()
            .ok_or_else(|| {
                std::io::Error::other("Cluster setup stopped without a Join Link").into()
            })
    })
}

async fn join(
    State(state): State<SetupState>,
    Json(input): Json<JoinRequest>,
) -> Result<(StatusCode, Json<JoinResponse>), ApiError> {
    let link = JoinLink::parse(input.join_link.trim()).map_err(ApiError::bad_request)?;
    let mut result = state
        .result
        .lock()
        .map_err(|_| ApiError::unavailable("Cluster setup state was poisoned"))?;
    if result.is_some() {
        return Err(ApiError::bad_request("a Join Link was already accepted"));
    }
    *result = Some(link);
    drop(result);
    state.accepted.notify_one();
    Ok((
        StatusCode::ACCEPTED,
        Json(JoinResponse { status: "joining" }),
    ))
}

async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/setup.html"
        )),
    )
}

async fn script() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/assets/setup.js"
        ))
        .as_slice(),
    )
}

async fn shared_script() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/assets/state.js"
        ))
        .as_slice(),
    )
}

async fn favicon() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/favicon.svg"
        )),
    )
}

async fn require_auth(State(state): State<SetupState>, request: Request, next: Next) -> Response {
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
        HeaderValue::from_static("Basic realm=\"UpGrid setup\""),
    );
    response
}
