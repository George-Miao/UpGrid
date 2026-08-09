use std::sync::{Arc, Mutex};

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use tokio::sync::Notify;
use upgrid_config::{AppResult, Config, JoinLink, store_node_name};

use crate::assets::{favicon, index, webui_script};
use crate::{
    ApiError, CreateClusterRequest, ErrorBody, JoinClusterRequest, JoinClusterView, SetupView,
};

#[derive(Clone)]
struct SetupState {
    username: String,
    password: String,
    data_dir: std::path::PathBuf,
    node_name: Arc<Mutex<String>>,
    result: Arc<Mutex<Option<OobeChoice>>>,
    accepted: Arc<Notify>,
}

pub enum OobeChoice {
    NewCluster {
        node_name: String,
    },
    Join {
        node_name: String,
        link: Box<JoinLink>,
    },
}

pub fn wait_for_oobe(config: &Config, node_name: &str) -> AppResult<OobeChoice> {
    let listener = std::net::TcpListener::bind(&config.bind)?;
    listener.set_nonblocking(true)?;
    let result = Arc::new(Mutex::new(None));
    let accepted = Arc::new(Notify::new());
    let state = SetupState {
        username: config.username.clone(),
        password: config.password.clone(),
        data_dir: config.data_dir.clone(),
        node_name: Arc::new(Mutex::new(node_name.to_owned())),
        result: result.clone(),
        accepted: accepted.clone(),
    };
    let protected = Router::new()
        .route("/", get(index))
        .route("/setup", get(index))
        .route("/api/v1/setup", get(setup_status))
        .route("/api/v1/cluster/join", post(join))
        .route("/api/v1/setup/new-cluster", post(new_cluster))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));
    let app = Router::new()
        .route("/assets/app.js", get(webui_script))
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
            .ok_or_else(|| std::io::Error::other("OOBE stopped without a Cluster choice").into())
    })
}

async fn join(
    State(state): State<SetupState>,
    Json(input): Json<JoinClusterRequest>,
) -> Result<(StatusCode, Json<JoinClusterView>), ApiError> {
    let node_name = persist_name(&state, &input.node_name)?;
    let link = JoinLink::parse(input.join_link.trim()).map_err(ApiError::bad_request)?;
    accept(
        &state,
        OobeChoice::Join {
            node_name,
            link: Box::new(link),
        },
    )?;
    Ok((
        StatusCode::ACCEPTED,
        Json(JoinClusterView { status: "joining" }),
    ))
}

async fn new_cluster(
    State(state): State<SetupState>,
    Json(input): Json<CreateClusterRequest>,
) -> Result<(StatusCode, Json<JoinClusterView>), ApiError> {
    let node_name = persist_name(&state, &input.node_name)?;
    accept(&state, OobeChoice::NewCluster { node_name })?;
    Ok((
        StatusCode::ACCEPTED,
        Json(JoinClusterView { status: "creating" }),
    ))
}

async fn setup_status(State(state): State<SetupState>) -> Result<Json<SetupView>, ApiError> {
    let node_name = state
        .node_name
        .lock()
        .map_err(|_| ApiError::unavailable("OOBE Node name was poisoned"))?
        .clone();
    Ok(Json(SetupView {
        setup: true,
        phase: "cluster".to_owned(),
        path: "/setup".to_owned(),
        cluster_ready: false,
        node_name,
        warning: None,
        channel_count: 0,
        target_count: 0,
    }))
}

fn persist_name(state: &SetupState, name: &str) -> Result<String, ApiError> {
    let name = store_node_name(&state.data_dir, name).map_err(ApiError::bad_request)?;
    *state
        .node_name
        .lock()
        .map_err(|_| ApiError::unavailable("OOBE Node name was poisoned"))? = name.clone();
    Ok(name)
}

fn accept(state: &SetupState, choice: OobeChoice) -> Result<(), ApiError> {
    let mut result = state
        .result
        .lock()
        .map_err(|_| ApiError::unavailable("OOBE state was poisoned"))?;
    if result.is_some() {
        return Err(ApiError::bad_request(
            "a Cluster choice was already accepted",
        ));
    }
    *result = Some(choice);
    drop(result);
    state.accepted.notify_one();
    Ok(())
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
