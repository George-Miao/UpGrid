use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::FutureExt;
use snafu::ResultExt;
use tokio::sync::Notify;
use upgrid_config::{Config, JoinLink, store_node_name};

use crate::assets::{favicon, index, webui_script};
use crate::error::{BindSnafu, ListenerSnafu, RuntimeSnafu, ServeSnafu, TlsSnafu};
use crate::listener::{TlsListener, tls_acceptor};
use crate::{
    ApiError, CreateClusterRequest, Error, JoinClusterRequest, JoinClusterView, Result, SetupView,
};

#[derive(Clone)]
struct SetupState {
    data_dir: std::path::PathBuf,
    node_name: Arc<Mutex<String>>,
    result: Arc<Mutex<Option<OobeChoice>>>,
    accepted: Arc<Notify>,
    deadline: Arc<Notify>,
}

pub enum OobeChoice {
    NewCluster {
        node_name: String,
        admin_username: String,
        admin_password: String,
    },
    Join {
        node_name: String,
        link: Box<JoinLink>,
    },
}

pub fn wait_for_oobe(config: &Config, node_name: &str) -> Result<OobeChoice> {
    let listener = std::net::TcpListener::bind(&config.bind).context(BindSnafu {
        address: config.bind.clone(),
    })?;
    listener.set_nonblocking(true).context(ListenerSnafu)?;
    let result = Arc::new(Mutex::new(None));
    let accepted = Arc::new(Notify::new());
    let deadline = Arc::new(Notify::new());
    let state = SetupState {
        data_dir: config.data_dir.clone(),
        node_name: Arc::new(Mutex::new(node_name.to_owned())),
        result: result.clone(),
        accepted: accepted.clone(),
        deadline: deadline.clone(),
    };
    let routes = Router::new()
        .route("/", get(index))
        .route("/setup", get(index))
        .route("/setup/channel", get(index))
        .route("/setup/target", get(index))
        .route("/alerts", get(index))
        .route("/cluster", get(index))
        .route("/api/v1/setup", get(setup_status))
        .route("/api/v1/cluster/join", post(join))
        .route("/api/v1/setup/new-cluster", post(new_cluster));
    let app = Router::new()
        .route("/assets/upgrid.js", get(webui_script))
        .route("/favicon.svg", get(favicon))
        .merge(routes)
        .with_state(state);
    let runtime = compio::runtime::Runtime::new().context(RuntimeSnafu)?;
    let tls_cert = config.tls_cert.clone();
    let tls_key = config.tls_key.clone();
    tracing::info!(bind = %config.bind, "WebUI ready for cluster setup");
    runtime.block_on(async move {
        let listener = compio::net::TcpListener::from_std(listener).context(ListenerSnafu)?;
        let shutdown = async move { accepted.notified().await };
        let server = if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
            let acceptor = tls_acceptor(&cert, &key).context(TlsSnafu)?;
            cyper_axum::serve(TlsListener::new(listener, acceptor), app)
                .with_graceful_shutdown(shutdown)
                .into_future()
                .boxed_local()
        } else {
            cyper_axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .into_future()
                .boxed_local()
        };
        let deadline = async move {
            deadline.notified().await;
            compio::time::sleep(Duration::from_secs(5)).await;
        }
        .boxed_local();
        match futures_util::future::select(server, deadline).await {
            futures_util::future::Either::Left((result, _)) => result.context(ServeSnafu)?,
            futures_util::future::Either::Right(_) => {
                tracing::warn!("OOBE API forced remaining connections closed");
            }
        }
        let choice = result
            .lock()
            .map_err(|_| Error::SetupStatePoisoned)?
            .take()
            .ok_or(Error::SetupStopped)?;
        Ok(choice)
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
    upgrid_raft::domain::validate_username(&input.admin_username).map_err(ApiError::bad_request)?;
    upgrid_raft::domain::validate_password(&input.admin_password).map_err(ApiError::bad_request)?;
    let node_name = persist_name(&state, &input.node_name)?;
    accept(
        &state,
        OobeChoice::NewCluster {
            node_name,
            admin_username: input.admin_username,
            admin_password: input.admin_password,
        },
    )?;
    Ok((
        StatusCode::ACCEPTED,
        Json(JoinClusterView { status: "creating" }),
    ))
}

async fn setup_status(State(state): State<SetupState>) -> Result<Json<SetupView>, ApiError> {
    let node_name = state
        .node_name
        .lock()
        .map_err(|_| ApiError::unavailable("OOBE node name was poisoned"))?
        .clone();
    Ok(Json(SetupView {
        setup: true,
        phase: crate::SetupPhase::Cluster,
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
        .map_err(|_| ApiError::unavailable("OOBE node name was poisoned"))? = name.clone();
    Ok(name)
}

fn accept(state: &SetupState, choice: OobeChoice) -> Result<(), ApiError> {
    let mut result = state
        .result
        .lock()
        .map_err(|_| ApiError::unavailable("OOBE state was poisoned"))?;
    if result.is_some() {
        return Err(ApiError::bad_request(
            "A cluster choice was already accepted",
        ));
    }
    *result = Some(choice);
    drop(result);
    state.accepted.notify_one();
    state.deadline.notify_one();
    Ok(())
}
