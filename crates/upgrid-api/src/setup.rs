use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use futures_util::FutureExt;
use ring::hmac;
use snafu::ResultExt;
use synchrony::sync::event::Event;
use upgrid_config::{
    Config, JoinLink, LocalAddress, MAX_DISCOVERY_SERVICES, is_supported_discovery_url,
    store_discovery_urls, store_node_name, store_pending_join, store_reachable_addresses,
};
use upgrid_raft::ReachableAddress;
use url::Url;

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
    local_addresses: BTreeSet<LocalAddress>,
    reachable_addresses: Vec<String>,
    discovery_urls: Vec<String>,
    reachable_addresses_explicit: bool,
    discovery_urls_explicit: bool,
    result: Arc<Mutex<Option<OobeChoice>>>,
    accepted: Arc<Event>,
    deadline: Arc<Event>,
}

#[derive(Clone)]
struct SetupAuth {
    key: hmac::Key,
    expected: hmac::Tag,
}

pub struct OobeNetworkSources {
    pub reachable_addresses: BTreeSet<ReachableAddress>,
    pub reachable_addresses_explicit: bool,
    pub discovery_urls: BTreeSet<Url>,
    pub discovery_urls_explicit: bool,
}

pub enum OobeChoice {
    NewCluster {
        node_name: String,
        admin_username: String,
        network: OobeNetworkSources,
        admin_password: String,
    },
    Join {
        node_name: String,
        link: Box<JoinLink>,
        network: OobeNetworkSources,
    },
}

impl SetupAuth {
    fn new(username: &str, password: &str) -> Self {
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"upgrid-oobe-basic-auth-v1");
        let expected = hmac::sign(&key, format!("{username}:{password}").as_bytes());
        Self { key, expected }
    }

    fn accepts(&self, headers: &HeaderMap) -> bool {
        let Some(encoded) = basic_credentials(headers) else {
            return false;
        };
        let Ok(credentials) = STANDARD.decode(encoded) else {
            return false;
        };
        hmac::verify(&self.key, &credentials, self.expected.as_ref()).is_ok()
    }
}

async fn require_setup_auth(
    State(auth): State<SetupAuth>,
    request: Request,
    next: Next,
) -> Response {
    if auth.accepts(request.headers()) {
        next.run(request).await
    } else {
        setup_unauthorized()
    }
}

fn basic_credentials(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let mut parts = value.split_ascii_whitespace();
    let scheme = parts.next()?;
    let credentials = parts.next()?;
    (scheme.eq_ignore_ascii_case("Basic") && parts.next().is_none()).then_some(credentials)
}

fn setup_unauthorized() -> Response {
    let mut response = (StatusCode::UNAUTHORIZED, "authentication required").into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static(r#"Basic realm="UpGrid setup", charset="UTF-8""#),
    );
    response
}

pub fn wait_for_oobe(config: &Config, node_name: &str) -> Result<OobeChoice> {
    let listener = std::net::TcpListener::bind(&config.bind).context(BindSnafu {
        address: config.bind.clone(),
    })?;
    listener.set_nonblocking(true).context(ListenerSnafu)?;
    let result = Arc::new(Mutex::new(None));
    let accepted = Arc::new(Event::new());
    let accepted_signal = accepted.listen();
    let deadline = Arc::new(Event::new());
    let deadline_signal = deadline.listen();
    let state = SetupState {
        data_dir: config.data_dir.clone(),
        node_name: Arc::new(Mutex::new(node_name.to_owned())),
        local_addresses: config.local_addresses.clone(),
        reachable_addresses: config
            .reachable_addresses
            .iter()
            .map(ToString::to_string)
            .collect(),
        discovery_urls: config
            .discovery_urls
            .iter()
            .map(ToString::to_string)
            .collect(),
        reachable_addresses_explicit: config.reachable_addresses_explicit,
        discovery_urls_explicit: config.discovery_urls_explicit,
        result: result.clone(),
        accepted,
        deadline,
    };
    let auth = SetupAuth::new(&config.username, &config.password);
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
        .layer(middleware::from_fn_with_state(auth, require_setup_auth))
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"status": "ok"})) }),
        )
        .with_state(state);
    let runtime = compio::runtime::Runtime::new().context(RuntimeSnafu)?;
    let tls_cert = config.tls_cert.clone();
    let tls_key = config.tls_key.clone();
    tracing::info!(bind = %config.bind, "WebUI ready for cluster setup");
    runtime.block_on(async move {
        let listener = compio::net::TcpListener::from_std(listener).context(ListenerSnafu)?;
        let shutdown = accepted_signal;
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
            deadline_signal.await;
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
    let network = parse_network_sources(input.reachable_addresses, input.discovery_urls)?;
    let link = JoinLink::parse(input.join_link.trim()).map_err(ApiError::bad_request)?;
    persist_network_sources(&state, &network)?;
    accept(
        &state,
        OobeChoice::Join {
            node_name,
            link: Box::new(link),
            network,
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
    let network = parse_network_sources(input.reachable_addresses, input.discovery_urls)?;
    persist_network_sources(&state, &network)?;
    accept(
        &state,
        OobeChoice::NewCluster {
            node_name,
            admin_username: input.admin_username,
            admin_password: input.admin_password,
            network,
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
        local_addresses: state
            .local_addresses
            .iter()
            .copied()
            .map(Into::into)
            .collect(),
        reachable_addresses: state.reachable_addresses.clone(),
        discovery_urls: state.discovery_urls.clone(),
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

fn parse_network_sources(
    reachable_addresses: Vec<String>,
    discovery_urls: Vec<String>,
) -> Result<OobeNetworkSources, ApiError> {
    Ok(OobeNetworkSources {
        reachable_addresses: parse_reachable_addresses(reachable_addresses)?,
        reachable_addresses_explicit: true,
        discovery_urls: parse_discovery_urls(discovery_urls)?,
        discovery_urls_explicit: true,
    })
}

fn parse_reachable_addresses(
    addresses: Vec<String>,
) -> Result<BTreeSet<ReachableAddress>, ApiError> {
    addresses
        .into_iter()
        .filter(|address| !address.trim().is_empty())
        .map(|address| ReachableAddress::parse(address.trim()).map_err(ApiError::bad_request))
        .collect()
}

fn parse_discovery_urls(urls: Vec<String>) -> Result<BTreeSet<Url>, ApiError> {
    let urls = urls
        .into_iter()
        .filter(|url| !url.trim().is_empty())
        .map(|value| {
            let url: Url = value.trim().parse().map_err(ApiError::bad_request)?;
            if !is_supported_discovery_url(&url) {
                return Err(ApiError::bad_request(
                    "Discovery service URLs must use http or https and contain no credentials, \
                     query, or fragment",
                ));
            }
            Ok(url)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if urls.len() > MAX_DISCOVERY_SERVICES {
        return Err(ApiError::bad_request(format!(
            "At most {MAX_DISCOVERY_SERVICES} discovery services are allowed"
        )));
    }
    Ok(urls)
}

fn persist_network_sources(
    state: &SetupState,
    additions: &OobeNetworkSources,
) -> Result<(), ApiError> {
    let addresses =
        if !state.reachable_addresses_explicit || !additions.reachable_addresses.is_empty() {
            let mut addresses = if state.reachable_addresses_explicit {
                parse_reachable_addresses(state.reachable_addresses.clone())?
            } else {
                BTreeSet::new()
            };
            addresses.extend(additions.reachable_addresses.iter().cloned());
            Some(addresses)
        } else {
            None
        };
    let urls = if !state.discovery_urls_explicit || !additions.discovery_urls.is_empty() {
        let mut urls = if state.discovery_urls_explicit {
            parse_discovery_urls(state.discovery_urls.clone())?
        } else {
            BTreeSet::new()
        };
        urls.extend(additions.discovery_urls.iter().cloned());
        if urls.len() > MAX_DISCOVERY_SERVICES {
            return Err(ApiError::bad_request(format!(
                "At most {MAX_DISCOVERY_SERVICES} discovery services are allowed"
            )));
        }
        Some(urls)
    } else {
        None
    };
    if let Some(addresses) = addresses {
        store_reachable_addresses(&state.data_dir, &addresses).map_err(ApiError::unavailable)?;
    }
    if let Some(urls) = urls {
        store_discovery_urls(&state.data_dir, &urls).map_err(ApiError::unavailable)?;
    }
    Ok(())
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
    if let OobeChoice::Join { link, .. } = &choice {
        store_pending_join(&state.data_dir, link, false).map_err(ApiError::unavailable)?;
    }
    *result = Some(choice);
    drop(result);
    state.accepted.notify(1);
    state.deadline.notify(1);
    Ok(())
}

#[cfg(test)]
mod persistence_tests;

#[cfg(test)]
mod tests;
