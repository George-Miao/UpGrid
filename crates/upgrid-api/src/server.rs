use axum::middleware;
use snafu::ResultExt;

use super::assets::*;
use super::auth::*;
use super::channels::*;
use super::history::*;
use super::identities::*;
use super::join::*;
use super::nodes::*;
use super::resources::*;
use super::status::*;
use super::targets::*;
use super::*;
use crate::error::{
    BindSnafu, ListenerSnafu, OpenApiSnafu, RuntimeSnafu, ServeSnafu, ThreadSpawnSnafu, TlsSnafu,
};
use crate::listener::{TlsListener, tls_acceptor};

pub fn start(
    config: Config,
    cluster: Handle,
    cipher: Cipher,
    quic_ca_key: QuicCaKey,
    notifications: upgrid_notification::Tester,
    oobe: Oobe,
    startup_warning: Option<String>,
) -> Result<()> {
    let listener = std::net::TcpListener::bind(&config.bind).context(BindSnafu {
        address: config.bind.clone(),
    })?;
    listener.set_nonblocking(true).context(ListenerSnafu)?;
    let bind = config.bind.clone();
    let state = WebState {
        cluster,
        cipher,
        quic_ca_key,
        notifications,
        raft_url: config.raft_url.clone(),
        node_name: config
            .node_name
            .clone()
            .expect("Orchestration resolves the node name before starting the API"),
        oobe,
        startup_warning,
    };
    let (runtime_tx, runtime_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("upgrid-web".to_owned())
        .spawn(move || {
            let runtime = match compio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = runtime_tx.send(Err(error));
                    return;
                }
            };
            if runtime_tx.send(Ok(())).is_err() {
                return;
            }
            if let Err(error) = runtime.block_on(serve(listener, config, state)) {
                tracing::error!(%error, "web API stopped");
            }
        })
        .context(ThreadSpawnSnafu)?;
    runtime_rx
        .recv()
        .map_err(std::io::Error::other)
        .context(RuntimeSnafu)?
        .context(RuntimeSnafu)?;
    tracing::debug!(%bind, "web API ready");
    Ok(())
}

async fn serve(listener: std::net::TcpListener, config: Config, state: WebState) -> Result<()> {
    let tls_cert = config.tls_cert.clone();
    let tls_key = config.tls_key.clone();

    let (api, mut openapi) = api_routes().with_state(state.clone()).split_for_parts();
    let (status, status_openapi) = status_routes()
        .with_state::<()>(state.clone())
        .split_for_parts();
    let status = status.layer(middleware::from_fn_with_state(
        state.clone(),
        require_public_status_enabled,
    ));
    openapi.merge(status_openapi);
    configure_openapi(&mut openapi);
    let specification = std::sync::Arc::new(openapi);
    let api = Router::new()
        .merge(api)
        .layer(middleware::from_fn_with_state(state, require_auth));
    let app = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"status": "ok"})) }),
        )
        .route("/assets/upgrid.js", get(webui_script))
        .route("/favicon.svg", get(favicon))
        .route("/", get(index))
        .route("/alerts", get(index))
        .route("/cluster", get(index))
        .route("/trash", get(index))
        .route("/admin/change-password", get(index))
        .route("/admin/users", get(index))
        .route("/admin/api-tokens", get(index))
        .route("/admin/manage", get(index))
        .route("/setup", get(index))
        .route("/setup/channel", get(index))
        .route("/setup/target", get(index))
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
        .merge(status)
        .merge(api);
    let listener = compio::net::TcpListener::from_std(listener).context(ListenerSnafu)?;
    if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        let acceptor = tls_acceptor(&cert, &key).context(TlsSnafu)?;
        cyper_axum::serve(TlsListener::new(listener, acceptor), app)
            .await
            .context(ServeSnafu)?;
    } else {
        cyper_axum::serve(listener, app).await.context(ServeSnafu)?;
    }
    Ok(())
}

fn api_routes() -> OpenApiRouter<WebState> {
    OpenApiRouter::new()
        .routes(routes!(login))
        .routes(routes!(session))
        .routes(routes!(logout))
        .routes(routes!(list_identities, create_identity))
        .routes(routes!(update_identity, delete_identity))
        .routes(routes!(list_api_tokens, create_api_token))
        .routes(routes!(revoke_api_token))
        .routes(routes!(get_settings, update_settings))
        .routes(routes!(list_targets, create_target))
        .routes(routes!(get_target, update_target, delete_target))
        .routes(routes!(get_target_history))
        .routes(routes!(list_trashed_targets))
        .routes(routes!(restore_target))
        .routes(routes!(purge_target))
        .routes(routes!(pause_target))
        .routes(routes!(resume_target))
        .routes(routes!(rename_node))
        .routes(routes!(set_node_drain))
        .routes(routes!(remove_node))
        .routes(routes!(list_channels, create_channel))
        .routes(routes!(update_channel))
        .routes(routes!(set_channel_default))
        .routes(routes!(crate::channels::test::test_channel))
        .routes(routes!(delete_channel))
        .routes(routes!(list_secrets, create_secret))
        .routes(routes!(delete_secret))
        .routes(routes!(delete_unreferenced_secrets))
        .routes(routes!(list_join_tokens, create_join_token))
        .routes(routes!(revoke_join_token))
        .routes(routes!(list_alerts))
        .routes(routes!(acknowledge_alert))
        .routes(routes!(retry_alert))
        .routes(routes!(list_transitions))
        .routes(routes!(get_cluster))
        .routes(routes!(get_setup))
        .routes(routes!(advance_setup))
        .routes(routes!(join_cluster))
        .routes(routes!(create_cluster))
        .routes(routes!(events))
}

fn status_routes() -> OpenApiRouter<WebState> {
    OpenApiRouter::new().routes(routes!(get_status))
}

pub fn openapi_json() -> Result<String> {
    let (_, mut openapi) = api_routes().split_for_parts();
    let (_, status_openapi) = status_routes().split_for_parts();
    openapi.merge(status_openapi);
    configure_openapi(&mut openapi);
    serde_json::to_string_pretty(&openapi)
        .context(OpenApiSnafu)
        .map(|mut json| {
            json.push('\n');
            json
        })
}

fn configure_openapi(openapi: &mut utoipa::openapi::OpenApi) {
    openapi.info = Info::new("UpGrid cluster API", env!("CARGO_PKG_VERSION"));
    openapi
        .components
        .get_or_insert_with(Components::new)
        .add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
    openapi.security = Some(vec![SecurityRequirement::new(
        "bearerAuth",
        std::iter::empty::<String>(),
    )]);
}
