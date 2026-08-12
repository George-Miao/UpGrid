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
use super::targets::*;
use super::*;
use crate::error::{
    BindSnafu, ListenerSnafu, OpenApiSnafu, RuntimeSnafu, ServeSnafu, ThreadSpawnSnafu, TlsSnafu,
};

pub fn start(
    config: Config,
    cluster: Handle,
    cipher: Cipher,
    notifications: upgrid_notification::Tester,
    oobe: Oobe,
    startup_warning: Option<String>,
) -> Result<()> {
    let listener = std::net::TcpListener::bind(&config.bind).context(BindSnafu {
        address: config.bind.clone(),
    })?;
    listener.set_nonblocking(true).context(ListenerSnafu)?;
    let bind = config.bind.clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("upgrid-web-worker")
        .build()
        .context(RuntimeSnafu)?;
    std::thread::Builder::new()
        .name("upgrid-web".to_owned())
        .spawn(move || {
            if let Err(error) = runtime.block_on(serve(
                listener,
                config,
                cluster,
                cipher,
                notifications,
                oobe,
                startup_warning,
            )) {
                tracing::error!(%error, "web API stopped");
            }
        })
        .context(ThreadSpawnSnafu)?;
    tracing::debug!(%bind, "web API ready");
    Ok(())
}

async fn serve(
    listener: std::net::TcpListener,
    config: Config,
    cluster: Handle,
    cipher: Cipher,
    notifications: upgrid_notification::Tester,
    oobe: Oobe,
    startup_warning: Option<String>,
) -> Result<()> {
    let tls_cert = config.tls_cert.clone();
    let tls_key = config.tls_key.clone();
    let state = WebState {
        cluster,
        cipher,
        notifications,
        raft_url: config.raft_url,
        node_name: config
            .node_name
            .expect("orchestration resolves the Node name before starting the API"),
        oobe,
        startup_warning,
    };
    let (api, mut openapi) = api_routes().with_state(state.clone()).split_for_parts();
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
        .merge(api);
    if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
            .await
            .context(TlsSnafu)?;
        axum_server::from_tcp_rustls(listener, tls)
            .context(ServeSnafu)?
            .serve(app.into_make_service())
            .await
            .context(ServeSnafu)?;
    } else {
        let listener = tokio::net::TcpListener::from_std(listener).context(ListenerSnafu)?;
        axum::serve(listener, app).await.context(ServeSnafu)?;
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
        .routes(routes!(test_channel))
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

pub fn openapi_json() -> Result<String> {
    let (_, mut openapi) = api_routes().split_for_parts();
    configure_openapi(&mut openapi);
    serde_json::to_string_pretty(&openapi)
        .context(OpenApiSnafu)
        .map(|mut json| {
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
            "bearerAuth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
    openapi.security = Some(vec![SecurityRequirement::new(
        "bearerAuth",
        std::iter::empty::<String>(),
    )]);
}
