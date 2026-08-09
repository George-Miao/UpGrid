use super::assets::*;
use super::join::*;
use super::resources::*;
use super::targets::*;
use super::*;

pub fn start(
    config: Config,
    cluster: Handle,
    cipher: Cipher,
    notifications: upgrid_notification::Tester,
    oobe: Oobe,
    startup_warning: Option<String>,
) -> AppResult<()> {
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
        })?;
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
) -> AppResult<()> {
    let tls_cert = config.tls_cert.clone();
    let tls_key = config.tls_key.clone();
    let state = WebState {
        cluster,
        cipher,
        notifications,
        raft_url: config.raft_url,
        username: config.username,
        password: config.password,
        node_name: config
            .node_name
            .expect("orchestration resolves the Node name before starting the API"),
        oobe,
        startup_warning,
    };
    let (api, mut openapi) = api_routes().with_state(state.clone()).split_for_parts();
    configure_openapi(&mut openapi);
    let specification = std::sync::Arc::new(openapi);
    let protected = Router::new()
        .merge(api)
        .route("/", get(index))
        .route("/alerts", get(index))
        .route("/cluster", get(index))
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
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));
    let app = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"status": "ok"})) }),
        )
        .route("/assets/upgrid.js", get(webui_script))
        .route("/favicon.svg", get(favicon))
        .merge(protected);
    if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
        axum_server::from_tcp_rustls(listener, tls)?
            .serve(app.into_make_service())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        axum::serve(listener, app).await?;
    }
    Ok(())
}

fn api_routes() -> OpenApiRouter<WebState> {
    OpenApiRouter::new()
        .routes(routes!(list_targets, create_target))
        .routes(routes!(get_target, update_target, delete_target))
        .routes(routes!(pause_target))
        .routes(routes!(resume_target))
        .routes(routes!(list_channels, create_channel))
        .routes(routes!(set_channel_default))
        .routes(routes!(test_channel))
        .routes(routes!(delete_channel))
        .routes(routes!(list_secrets, create_secret))
        .routes(routes!(delete_secret))
        .routes(routes!(list_join_tokens, create_join_token))
        .routes(routes!(revoke_join_token))
        .routes(routes!(list_alerts))
        .routes(routes!(list_transitions))
        .routes(routes!(get_cluster))
        .routes(routes!(get_setup))
        .routes(routes!(advance_setup))
        .routes(routes!(join_cluster))
        .routes(routes!(create_cluster))
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
