use super::assets::*;
use super::resources::*;
use super::targets::*;
use super::*;

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
        raft_url: config.raft_url,
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
        .route("/assets/app.js", get(webui_script))
        .route("/favicon.svg", get(favicon))
        .merge(protected);
    let listener = tokio::net::TcpListener::from_std(listener)?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn api_routes() -> OpenApiRouter<WebState> {
    OpenApiRouter::new()
        .routes(routes!(list_targets, create_target))
        .routes(routes!(get_target, update_target, delete_target))
        .routes(routes!(pause_target))
        .routes(routes!(resume_target))
        .routes(routes!(list_channels, create_channel))
        .routes(routes!(delete_channel))
        .routes(routes!(list_secrets, create_secret))
        .routes(routes!(delete_secret))
        .routes(routes!(create_join_link))
        .routes(routes!(list_alerts))
        .routes(routes!(get_cluster))
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
