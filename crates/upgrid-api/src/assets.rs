use super::*;

pub(super) async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/index.html"
        )),
    )
}

pub(super) async fn webui_script() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/assets/app.js"
        ))
        .as_slice(),
    )
}

pub(super) async fn favicon() -> impl IntoResponse {
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

pub(super) async fn require_auth(
    State(state): State<WebState>,
    request: Request,
    next: Next,
) -> Response {
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
