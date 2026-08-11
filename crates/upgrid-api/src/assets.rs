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
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/assets/upgrid.js"
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
