use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

use crate::ErrorBody;

pub(super) fn verify_basic_credentials(
    authorization: Option<&HeaderValue>,
    username: &str,
    password: &str,
) -> bool {
    let Some(encoded) = authorization
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
    else {
        return false;
    };
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return false;
    };
    decoded
        .strip_prefix(username.as_bytes())
        .and_then(|value| value.strip_prefix(b":"))
        .is_some_and(|value| value == password.as_bytes())
}

pub(super) fn unauthorized(realm: &str) -> Response {
    let mut response = (
        StatusCode::UNAUTHORIZED,
        Json(ErrorBody {
            error: "authentication required".to_owned(),
        }),
    )
        .into_response();
    let challenge = HeaderValue::try_from(format!("Basic realm=\"{realm}\""))
        .expect("authentication realms form valid header values");
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, challenge);
    response
}

#[cfg(test)]
mod tests {
    use axum::http::header;

    use super::*;

    #[test]
    fn basic_credentials_must_decode_to_the_exact_pair() {
        let valid = HeaderValue::try_from(format!(
            "Basic {}",
            STANDARD.encode("operator:correct horse")
        ))
        .unwrap();
        let wrong_password =
            HeaderValue::try_from(format!("Basic {}", STANDARD.encode("operator:wrong horse")))
                .unwrap();

        assert!(verify_basic_credentials(
            Some(&valid),
            "operator",
            "correct horse"
        ));
        assert!(!verify_basic_credentials(
            Some(&wrong_password),
            "operator",
            "correct horse"
        ));
        assert!(!verify_basic_credentials(
            Some(&HeaderValue::from_static("Bearer token")),
            "operator",
            "correct horse"
        ));
        assert!(!verify_basic_credentials(
            Some(&HeaderValue::from_static("Basic not-base64")),
            "operator",
            "correct horse"
        ));
    }

    #[test]
    fn unauthorized_response_uses_the_requested_basic_realm() {
        let response = unauthorized("UpGrid setup");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"UpGrid setup\""
        );
    }
}
