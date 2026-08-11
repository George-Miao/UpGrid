use std::collections::{BTreeMap, BTreeSet};

use http::{Method, StatusCode, header};
use upgrid_config::Cipher;
use upgrid_raft::Handle;
use upgrid_raft::domain::{
    ApplicationState, ConfigValue, HttpTarget, MAX_RESPONSE_BYTES, resolve_config_value,
};
use url::Url;

use super::Clients;

pub(super) struct Request {
    method: Method,
    url: Url,
    headers: BTreeMap<String, String>,
    sensitive_headers: BTreeSet<String>,
    body: Vec<u8>,
    follow_redirects: bool,
    redirects_left: u8,
    skip_tls_verification: bool,
}

pub(super) struct Response {
    pub status: StatusCode,
    pub body: Vec<u8>,
    pub url: Url,
}

pub(super) async fn resolve(
    cluster: &Handle,
    cipher: &Cipher,
    target: &HttpTarget,
) -> Result<Request, String> {
    let state = cluster.read().await?;
    let sensitive_headers = target
        .headers
        .iter()
        .filter(|(_, value)| matches!(value, ConfigValue::Secret(_)))
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect();
    let headers = target
        .headers
        .iter()
        .map(|(name, value)| {
            resolve_value(&state, cipher, value).map(|value| (name.clone(), value))
        })
        .collect::<Result<_, _>>()?;
    let body = target
        .body
        .as_ref()
        .map(|value| resolve_value(&state, cipher, value))
        .transpose()?
        .unwrap_or_default()
        .into_bytes();
    Ok(Request {
        method: Method::from_bytes(target.method.as_bytes()).map_err(|error| error.to_string())?,
        url: target.url.clone(),
        headers,
        sensitive_headers,
        body,
        follow_redirects: target.follow_redirects,
        redirects_left: target.max_redirects,
        skip_tls_verification: target.skip_tls_verification,
    })
}

fn resolve_value(
    state: &ApplicationState,
    cipher: &Cipher,
    value: &ConfigValue,
) -> Result<String, String> {
    match resolve_config_value(state, cipher, value) {
        Ok(value) => Ok(value),
        Err(error) => Err(error.to_string()),
    }
}

pub(super) async fn send(clients: &Clients, mut request: Request) -> Result<Response, String> {
    loop {
        let client = if request.skip_tls_verification {
            &clients.insecure
        } else {
            &clients.verified
        };
        let mut builder = client
            .request(request.method.clone(), request.url.clone())
            .map_err(|error| error.to_string())?;
        for (name, value) in &request.headers {
            builder = builder
                .header(name.as_str(), value.as_str())
                .map_err(|error| error.to_string())?;
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body.clone());
        }
        let response = builder.send().await.map_err(|error| error.to_string())?;
        let status = response.status();
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if request.follow_redirects && status.is_redirection() && location.is_some() {
            if request.redirects_left == 0 {
                return Err("redirect limit exceeded".to_owned());
            }
            request.redirects_left -= 1;
            let next = request
                .url
                .join(location.as_deref().unwrap_or_default())
                .map_err(|error| format!("invalid redirect: {error}"))?;
            if request.url.origin() != next.origin() {
                strip_cross_origin_headers(&mut request.headers, &request.sensitive_headers);
            }
            if status == StatusCode::SEE_OTHER
                || ((status == StatusCode::MOVED_PERMANENTLY || status == StatusCode::FOUND)
                    && request.method != Method::GET
                    && request.method != Method::HEAD)
            {
                request.method = Method::GET;
                request.body.clear();
            }
            request.url = next;
            continue;
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES)
        {
            return Err(format!("response body exceeds {MAX_RESPONSE_BYTES} bytes"));
        }
        let url = request.url;
        let body = response.bytes().await.map_err(|error| error.to_string())?;
        if body.len() > MAX_RESPONSE_BYTES as usize {
            return Err(format!("response body exceeds {MAX_RESPONSE_BYTES} bytes"));
        }
        return Ok(Response {
            status,
            body: body.to_vec(),
            url,
        });
    }
}

fn strip_cross_origin_headers(
    headers: &mut BTreeMap<String, String>,
    sensitive: &BTreeSet<String>,
) {
    headers.retain(|name, _| {
        let name = name.to_ascii_lowercase();
        !sensitive.contains(&name)
            && !matches!(
                name.as_str(),
                "authorization" | "cookie" | "proxy-authorization"
            )
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_origin_redirect_strips_secret_backed_headers() {
        let mut headers = BTreeMap::from([
            ("x-api-key".to_owned(), "secret".to_owned()),
            ("x-public".to_owned(), "visible".to_owned()),
        ]);
        let sensitive = BTreeSet::from(["x-api-key".to_owned()]);

        strip_cross_origin_headers(&mut headers, &sensitive);

        assert!(!headers.contains_key("x-api-key"));
        assert_eq!(headers["x-public"], "visible");
    }
}
