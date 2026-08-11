use std::collections::{BTreeMap, BTreeSet};

use http::{Method, StatusCode, header};
use snafu::{ResultExt, Snafu};
use upgrid_config::Cipher;
use upgrid_raft::domain::{
    ApplicationState, ConfigValue, ConfigValueError, HttpTarget, MAX_RESPONSE_BYTES,
    resolve_config_value,
};
use upgrid_raft::{ClusterError, Handle};
use url::Url;

use super::runtime::Clients;

#[derive(Debug, Snafu)]
pub(super) enum Error {
    #[snafu(display("{source}"))]
    Cluster { source: ClusterError },

    #[snafu(display("{source}"))]
    ConfigValue { source: ConfigValueError },

    #[snafu(display("invalid HTTP method {method}: {source}"))]
    Method {
        method: String,
        source: http::method::InvalidMethod,
    },

    #[snafu(display("failed to construct HTTP request: {source}"))]
    Request { source: cyper::Error },

    #[snafu(display("invalid HTTP header {name}: {source}"))]
    Header { name: String, source: cyper::Error },

    #[snafu(display("HTTP request failed: {source}"))]
    Send { source: cyper::Error },

    #[snafu(display("redirect limit exceeded"))]
    RedirectLimit,

    #[snafu(display("invalid redirect {location}: {source}"))]
    Redirect {
        location: String,
        source: url::ParseError,
    },

    #[snafu(display("failed to read HTTP response: {source}"))]
    ResponseBody { source: cyper::Error },

    #[snafu(display("response body exceeds {limit} bytes"))]
    ResponseTooLarge { limit: u64 },

    #[snafu(display("request timed out"))]
    RequestTimeout,
}

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
) -> Result<Request, Error> {
    let state = cluster.read().await.context(ClusterSnafu)?;
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
    let method = Method::from_bytes(target.method.as_bytes()).context(MethodSnafu {
        method: target.method.clone(),
    })?;
    Ok(Request {
        method,
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
) -> Result<String, Error> {
    resolve_config_value(state, cipher, value).context(ConfigValueSnafu)
}

pub(super) async fn send(clients: &Clients, mut request: Request) -> Result<Response, Error> {
    loop {
        let client = if request.skip_tls_verification {
            &clients.insecure
        } else {
            &clients.verified
        };
        let mut builder = client
            .request(request.method.clone(), request.url.clone())
            .context(RequestSnafu)?;
        for (name, value) in &request.headers {
            builder = builder
                .header(name.as_str(), value.as_str())
                .context(HeaderSnafu { name: name.clone() })?;
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body.clone());
        }
        let response = builder.send().await.context(SendSnafu)?;
        let status = response.status();
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if request.follow_redirects && status.is_redirection() && location.is_some() {
            if request.redirects_left == 0 {
                return Err(Error::RedirectLimit);
            }
            request.redirects_left -= 1;
            let location = location.unwrap_or_default();
            let next = request
                .url
                .join(&location)
                .context(RedirectSnafu { location })?;
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
            return Err(Error::ResponseTooLarge {
                limit: MAX_RESPONSE_BYTES,
            });
        }
        let url = request.url;
        let body = response.bytes().await.context(ResponseBodySnafu)?;
        if body.len() > MAX_RESPONSE_BYTES as usize {
            return Err(Error::ResponseTooLarge {
                limit: MAX_RESPONSE_BYTES,
            });
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
