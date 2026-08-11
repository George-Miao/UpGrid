use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use http::{Method, StatusCode, header};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use snafu::{ResultExt, Snafu};
use upgrid_config::Cipher;
use upgrid_raft::domain::{
    ApplicationState, ConfigValue, ConfigValueError, HttpTarget, MAX_RESPONSE_BYTES, SecretId,
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

    #[snafu(display("failed to construct custom TLS client: {source}"))]
    Client { source: cyper::Error },

    #[snafu(display("invalid {name}: {diagnostic}"))]
    TlsCredential {
        name: &'static str,
        diagnostic: String,
    },

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
    custom_tls: Option<Arc<ClientConfig>>,
}

pub(super) struct Response {
    pub status: StatusCode,
    pub headers: BTreeMap<String, String>,
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
    let custom_tls = resolve_tls(&state, cipher, target)?;
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
        custom_tls,
    })
}

fn resolve_value(
    state: &ApplicationState,
    cipher: &Cipher,
    value: &ConfigValue,
) -> Result<String, Error> {
    resolve_config_value(state, cipher, value).context(ConfigValueSnafu)
}

fn resolve_tls(
    state: &ApplicationState,
    cipher: &Cipher,
    target: &HttpTarget,
) -> Result<Option<Arc<ClientConfig>>, Error> {
    let ca = target
        .tls_ca_secret
        .map(|id| resolve_secret(state, cipher, id))
        .transpose()?;
    let certificate = target
        .tls_client_certificate_secret
        .map(|id| resolve_secret(state, cipher, id))
        .transpose()?;
    let private_key = target
        .tls_client_private_key_secret
        .map(|id| resolve_secret(state, cipher, id))
        .transpose()?;
    if ca.is_none() && certificate.is_none() && private_key.is_none() {
        return Ok(None);
    }
    custom_tls_config(
        ca.as_deref(),
        certificate.as_deref(),
        private_key.as_deref(),
    )
    .map(Some)
}

fn resolve_secret(
    state: &ApplicationState,
    cipher: &Cipher,
    id: SecretId,
) -> Result<String, Error> {
    resolve_value(state, cipher, &ConfigValue::Secret(id))
}

fn custom_tls_config(
    ca_pem: Option<&str>,
    certificate_pem: Option<&str>,
    private_key_pem: Option<&str>,
) -> Result<Arc<ClientConfig>, Error> {
    let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(ca_pem) = ca_pem {
        for certificate in certificates("custom CA bundle", ca_pem)? {
            roots
                .add(certificate)
                .map_err(|error| invalid_tls("custom CA bundle", error))?;
        }
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match (certificate_pem, private_key_pem) {
        (Some(certificate_pem), Some(private_key_pem)) => {
            let certificates = certificates("client certificate", certificate_pem)?;
            let private_key = PrivateKeyDer::from_pem_slice(private_key_pem.as_bytes())
                .map_err(|error| invalid_tls("client private key", error))?;
            builder
                .with_client_auth_cert(certificates, private_key)
                .map_err(|error| invalid_tls("client certificate and private key", error))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(invalid_tls(
                "client identity",
                "certificate and private key must both be configured",
            ));
        }
    };
    Ok(Arc::new(config))
}

fn certificates(name: &'static str, pem: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
    let certificates = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid_tls(name, error))?;
    if certificates.is_empty() {
        return Err(invalid_tls(name, "PEM contains no certificates"));
    }
    Ok(certificates)
}

fn invalid_tls(name: &'static str, error: impl std::fmt::Display) -> Error {
    Error::TlsCredential {
        name,
        diagnostic: error.to_string(),
    }
}

pub(super) async fn send(clients: &Clients, mut request: Request) -> Result<Response, Error> {
    let custom_client = request
        .custom_tls
        .take()
        .map(|config| {
            cyper::Client::builder()
                .use_rustls(config)
                .build()
                .context(ClientSnafu)
        })
        .transpose()?;
    loop {
        let client = match &custom_client {
            Some(client) => client,
            None if request.skip_tls_verification => &clients.insecure,
            None => &clients.verified,
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
        let mut headers = BTreeMap::<String, String>::new();
        for (name, value) in response.headers() {
            let Ok(value) = value.to_str() else {
                continue;
            };
            headers
                .entry(name.as_str().to_ascii_lowercase())
                .and_modify(|current| {
                    current.push_str(", ");
                    current.push_str(value);
                })
                .or_insert_with(|| value.to_owned());
        }
        let url = request.url;
        let body = response.bytes().await.context(ResponseBodySnafu)?;
        if body.len() > MAX_RESPONSE_BYTES as usize {
            return Err(Error::ResponseTooLarge {
                limit: MAX_RESPONSE_BYTES,
            });
        }
        return Ok(Response {
            headers,
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
mod tests;
