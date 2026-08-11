use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use url::Url;

use super::model::is_http_token;
use super::{
    ConfigValue, DomainError, HttpAssertion, MAX_HTTP_ASSERTIONS, NotificationChannelId, SecretId,
    TargetId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusRange {
    pub start: u16,
    pub end: u16,
}

impl StatusRange {
    pub fn new(start: u16, end: u16) -> Self {
        Self { start, end }
    }

    pub fn contains(&self, status: u16) -> bool {
        self.start <= status && status <= self.end
    }

    fn is_valid(&self) -> bool {
        100 <= self.start && self.start <= self.end && self.end <= 599
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpTarget {
    pub url: Url,
    pub method: String,
    pub headers: BTreeMap<String, ConfigValue>,
    pub body: Option<ConfigValue>,
    pub accepted_statuses: Vec<StatusRange>,
    pub follow_redirects: bool,
    pub max_redirects: u8,
    pub assertions: Vec<HttpAssertion>,
    pub skip_tls_verification: bool,
    pub tls_ca_secret: Option<SecretId>,
    pub tls_client_certificate_secret: Option<SecretId>,
    pub tls_client_private_key_secret: Option<SecretId>,
}

impl HttpTarget {
    pub fn get(url: Url) -> Self {
        Self {
            url,
            method: "GET".to_owned(),
            headers: BTreeMap::new(),
            body: None,
            accepted_statuses: vec![StatusRange::new(200, 299)],
            follow_redirects: true,
            max_redirects: 5,
            assertions: Vec::new(),
            skip_tls_verification: false,
            tls_ca_secret: None,
            tls_client_certificate_secret: None,
            tls_client_private_key_secret: None,
        }
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        let kind = TargetKind::from_scheme(self.url.scheme()).ok_or_else(|| {
            DomainError::InvalidTarget(
                "target URL must use http, https, tcp, dns, icmp, or tls".to_owned(),
            )
        })?;
        if self.url.host_str().is_none() {
            return Err(DomainError::InvalidTarget(
                "target endpoint must include a host".to_owned(),
            ));
        }
        if kind != TargetKind::Http {
            return self.validate_network(kind);
        }
        if !is_http_token(&self.method) {
            return Err(DomainError::InvalidTarget(
                "HTTP method is not a valid token".to_owned(),
            ));
        }
        if self.accepted_statuses.is_empty()
            || self.accepted_statuses.iter().any(|range| !range.is_valid())
        {
            return Err(DomainError::InvalidTarget(
                "accepted HTTP statuses must contain valid ranges".to_owned(),
            ));
        }
        if self.follow_redirects && self.max_redirects == 0 {
            return Err(DomainError::InvalidTarget(
                "redirect limit must be greater than zero".to_owned(),
            ));
        }
        if self.assertions.len() > MAX_HTTP_ASSERTIONS {
            return Err(DomainError::InvalidTarget(format!(
                "HTTP Targets accept at most {MAX_HTTP_ASSERTIONS} assertions"
            )));
        }
        for assertion in &self.assertions {
            assertion.validate()?;
        }
        let tls_configured = self.tls_ca_secret.is_some()
            || self.tls_client_certificate_secret.is_some()
            || self.tls_client_private_key_secret.is_some();
        if tls_configured && self.url.scheme() != "https" {
            return Err(DomainError::InvalidTarget(
                "custom TLS credentials require an HTTPS Target".to_owned(),
            ));
        }
        if tls_configured && self.skip_tls_verification {
            return Err(DomainError::InvalidTarget(
                "custom TLS credentials cannot be combined with skipped TLS verification"
                    .to_owned(),
            ));
        }
        if self.tls_client_certificate_secret.is_some()
            != self.tls_client_private_key_secret.is_some()
        {
            return Err(DomainError::InvalidTarget(
                "mutual TLS requires both client certificate and private key Secrets".to_owned(),
            ));
        }

        let mut names = BTreeSet::new();
        for name in self.headers.keys() {
            if !is_http_token(name) {
                return Err(DomainError::InvalidTarget(format!(
                    "invalid HTTP header name: {name}"
                )));
            }
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(DomainError::InvalidTarget(format!(
                    "duplicate HTTP header name: {name}"
                )));
            }
        }
        Ok(())
    }

    fn validate_network(&self, kind: TargetKind) -> Result<(), DomainError> {
        if !self.url.username().is_empty() || self.url.password().is_some() {
            return Err(DomainError::InvalidTarget(format!(
                "{} target endpoint must not include user information",
                kind.as_str()
            )));
        }
        let has_port = self.url.port().is_some();
        if matches!(kind, TargetKind::Tcp | TargetKind::Tls) != has_port {
            return Err(DomainError::InvalidTarget(format!(
                "{} target endpoint must include an explicit port",
                kind.as_str()
            )));
        }
        if !matches!(self.url.path(), "" | "/")
            || self.url.query().is_some()
            || self.url.fragment().is_some()
        {
            return Err(DomainError::InvalidTarget(format!(
                "{} target endpoint must not include a path, query, or fragment",
                kind.as_str()
            )));
        }
        let defaults = Self::get(self.url.clone());
        if self.method != defaults.method
            || self.headers != defaults.headers
            || self.body != defaults.body
            || self.accepted_statuses != defaults.accepted_statuses
            || self.follow_redirects != defaults.follow_redirects
            || self.max_redirects != defaults.max_redirects
            || self.assertions != defaults.assertions
            || self.skip_tls_verification != defaults.skip_tls_verification
            || self.tls_ca_secret != defaults.tls_ca_secret
            || self.tls_client_certificate_secret != defaults.tls_client_certificate_secret
            || self.tls_client_private_key_secret != defaults.tls_client_private_key_secret
        {
            return Err(DomainError::InvalidTarget(format!(
                "{} targets do not accept HTTP request options",
                kind.as_str()
            )));
        }
        Ok(())
    }

    pub(super) fn secret_ids(&self) -> impl Iterator<Item = SecretId> + '_ {
        self.headers
            .values()
            .chain(self.body.iter())
            .filter_map(|value| match value {
                ConfigValue::Literal(_) => None,
                ConfigValue::Secret(id) => Some(*id),
            })
            .chain(self.tls_ca_secret)
            .chain(self.tls_client_certificate_secret)
            .chain(self.tls_client_private_key_secret)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationPolicy {
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub failure_threshold: u32,
}

impl Default for EvaluationPolicy {
    fn default() -> Self {
        Self {
            interval_ms: 60_000,
            timeout_ms: 10_000,
            failure_threshold: 3,
        }
    }
}

impl EvaluationPolicy {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.interval_ms == 0 {
            return Err(DomainError::InvalidTarget(
                "evaluation interval must be greater than zero".to_owned(),
            ));
        }
        if self.timeout_ms == 0 {
            return Err(DomainError::InvalidTarget(
                "evaluation timeout must be greater than zero".to_owned(),
            ));
        }
        if self.failure_threshold == 0 {
            return Err(DomainError::InvalidTarget(
                "failure threshold must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Http,
    Tcp,
    Dns,
    Icmp,
    Tls,
}

impl TargetKind {
    pub fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "http" | "https" => Some(Self::Http),
            "tcp" => Some(Self::Tcp),
            "dns" => Some(Self::Dns),
            "icmp" => Some(Self::Icmp),
            "tls" => Some(Self::Tls),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::Dns => "dns",
            Self::Icmp => "icmp",
            Self::Tls => "tls",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub http: HttpTarget,
    pub policy: EvaluationPolicy,
    pub notification_channels: BTreeSet<NotificationChannelId>,
}

impl Target {
    pub fn kind(&self) -> TargetKind {
        TargetKind::from_scheme(self.http.url.scheme())
            .expect("validated Target must use a supported scheme")
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.name.trim().is_empty() {
            return Err(DomainError::InvalidTarget(
                "target name must not be empty".to_owned(),
            ));
        }
        self.http.validate()?;
        self.policy.validate()
    }
}
