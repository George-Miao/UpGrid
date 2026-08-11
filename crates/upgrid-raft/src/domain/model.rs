use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use super::DomainError;

pub const DEFAULT_HISTORY_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;
pub const DEFAULT_OPERATION_RETENTION_MS: u64 = 10 * 60 * 1_000;
pub const MAX_DIAGNOSTIC_BYTES: usize = 1_024;
pub const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TargetId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SecretId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NotificationChannelId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JoinTokenHash(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigValue {
    Literal(String),
    Secret(SecretId),
}

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
    pub body_contains: Option<String>,
    pub skip_tls_verification: bool,
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
            body_contains: None,
            skip_tls_verification: false,
        }
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if !matches!(self.url.scheme(), "http" | "https") {
            return Err(DomainError::InvalidTarget(
                "target URL must use http or https".to_owned(),
            ));
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

    pub(super) fn secret_ids(&self) -> impl Iterator<Item = SecretId> + '_ {
        self.headers
            .values()
            .chain(self.body.iter())
            .filter_map(|value| match value {
                ConfigValue::Literal(_) => None,
                ConfigValue::Secret(id) => Some(*id),
            })
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub http: HttpTarget,
    pub policy: EvaluationPolicy,
    pub notification_channels: BTreeSet<NotificationChannelId>,
}

impl Target {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Secret {
    pub id: SecretId,
    pub name: String,
    pub ciphertext: Vec<u8>,
}

impl Secret {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.name.trim().is_empty() {
            return Err(DomainError::InvalidSecret(
                "secret name must not be empty".to_owned(),
            ));
        }
        if self.ciphertext.is_empty() {
            return Err(DomainError::InvalidSecret(
                "secret ciphertext must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SmtpSecurity {
    None,
    StartTls,
    Tls,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationChannelKind {
    Telegram {
        bot_token: SecretId,
        chat_id: String,
    },
    Webhook {
        url: Url,
        headers: BTreeMap<String, ConfigValue>,
    },
    Smtp {
        host: String,
        port: u16,
        security: SmtpSecurity,
        username: Option<String>,
        password: Option<SecretId>,
        from: String,
        to: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationChannel {
    pub id: NotificationChannelId,
    pub name: String,
    pub kind: NotificationChannelKind,
}

impl NotificationChannel {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.name.trim().is_empty() {
            return Err(DomainError::InvalidNotificationChannel(
                "notification channel name must not be empty".to_owned(),
            ));
        }
        match &self.kind {
            NotificationChannelKind::Telegram { chat_id, .. } if chat_id.trim().is_empty() => {
                Err(DomainError::InvalidNotificationChannel(
                    "Telegram chat ID must not be empty".to_owned(),
                ))
            }
            NotificationChannelKind::Webhook { url, headers } => {
                if !matches!(url.scheme(), "http" | "https") {
                    return Err(DomainError::InvalidNotificationChannel(
                        "webhook URL must use http or https".to_owned(),
                    ));
                }
                if headers.keys().any(|name| !is_http_token(name)) {
                    return Err(DomainError::InvalidNotificationChannel(
                        "webhook contains an invalid header name".to_owned(),
                    ));
                }
                Ok(())
            }
            NotificationChannelKind::Smtp {
                host,
                port,
                username,
                password,
                from,
                to,
                ..
            } => {
                if host.trim().is_empty() || *port == 0 {
                    return Err(DomainError::InvalidNotificationChannel(
                        "SMTP host and port must be configured".to_owned(),
                    ));
                }
                if from.trim().is_empty() || to.trim().is_empty() {
                    return Err(DomainError::InvalidNotificationChannel(
                        "SMTP sender and recipient must be configured".to_owned(),
                    ));
                }
                if username.is_some() != password.is_some() {
                    return Err(DomainError::InvalidNotificationChannel(
                        "SMTP username and password must be configured together".to_owned(),
                    ));
                }
                Ok(())
            }
            NotificationChannelKind::Telegram { .. } => Ok(()),
        }
    }

    pub(super) fn secret_ids(&self) -> Vec<SecretId> {
        match &self.kind {
            NotificationChannelKind::Telegram { bot_token, .. } => vec![*bot_token],
            NotificationChannelKind::Webhook { headers, .. } => headers
                .values()
                .filter_map(|value| match value {
                    ConfigValue::Literal(_) => None,
                    ConfigValue::Secret(id) => Some(*id),
                })
                .collect(),
            NotificationChannelKind::Smtp { password, .. } => password.iter().copied().collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AvailabilityState {
    Unknown,
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EvaluationId {
    pub target_id: TargetId,
    pub scheduled_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpEvaluationMetadata {
    pub status_code: Option<u16>,
    pub latency_ms: u64,
    pub received_bytes: u64,
    pub final_url: Url,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evaluation {
    pub id: EvaluationId,
    pub recorded_at_ms: u64,
    pub executor_node_id: Uuid,
    pub succeeded: bool,
    pub http: HttpEvaluationMetadata,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationAssignment {
    pub id: EvaluationId,
    pub executor_node_id: Uuid,
    pub assigned_at_ms: u64,
    pub expires_at_ms: u64,
    pub attempt: u32,
}

impl EvaluationAssignment {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.attempt == 0 {
            return Err(DomainError::InvalidEvaluation(
                "evaluation assignment attempt must be greater than zero".to_owned(),
            ));
        }
        if self.expires_at_ms <= self.assigned_at_ms {
            return Err(DomainError::InvalidEvaluation(
                "evaluation assignment must expire after it is assigned".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Evaluation {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self
            .diagnostic
            .as_ref()
            .is_some_and(|value| value.len() > MAX_DIAGNOSTIC_BYTES)
        {
            return Err(DomainError::InvalidEvaluation(format!(
                "diagnostic exceeds {MAX_DIAGNOSTIC_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetState {
    pub target: Target,
    pub availability: AvailabilityState,
    pub consecutive_failures: u32,
    pub latest_evaluation: Option<Evaluation>,
    pub history: BTreeMap<u64, Evaluation>,
    #[serde(default)]
    pub paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTarget {
    pub node_id: Uuid,
    pub name: String,
    pub url: Url,
    pub policy: EvaluationPolicy,
}

impl NodeTarget {
    pub fn id(&self) -> TargetId {
        TargetId(self.node_id)
    }

    pub(super) fn validate(&self) -> Result<(), DomainError> {
        if self.name.trim().is_empty() {
            return Err(DomainError::InvalidTarget(
                "Node Target name must not be empty".to_owned(),
            ));
        }
        if self.url.scheme() != "up" {
            return Err(DomainError::InvalidTarget(
                "Node Target URL must use up".to_owned(),
            ));
        }
        self.policy.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTargetState {
    pub target: NodeTarget,
    pub availability: AvailabilityState,
    pub consecutive_failures: u32,
    pub latest_evaluation: Option<Evaluation>,
    pub history: BTreeMap<u64, Evaluation>,
}

impl NodeTargetState {
    pub(super) fn new(target: NodeTarget) -> Self {
        Self {
            target,
            availability: AvailabilityState::Unknown,
            consecutive_failures: 0,
            latest_evaluation: None,
            history: BTreeMap::new(),
        }
    }
}

impl TargetState {
    pub(super) fn new(target: Target) -> Self {
        Self {
            target,
            availability: AvailabilityState::Unknown,
            consecutive_failures: 0,
            latest_evaluation: None,
            history: BTreeMap::new(),
            paused: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertKind {
    Down,
    Recovered,
}

pub(super) fn is_http_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}
