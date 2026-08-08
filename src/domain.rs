use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

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

    fn validate(&self) -> Result<(), DomainError> {
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

    fn secret_ids(&self) -> impl Iterator<Item = SecretId> + '_ {
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
    fn validate(&self) -> Result<(), DomainError> {
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
    fn validate(&self) -> Result<(), DomainError> {
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
    fn validate(&self) -> Result<(), DomainError> {
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationChannel {
    pub id: NotificationChannelId,
    pub name: String,
    pub kind: NotificationChannelKind,
}

impl NotificationChannel {
    fn validate(&self) -> Result<(), DomainError> {
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
            NotificationChannelKind::Telegram { .. } => Ok(()),
        }
    }

    fn secret_ids(&self) -> Vec<SecretId> {
        match &self.kind {
            NotificationChannelKind::Telegram { bot_token, .. } => vec![*bot_token],
            NotificationChannelKind::Webhook { headers, .. } => headers
                .values()
                .filter_map(|value| match value {
                    ConfigValue::Literal(_) => None,
                    ConfigValue::Secret(id) => Some(*id),
                })
                .collect(),
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
    fn validate(&self) -> Result<(), DomainError> {
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
    fn validate(&self) -> Result<(), DomainError> {
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
}

impl TargetState {
    fn new(target: Target) -> Self {
        Self {
            target,
            availability: AvailabilityState::Unknown,
            consecutive_failures: 0,
            latest_evaluation: None,
            history: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertKind {
    Down,
    Recovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AlertId {
    pub target_id: TargetId,
    pub channel_id: NotificationChannelId,
    pub evaluation_scheduled_at_ms: u64,
    pub kind: AlertKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertDelivery {
    Pending {
        attempts: u32,
        next_attempt_at_ms: u64,
    },
    Delivered {
        delivered_at_ms: u64,
    },
    Failed {
        failed_at_ms: u64,
        diagnostic: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Alert {
    pub id: AlertId,
    pub target_name: String,
    pub target_url: Url,
    pub evaluation: Evaluation,
    pub delivery: AlertDelivery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    PutSecret(Secret),
    PutNotificationChannel(NotificationChannel),
    CreateTarget(Target),
    UpdateTarget(Target),
    DeleteTarget(TargetId),
    RecordEvaluation(Evaluation),
    MarkAlertDelivered {
        alert_id: AlertId,
        delivered_at_ms: u64,
    },
    RecordAlertFailure {
        alert_id: AlertId,
        attempted_at_ms: u64,
        retry_at_ms: Option<u64>,
        diagnostic: String,
    },
    AssignEvaluation(EvaluationAssignment),
    SetHistoryRetention {
        retention_ms: u64,
    },
    PutJoinToken {
        hash: JoinTokenHash,
        expires_at_ms: u64,
    },
    ConsumeJoinToken {
        hash: JoinTokenHash,
        consumed_at_ms: u64,
    },
    AssignEvaluations(Vec<EvaluationAssignment>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResult {
    SecretStored(SecretId),
    NotificationChannelStored(NotificationChannelId),
    TargetCreated(TargetId),
    TargetUpdated(TargetId),
    TargetDeleted(TargetId),
    EvaluationAccepted {
        availability: AvailabilityState,
        alerts: Vec<AlertId>,
    },
    EvaluationDiscarded,
    AlertUpdated(AlertId),
    Noop,
    EvaluationAssigned(EvaluationId),
    HistoryRetentionSet(u64),
    JoinTokenStored,
    JoinTokenConsumed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainError {
    InvalidTarget(String),
    InvalidSecret(String),
    InvalidNotificationChannel(String),
    InvalidEvaluation(String),
    InvalidAlert(String),
    TargetAlreadyExists(TargetId),
    TargetNotFound(TargetId),
    SecretNotFound(SecretId),
    NotificationChannelNotFound(NotificationChannelId),
    AlertNotFound(AlertId),
    InvalidJoinToken,
}

impl Display for DomainError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTarget(message)
            | Self::InvalidSecret(message)
            | Self::InvalidNotificationChannel(message)
            | Self::InvalidEvaluation(message)
            | Self::InvalidAlert(message) => formatter.write_str(message),
            Self::TargetAlreadyExists(id) => write!(formatter, "target already exists: {}", id.0),
            Self::TargetNotFound(id) => write!(formatter, "target not found: {}", id.0),
            Self::SecretNotFound(id) => write!(formatter, "secret not found: {}", id.0),
            Self::NotificationChannelNotFound(id) => {
                write!(formatter, "notification channel not found: {}", id.0)
            }
            Self::AlertNotFound(id) => write!(
                formatter,
                "alert not found for target {} at {}",
                id.target_id.0, id.evaluation_scheduled_at_ms
            ),
            Self::InvalidJoinToken => formatter.write_str("join token is invalid or expired"),
        }
    }
}

impl std::error::Error for DomainError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationState {
    pub targets: BTreeMap<TargetId, TargetState>,
    pub secrets: BTreeMap<SecretId, Secret>,
    pub notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    pub alerts: BTreeMap<AlertId, Alert>,
    pub history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    #[serde(default)]
    pub assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
    #[serde(default)]
    pub join_tokens: BTreeMap<JoinTokenHash, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProcessedOperation {
    submitted_at_ms: u64,
    result: Result<CommandResult, DomainError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LegacyApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    alerts: BTreeMap<AlertId, Alert>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreviousApplicationState {
    targets: BTreeMap<TargetId, TargetState>,
    secrets: BTreeMap<SecretId, Secret>,
    notification_channels: BTreeMap<NotificationChannelId, NotificationChannel>,
    alerts: BTreeMap<AlertId, Alert>,
    history_retention_ms: u64,
    processed_operations: BTreeMap<Uuid, ProcessedOperation>,
    latest_operation_at_ms: u64,
    assignments: BTreeMap<EvaluationId, EvaluationAssignment>,
}

#[cfg(test)]
impl From<ApplicationState> for LegacyApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            alerts: current.alerts,
            history_retention_ms: current.history_retention_ms,
            processed_operations: current.processed_operations,
            latest_operation_at_ms: current.latest_operation_at_ms,
        }
    }
}

impl From<LegacyApplicationState> for ApplicationState {
    fn from(legacy: LegacyApplicationState) -> Self {
        Self {
            targets: legacy.targets,
            secrets: legacy.secrets,
            notification_channels: legacy.notification_channels,
            alerts: legacy.alerts,
            history_retention_ms: legacy.history_retention_ms,
            processed_operations: legacy.processed_operations,
            latest_operation_at_ms: legacy.latest_operation_at_ms,
            assignments: BTreeMap::new(),
            join_tokens: BTreeMap::new(),
        }
    }
}

impl From<PreviousApplicationState> for ApplicationState {
    fn from(previous: PreviousApplicationState) -> Self {
        Self {
            targets: previous.targets,
            secrets: previous.secrets,
            notification_channels: previous.notification_channels,
            alerts: previous.alerts,
            history_retention_ms: previous.history_retention_ms,
            processed_operations: previous.processed_operations,
            latest_operation_at_ms: previous.latest_operation_at_ms,
            assignments: previous.assignments,
            join_tokens: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
impl From<ApplicationState> for PreviousApplicationState {
    fn from(current: ApplicationState) -> Self {
        Self {
            targets: current.targets,
            secrets: current.secrets,
            notification_channels: current.notification_channels,
            alerts: current.alerts,
            history_retention_ms: current.history_retention_ms,
            processed_operations: current.processed_operations,
            latest_operation_at_ms: current.latest_operation_at_ms,
            assignments: current.assignments,
        }
    }
}

impl Default for ApplicationState {
    fn default() -> Self {
        Self {
            targets: BTreeMap::new(),
            secrets: BTreeMap::new(),
            notification_channels: BTreeMap::new(),
            alerts: BTreeMap::new(),
            assignments: BTreeMap::new(),
            join_tokens: BTreeMap::new(),
            history_retention_ms: DEFAULT_HISTORY_RETENTION_MS,
            processed_operations: BTreeMap::new(),
            latest_operation_at_ms: 0,
        }
    }
}

impl ApplicationState {
    pub fn apply_operation(
        &mut self,
        operation_id: Uuid,
        submitted_at_ms: u64,
        command: Command,
    ) -> Result<CommandResult, DomainError> {
        if let Some(processed) = self.processed_operations.get(&operation_id) {
            return processed.result.clone();
        }

        let result = self.apply(command);
        self.latest_operation_at_ms = self.latest_operation_at_ms.max(submitted_at_ms);
        let cutoff = self
            .latest_operation_at_ms
            .saturating_sub(DEFAULT_OPERATION_RETENTION_MS);
        self.processed_operations
            .retain(|_, item| item.submitted_at_ms >= cutoff);
        self.processed_operations.insert(
            operation_id,
            ProcessedOperation {
                submitted_at_ms,
                result: result.clone(),
            },
        );
        result
    }

    pub fn apply(&mut self, command: Command) -> Result<CommandResult, DomainError> {
        match command {
            Command::PutSecret(secret) => self.put_secret(secret),
            Command::PutNotificationChannel(channel) => self.put_notification_channel(channel),
            Command::CreateTarget(target) => self.create_target(target),
            Command::UpdateTarget(target) => self.update_target(target),
            Command::DeleteTarget(target_id) => {
                self.targets
                    .remove(&target_id)
                    .ok_or(DomainError::TargetNotFound(target_id))?;
                self.assignments
                    .retain(|evaluation_id, _| evaluation_id.target_id != target_id);
                Ok(CommandResult::TargetDeleted(target_id))
            }
            Command::AssignEvaluation(assignment) => self.assign_evaluation(assignment),
            Command::AssignEvaluations(assignments) => self.assign_evaluations(assignments),
            Command::SetHistoryRetention { retention_ms } => {
                if retention_ms == 0 {
                    return Err(DomainError::InvalidEvaluation(
                        "history retention must be greater than zero".to_owned(),
                    ));
                }
                self.history_retention_ms = retention_ms;
                Ok(CommandResult::HistoryRetentionSet(retention_ms))
            }
            Command::PutJoinToken {
                hash,
                expires_at_ms,
            } => {
                if expires_at_ms == 0 {
                    return Err(DomainError::InvalidJoinToken);
                }
                self.join_tokens.insert(hash, expires_at_ms);
                Ok(CommandResult::JoinTokenStored)
            }
            Command::ConsumeJoinToken {
                hash,
                consumed_at_ms,
            } => match self.join_tokens.remove(&hash) {
                Some(expires_at_ms) if consumed_at_ms <= expires_at_ms => {
                    Ok(CommandResult::JoinTokenConsumed)
                }
                _ => Err(DomainError::InvalidJoinToken),
            },
            Command::RecordEvaluation(evaluation) => self.record_evaluation(evaluation),
            Command::MarkAlertDelivered {
                alert_id,
                delivered_at_ms,
            } => {
                let alert = self
                    .alerts
                    .get_mut(&alert_id)
                    .ok_or(DomainError::AlertNotFound(alert_id))?;
                if !matches!(&alert.delivery, AlertDelivery::Delivered { .. }) {
                    alert.delivery = AlertDelivery::Delivered { delivered_at_ms };
                }
                Ok(CommandResult::AlertUpdated(alert_id))
            }
            Command::RecordAlertFailure {
                alert_id,
                attempted_at_ms,
                retry_at_ms,
                diagnostic,
            } => self.record_alert_failure(alert_id, attempted_at_ms, retry_at_ms, diagnostic),
        }
    }

    fn put_secret(&mut self, secret: Secret) -> Result<CommandResult, DomainError> {
        secret.validate()?;
        let id = secret.id;
        self.secrets.insert(id, secret);
        Ok(CommandResult::SecretStored(id))
    }

    fn put_notification_channel(
        &mut self,
        channel: NotificationChannel,
    ) -> Result<CommandResult, DomainError> {
        channel.validate()?;
        for secret_id in channel.secret_ids() {
            if !self.secrets.contains_key(&secret_id) {
                return Err(DomainError::SecretNotFound(secret_id));
            }
        }
        let id = channel.id;
        self.notification_channels.insert(id, channel);
        Ok(CommandResult::NotificationChannelStored(id))
    }

    fn create_target(&mut self, target: Target) -> Result<CommandResult, DomainError> {
        target.validate()?;
        if self.targets.contains_key(&target.id) {
            return Err(DomainError::TargetAlreadyExists(target.id));
        }
        self.validate_target_references(&target)?;
        let id = target.id;
        self.targets.insert(id, TargetState::new(target));
        Ok(CommandResult::TargetCreated(id))
    }

    fn update_target(&mut self, target: Target) -> Result<CommandResult, DomainError> {
        target.validate()?;
        self.validate_target_references(&target)?;
        let target_state = self
            .targets
            .get_mut(&target.id)
            .ok_or(DomainError::TargetNotFound(target.id))?;
        let id = target.id;
        target_state.target = target;
        Ok(CommandResult::TargetUpdated(id))
    }

    fn validate_target_references(&self, target: &Target) -> Result<(), DomainError> {
        for channel_id in &target.notification_channels {
            if !self.notification_channels.contains_key(channel_id) {
                return Err(DomainError::NotificationChannelNotFound(*channel_id));
            }
        }
        for secret_id in target.http.secret_ids() {
            if !self.secrets.contains_key(&secret_id) {
                return Err(DomainError::SecretNotFound(secret_id));
            }
        }
        Ok(())
    }

    fn assign_evaluation(
        &mut self,
        assignment: EvaluationAssignment,
    ) -> Result<CommandResult, DomainError> {
        assignment.validate()?;
        let Some(target) = self.targets.get(&assignment.id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if target
            .latest_evaluation
            .as_ref()
            .is_some_and(|latest| latest.id.scheduled_at_ms >= assignment.id.scheduled_at_ms)
            || target.history.contains_key(&assignment.id.scheduled_at_ms)
            || self
                .assignments
                .get(&assignment.id)
                .is_some_and(|current| current.attempt >= assignment.attempt)
            || self
                .assignments
                .keys()
                .any(|id| id.target_id == assignment.id.target_id && *id != assignment.id)
        {
            return Ok(CommandResult::EvaluationDiscarded);
        }
        let id = assignment.id;
        self.assignments.insert(id, assignment);
        Ok(CommandResult::EvaluationAssigned(id))
    }

    fn assign_evaluations(
        &mut self,
        assignments: Vec<EvaluationAssignment>,
    ) -> Result<CommandResult, DomainError> {
        for assignment in &assignments {
            assignment.validate()?;
        }
        for assignment in assignments {
            self.assign_evaluation(assignment)?;
        }
        Ok(CommandResult::Noop)
    }

    fn record_evaluation(&mut self, evaluation: Evaluation) -> Result<CommandResult, DomainError> {
        evaluation.validate()?;
        self.assignments.remove(&evaluation.id);
        let Some(target_state) = self.targets.get_mut(&evaluation.id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if target_state
            .history
            .contains_key(&evaluation.id.scheduled_at_ms)
            || target_state
                .latest_evaluation
                .as_ref()
                .is_some_and(|latest| latest.id.scheduled_at_ms >= evaluation.id.scheduled_at_ms)
        {
            return Ok(CommandResult::EvaluationDiscarded);
        }

        let previous_availability = target_state.availability;
        if evaluation.succeeded {
            target_state.consecutive_failures = 0;
            target_state.availability = AvailabilityState::Up;
        } else {
            target_state.consecutive_failures = target_state.consecutive_failures.saturating_add(1);
            if target_state.consecutive_failures >= target_state.target.policy.failure_threshold {
                target_state.availability = AvailabilityState::Down;
            }
        }

        let transition = match (previous_availability, target_state.availability) {
            (AvailabilityState::Down, AvailabilityState::Up) => Some(AlertKind::Recovered),
            (previous, AvailabilityState::Down) if previous != AvailabilityState::Down => {
                Some(AlertKind::Down)
            }
            _ => None,
        };

        let channel_ids = target_state
            .target
            .notification_channels
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let target_name = target_state.target.name.clone();
        let target_url = target_state.target.http.url.clone();
        target_state.latest_evaluation = Some(evaluation.clone());
        target_state
            .history
            .insert(evaluation.id.scheduled_at_ms, evaluation.clone());
        let cutoff = evaluation
            .recorded_at_ms
            .saturating_sub(self.history_retention_ms);
        target_state
            .history
            .retain(|_, item| item.recorded_at_ms >= cutoff);
        let availability = target_state.availability;

        let mut alert_ids = Vec::new();
        if let Some(kind) = transition {
            for channel_id in channel_ids {
                let id = AlertId {
                    target_id: evaluation.id.target_id,
                    channel_id,
                    evaluation_scheduled_at_ms: evaluation.id.scheduled_at_ms,
                    kind,
                };
                self.alerts.entry(id).or_insert_with(|| Alert {
                    id,
                    target_name: target_name.clone(),
                    target_url: target_url.clone(),
                    evaluation: evaluation.clone(),
                    delivery: AlertDelivery::Pending {
                        attempts: 0,
                        next_attempt_at_ms: evaluation.recorded_at_ms,
                    },
                });
                alert_ids.push(id);
            }
        }

        Ok(CommandResult::EvaluationAccepted {
            availability,
            alerts: alert_ids,
        })
    }

    fn record_alert_failure(
        &mut self,
        alert_id: AlertId,
        attempted_at_ms: u64,
        retry_at_ms: Option<u64>,
        diagnostic: String,
    ) -> Result<CommandResult, DomainError> {
        if diagnostic.len() > MAX_DIAGNOSTIC_BYTES {
            return Err(DomainError::InvalidAlert(format!(
                "diagnostic exceeds {MAX_DIAGNOSTIC_BYTES} bytes"
            )));
        }
        let alert = self
            .alerts
            .get_mut(&alert_id)
            .ok_or(DomainError::AlertNotFound(alert_id))?;
        if matches!(&alert.delivery, AlertDelivery::Delivered { .. }) {
            return Ok(CommandResult::AlertUpdated(alert_id));
        }
        let attempts = match &alert.delivery {
            AlertDelivery::Pending { attempts, .. } => (*attempts).saturating_add(1),
            AlertDelivery::Delivered { .. } | AlertDelivery::Failed { .. } => 1,
        };
        alert.delivery = match retry_at_ms {
            Some(next_attempt_at_ms) => AlertDelivery::Pending {
                attempts,
                next_attempt_at_ms,
            },
            None => AlertDelivery::Failed {
                failed_at_ms: attempted_at_ms,
                diagnostic,
            },
        };
        Ok(CommandResult::AlertUpdated(alert_id))
    }
}

fn is_http_token(value: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn target(target_id: TargetId, channel_id: NotificationChannelId) -> Target {
        Target {
            id: target_id,
            name: "Example".to_owned(),
            http: HttpTarget::get(Url::parse("https://example.com/health").unwrap()),
            policy: EvaluationPolicy::default(),
            notification_channels: BTreeSet::from([channel_id]),
        }
    }

    fn evaluation(target_id: TargetId, scheduled_at_ms: u64, succeeded: bool) -> Evaluation {
        Evaluation {
            id: EvaluationId {
                target_id,
                scheduled_at_ms,
            },
            recorded_at_ms: scheduled_at_ms + 50,
            executor_node_id: id(99),
            succeeded,
            http: HttpEvaluationMetadata {
                status_code: succeeded.then_some(200),
                latency_ms: 50,
                received_bytes: 2,
                final_url: Url::parse("https://example.com/health").unwrap(),
            },
            diagnostic: (!succeeded).then(|| "connection refused".to_owned()),
        }
    }

    fn state_with_target() -> (ApplicationState, TargetId, NotificationChannelId) {
        let mut state = ApplicationState::default();
        let secret_id = SecretId(id(1));
        let channel_id = NotificationChannelId(id(2));
        let target_id = TargetId(id(3));
        state
            .apply(Command::PutSecret(Secret {
                id: secret_id,
                name: "telegram-token".to_owned(),
                ciphertext: vec![1, 2, 3],
            }))
            .unwrap();
        state
            .apply(Command::PutNotificationChannel(NotificationChannel {
                id: channel_id,
                name: "Operations".to_owned(),
                kind: NotificationChannelKind::Telegram {
                    bot_token: secret_id,
                    chat_id: "1234".to_owned(),
                },
            }))
            .unwrap();
        state
            .apply(Command::CreateTarget(target(target_id, channel_id)))
            .unwrap();
        (state, target_id, channel_id)
    }

    #[test]
    fn availability_transitions_create_one_alert_per_channel() {
        let (mut state, target_id, channel_id) = state_with_target();

        for scheduled_at_ms in [1_000, 2_000] {
            let result = state
                .apply(Command::RecordEvaluation(evaluation(
                    target_id,
                    scheduled_at_ms,
                    false,
                )))
                .unwrap();
            assert_eq!(
                result,
                CommandResult::EvaluationAccepted {
                    availability: AvailabilityState::Unknown,
                    alerts: vec![],
                }
            );
        }

        let result = state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 3_000, false,
            )))
            .unwrap();
        let down_alert = AlertId {
            target_id,
            channel_id,
            evaluation_scheduled_at_ms: 3_000,
            kind: AlertKind::Down,
        };
        assert_eq!(
            result,
            CommandResult::EvaluationAccepted {
                availability: AvailabilityState::Down,
                alerts: vec![down_alert],
            }
        );

        let result = state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 4_000, true,
            )))
            .unwrap();
        let recovered_alert = AlertId {
            target_id,
            channel_id,
            evaluation_scheduled_at_ms: 4_000,
            kind: AlertKind::Recovered,
        };
        assert_eq!(
            result,
            CommandResult::EvaluationAccepted {
                availability: AvailabilityState::Up,
                alerts: vec![recovered_alert],
            }
        );
        assert_eq!(state.alerts.len(), 2);
    }

    #[test]
    fn duplicate_and_deleted_target_results_are_discarded() {
        let (mut state, target_id, _) = state_with_target();
        let item = evaluation(target_id, 1_000, true);
        state
            .apply(Command::RecordEvaluation(item.clone()))
            .unwrap();

        assert_eq!(
            state.apply(Command::RecordEvaluation(item)).unwrap(),
            CommandResult::EvaluationDiscarded
        );
        assert_eq!(state.targets[&target_id].history.len(), 1);

        state.apply(Command::DeleteTarget(target_id)).unwrap();
        assert_eq!(
            state
                .apply(Command::RecordEvaluation(evaluation(
                    target_id, 2_000, false,
                )))
                .unwrap(),
            CommandResult::EvaluationDiscarded
        );
    }

    #[test]
    fn evaluation_history_is_pruned_by_recorded_time() {
        let (mut state, target_id, _) = state_with_target();
        state.history_retention_ms = 1_000;
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 1_000, true,
            )))
            .unwrap();
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 3_000, true,
            )))
            .unwrap();

        let history = &state.targets[&target_id].history;
        assert_eq!(history.len(), 1);
        assert!(history.contains_key(&3_000));
    }

    #[test]
    fn targets_cannot_reference_missing_channels_or_secrets() {
        let mut state = ApplicationState::default();
        let channel_id = NotificationChannelId(id(1));
        let target_id = TargetId(id(2));
        assert_eq!(
            state
                .apply(Command::CreateTarget(target(target_id, channel_id)))
                .unwrap_err(),
            DomainError::NotificationChannelNotFound(channel_id)
        );

        let secret_id = SecretId(id(3));
        let channel = NotificationChannel {
            id: channel_id,
            name: "Webhook".to_owned(),
            kind: NotificationChannelKind::Webhook {
                url: Url::parse("https://example.com/hook").unwrap(),
                headers: BTreeMap::from([(
                    "Authorization".to_owned(),
                    ConfigValue::Secret(secret_id),
                )]),
            },
        };
        assert_eq!(
            state
                .apply(Command::PutNotificationChannel(channel))
                .unwrap_err(),
            DomainError::SecretNotFound(secret_id)
        );
    }

    #[test]
    fn operations_are_deduplicated_without_client_keys() {
        let (mut state, target_id, _) = state_with_target();
        let operation_id = id(500);
        let command = Command::RecordEvaluation(evaluation(target_id, 1_000, false));

        let first = state
            .apply_operation(operation_id, 1_050, command.clone())
            .unwrap();
        let repeated = state.apply_operation(operation_id, 1_050, command).unwrap();

        assert_eq!(first, repeated);
        assert_eq!(state.targets[&target_id].consecutive_failures, 1);
        let reused = state
            .apply_operation(
                operation_id,
                2_050,
                Command::RecordEvaluation(evaluation(target_id, 2_000, false)),
            )
            .unwrap();
        assert_eq!(first, reused);
        assert_eq!(state.targets[&target_id].consecutive_failures, 1);
    }

    #[test]
    fn assignment_variants_preserve_existing_postcard_discriminants() {
        #[allow(dead_code)]
        #[derive(Serialize)]
        enum LegacyCommand {
            PutSecret(Secret),
            PutNotificationChannel(NotificationChannel),
            CreateTarget(Target),
            UpdateTarget(Target),
            DeleteTarget(TargetId),
        }

        let target_id = TargetId(id(77));
        let encoded = postcard::to_stdvec(&LegacyCommand::DeleteTarget(target_id)).unwrap();
        let decoded = postcard::from_bytes::<Command>(&encoded).unwrap();

        assert_eq!(decoded, Command::DeleteTarget(target_id));
    }

    #[test]
    fn out_of_order_results_do_not_roll_state_back() {
        let (mut state, target_id, _) = state_with_target();
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 2_000, true,
            )))
            .unwrap();

        assert_eq!(
            state
                .apply(Command::RecordEvaluation(evaluation(
                    target_id, 1_000, false,
                )))
                .unwrap(),
            CommandResult::EvaluationDiscarded
        );
        assert_eq!(
            state.targets[&target_id].availability,
            AvailabilityState::Up
        );
    }

    #[test]
    fn committed_result_completes_the_replicated_assignment() {
        let (mut state, target_id, _) = state_with_target();
        let evaluation_id = EvaluationId {
            target_id,
            scheduled_at_ms: 1_000,
        };
        let assignment = EvaluationAssignment {
            id: evaluation_id,
            executor_node_id: id(10),
            assigned_at_ms: 900,
            expires_at_ms: 2_000,
            attempt: 1,
        };

        assert_eq!(
            state.apply(Command::AssignEvaluation(assignment)).unwrap(),
            CommandResult::EvaluationAssigned(evaluation_id)
        );
        assert!(state.assignments.contains_key(&evaluation_id));

        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 1_000, true,
            )))
            .unwrap();
        assert!(!state.assignments.contains_key(&evaluation_id));
    }

    #[test]
    fn assignment_batch_applies_every_valid_assignment() {
        let (mut state, first_target_id, channel_id) = state_with_target();
        let second_target_id = TargetId(id(20));
        state
            .apply(Command::CreateTarget(target(second_target_id, channel_id)))
            .unwrap();
        let assignments = [first_target_id, second_target_id]
            .into_iter()
            .map(|target_id| EvaluationAssignment {
                id: EvaluationId {
                    target_id,
                    scheduled_at_ms: 1_000,
                },
                executor_node_id: id(10),
                assigned_at_ms: 900,
                expires_at_ms: 2_000,
                attempt: 1,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            state
                .apply(Command::AssignEvaluations(assignments.clone()))
                .unwrap(),
            CommandResult::Noop
        );
        assert!(
            assignments
                .iter()
                .all(|assignment| state.assignments.contains_key(&assignment.id))
        );
    }

    #[test]
    fn updating_a_target_preserves_runtime_state() {
        let (mut state, target_id, channel_id) = state_with_target();
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 1_000, true,
            )))
            .unwrap();
        let mut updated = target(target_id, channel_id);
        updated.name = "Renamed".to_owned();

        state.apply(Command::UpdateTarget(updated)).unwrap();

        let target_state = &state.targets[&target_id];
        assert_eq!(target_state.target.name, "Renamed");
        assert_eq!(target_state.availability, AvailabilityState::Up);
        assert_eq!(target_state.history.len(), 1);
    }

    #[test]
    fn delivered_alerts_cannot_regress_to_pending() {
        let (mut state, target_id, channel_id) = state_with_target();
        for scheduled_at_ms in [1_000, 2_000, 3_000] {
            state
                .apply(Command::RecordEvaluation(evaluation(
                    target_id,
                    scheduled_at_ms,
                    false,
                )))
                .unwrap();
        }
        let alert_id = AlertId {
            target_id,
            channel_id,
            evaluation_scheduled_at_ms: 3_000,
            kind: AlertKind::Down,
        };
        state
            .apply(Command::MarkAlertDelivered {
                alert_id,
                delivered_at_ms: 3_100,
            })
            .unwrap();
        state
            .apply(Command::RecordAlertFailure {
                alert_id,
                attempted_at_ms: 3_200,
                retry_at_ms: Some(4_000),
                diagnostic: "late failure".to_owned(),
            })
            .unwrap();

        assert_eq!(
            state.alerts[&alert_id].delivery,
            AlertDelivery::Delivered {
                delivered_at_ms: 3_100
            }
        );
    }

    #[test]
    fn history_retention_is_replicated_configuration() {
        let mut state = ApplicationState::default();

        assert_eq!(
            state
                .apply(Command::SetHistoryRetention {
                    retention_ms: 6 * 60 * 60 * 1_000,
                })
                .unwrap(),
            CommandResult::HistoryRetentionSet(6 * 60 * 60 * 1_000)
        );
        assert_eq!(state.history_retention_ms, 6 * 60 * 60 * 1_000);
        assert!(
            state
                .apply(Command::SetHistoryRetention { retention_ms: 0 })
                .is_err()
        );
    }

    #[test]
    fn join_token_is_single_use_and_expires() {
        let mut state = ApplicationState::default();
        let hash = JoinTokenHash([7; 32]);
        state
            .apply(Command::PutJoinToken {
                hash,
                expires_at_ms: 2_000,
            })
            .unwrap();

        assert_eq!(
            state
                .apply(Command::ConsumeJoinToken {
                    hash,
                    consumed_at_ms: 1_000,
                })
                .unwrap(),
            CommandResult::JoinTokenConsumed
        );
        assert!(
            state
                .apply(Command::ConsumeJoinToken {
                    hash,
                    consumed_at_ms: 1_001,
                })
                .is_err()
        );

        state
            .apply(Command::PutJoinToken {
                hash,
                expires_at_ms: 2_000,
            })
            .unwrap();
        assert!(
            state
                .apply(Command::ConsumeJoinToken {
                    hash,
                    consumed_at_ms: 2_001,
                })
                .is_err()
        );
    }
}
