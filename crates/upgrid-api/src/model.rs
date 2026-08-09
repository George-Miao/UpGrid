use super::*;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(super) struct StatusRangeInput {
    pub(super) start: u16,
    pub(super) end: u16,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(untagged)]
pub(super) enum ConfigValueInput {
    Literal(String),
    Secret { secret_id: Uuid },
}

impl From<ConfigValueInput> for ConfigValue {
    fn from(value: ConfigValueInput) -> Self {
        match value {
            ConfigValueInput::Literal(value) => Self::Literal(value),
            ConfigValueInput::Secret { secret_id } => Self::Secret(SecretId(secret_id)),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum ConfigValueView {
    Literal { value: String },
    Secret { secret_id: Uuid },
}

impl From<&ConfigValue> for ConfigValueView {
    fn from(value: &ConfigValue) -> Self {
        match value {
            ConfigValue::Literal(value) => Self::Literal {
                value: value.clone(),
            },
            ConfigValue::Secret(secret_id) => Self::Secret {
                secret_id: secret_id.0,
            },
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct PutTargetRequest {
    pub(super) name: String,
    pub(super) url: String,
    #[serde(default = "default_method")]
    pub(super) method: String,
    #[serde(default)]
    pub(super) headers: BTreeMap<String, ConfigValueInput>,
    #[serde(default)]
    pub(super) body: Option<ConfigValueInput>,
    #[serde(default = "default_statuses")]
    pub(super) accepted_statuses: Vec<StatusRangeInput>,
    #[serde(default = "default_true")]
    pub(super) follow_redirects: bool,
    #[serde(default = "default_redirects")]
    pub(super) max_redirects: u8,
    #[serde(default)]
    pub(super) body_contains: Option<String>,
    #[serde(default)]
    pub(super) skip_tls_verification: bool,
    #[serde(default = "default_interval_seconds")]
    pub(super) interval_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub(super) timeout_seconds: u64,
    #[serde(default = "default_failure_threshold")]
    pub(super) failure_threshold: u32,
    #[serde(default)]
    pub(super) notification_channel_ids: BTreeSet<Uuid>,
    #[serde(default = "default_true")]
    pub(super) use_default_channels: bool,
}

fn default_method() -> String {
    "GET".to_owned()
}

fn default_statuses() -> Vec<StatusRangeInput> {
    vec![StatusRangeInput {
        start: 200,
        end: 299,
    }]
}

fn default_true() -> bool {
    true
}

fn default_redirects() -> u8 {
    5
}

fn default_interval_seconds() -> u64 {
    60
}

fn default_timeout_seconds() -> u64 {
    10
}

fn default_failure_threshold() -> u32 {
    3
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct EvaluationView {
    scheduled_at_ms: u64,
    recorded_at_ms: u64,
    executor_node_id: Uuid,
    succeeded: bool,
    status_code: Option<u16>,
    latency_ms: u64,
    diagnostic: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct TargetView {
    id: Uuid,
    name: String,
    url: String,
    method: String,
    headers: BTreeMap<String, ConfigValueView>,
    body: Option<ConfigValueView>,
    accepted_statuses: Vec<StatusRangeInput>,
    follow_redirects: bool,
    max_redirects: u8,
    body_contains: Option<String>,
    skip_tls_verification: bool,
    interval_seconds: u64,
    timeout_seconds: u64,
    failure_threshold: u32,
    notification_channel_ids: BTreeSet<Uuid>,
    use_default_channels: bool,
    availability: String,
    consecutive_failures: u32,
    latest_evaluation: Option<EvaluationView>,
    history: Vec<EvaluationView>,
    paused: bool,
}

impl TargetView {
    pub(super) fn from_state(application: &ApplicationState, state: &TargetState) -> Self {
        let target = &state.target;
        Self {
            id: target.id.0,
            name: target.name.clone(),
            url: target.http.url.to_string(),
            method: target.http.method.clone(),
            headers: target
                .http
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), ConfigValueView::from(value)))
                .collect(),
            body: target.http.body.as_ref().map(ConfigValueView::from),
            accepted_statuses: target
                .http
                .accepted_statuses
                .iter()
                .map(|range| StatusRangeInput {
                    start: range.start,
                    end: range.end,
                })
                .collect(),
            follow_redirects: target.http.follow_redirects,
            max_redirects: target.http.max_redirects,
            body_contains: target.http.body_contains.clone(),
            skip_tls_verification: target.http.skip_tls_verification,
            interval_seconds: target.policy.interval_ms / 1_000,
            timeout_seconds: target.policy.timeout_ms / 1_000,
            failure_threshold: target.policy.failure_threshold,
            notification_channel_ids: target.notification_channels.iter().map(|id| id.0).collect(),
            use_default_channels: !application
                .default_notifications_disabled
                .contains(&target.id),
            availability: availability_name(state.availability).to_owned(),
            consecutive_failures: state.consecutive_failures,
            latest_evaluation: state.latest_evaluation.as_ref().map(EvaluationView::from),
            history: state
                .history
                .values()
                .rev()
                .take(100)
                .map(EvaluationView::from)
                .collect(),
            paused: state.paused,
        }
    }
}

impl From<&upgrid_raft::domain::Evaluation> for EvaluationView {
    fn from(value: &upgrid_raft::domain::Evaluation) -> Self {
        Self {
            scheduled_at_ms: value.id.scheduled_at_ms,
            recorded_at_ms: value.recorded_at_ms,
            executor_node_id: value.executor_node_id,
            succeeded: value.succeeded,
            status_code: value.http.status_code,
            latency_ms: value.http.latency_ms,
            diagnostic: value.diagnostic.clone(),
        }
    }
}

fn availability_name(value: AvailabilityState) -> &'static str {
    match value {
        AvailabilityState::Unknown => "unknown",
        AvailabilityState::Up => "up",
        AvailabilityState::Down => "down",
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum PutChannelRequest {
    Telegram {
        name: String,
        bot_token: String,
        chat_id: String,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
    Webhook {
        name: String,
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, ConfigValueInput>,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ChannelView {
    id: Uuid,
    name: String,
    kind: String,
    destination: String,
    #[serde(rename = "default")]
    is_default: bool,
}

impl ChannelView {
    pub(super) fn from_channel(channel: &NotificationChannel, is_default: bool) -> Self {
        let (kind, destination) = match &channel.kind {
            NotificationChannelKind::Telegram { chat_id, .. } => ("telegram", chat_id.clone()),
            NotificationChannelKind::Webhook { url, .. } => ("webhook", url.to_string()),
        };
        Self {
            id: channel.id.0,
            name: channel.name.clone(),
            kind: kind.to_owned(),
            destination,
            is_default,
        }
    }
}
