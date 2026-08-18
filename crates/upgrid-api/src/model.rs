use super::*;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(super) struct StatusRangeInput {
    pub(super) start: u16,
    pub(super) end: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum HttpAssertionModel {
    BodyContains {
        value: String,
    },
    BodyRegex {
        pattern: String,
    },
    JsonPath {
        path: String,
        #[serde(default)]
        expected: Option<String>,
    },
    ResponseHeader {
        name: String,
        #[serde(default)]
        value: Option<String>,
    },
    Latency {
        max_ms: u64,
    },
    Script {
        source: String,
    },
}

impl From<HttpAssertionModel> for HttpAssertion {
    fn from(value: HttpAssertionModel) -> Self {
        match value {
            HttpAssertionModel::BodyContains { value } => Self::BodyContains { value },
            HttpAssertionModel::BodyRegex { pattern } => Self::BodyRegex { pattern },
            HttpAssertionModel::JsonPath { path, expected } => Self::JsonPath { path, expected },
            HttpAssertionModel::ResponseHeader { name, value } => {
                Self::ResponseHeader { name, value }
            }
            HttpAssertionModel::Latency { max_ms } => Self::Latency { max_ms },
            HttpAssertionModel::Script { source } => Self::Script { source },
        }
    }
}

impl From<&HttpAssertion> for HttpAssertionModel {
    fn from(value: &HttpAssertion) -> Self {
        match value {
            HttpAssertion::BodyContains { value } => Self::BodyContains {
                value: value.clone(),
            },
            HttpAssertion::BodyRegex { pattern } => Self::BodyRegex {
                pattern: pattern.clone(),
            },
            HttpAssertion::JsonPath { path, expected } => Self::JsonPath {
                path: path.clone(),
                expected: expected.clone(),
            },
            HttpAssertion::ResponseHeader { name, value } => Self::ResponseHeader {
                name: name.clone(),
                value: value.clone(),
            },
            HttpAssertion::Latency { max_ms } => Self::Latency { max_ms: *max_ms },
            HttpAssertion::Script { source } => Self::Script {
                source: source.clone(),
            },
        }
    }
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

#[derive(Debug, Clone, Copy, Default, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum TargetKindInput {
    #[default]
    Http,
    Tcp,
    Dns,
    Icmp,
    Tls,
}

impl From<TargetKindInput> for TargetKind {
    fn from(value: TargetKindInput) -> Self {
        match value {
            TargetKindInput::Http => Self::Http,
            TargetKindInput::Tcp => Self::Tcp,
            TargetKindInput::Dns => Self::Dns,
            TargetKindInput::Icmp => Self::Icmp,
            TargetKindInput::Tls => Self::Tls,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct PutTargetRequest {
    pub(super) name: String,
    #[serde(default)]
    pub(super) kind: TargetKindInput,
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
    pub(super) assertions: Vec<HttpAssertionModel>,
    #[serde(default)]
    pub(super) skip_tls_verification: bool,
    #[serde(default)]
    pub(super) tls_ca_secret_id: Option<Uuid>,
    #[serde(default)]
    pub(super) tls_client_certificate_secret_id: Option<Uuid>,
    #[serde(default)]
    pub(super) tls_client_private_key_secret_id: Option<Uuid>,
    #[serde(default = "default_interval_seconds")]
    pub(super) interval_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub(super) timeout_seconds: u64,
    #[serde(default = "default_failure_threshold")]
    pub(super) failure_threshold: u32,
    #[serde(default = "default_location_count")]
    #[schema(minimum = 1, maximum = 32)]
    pub(super) locations: u16,
    #[serde(default)]
    pub(super) notification_channel_ids: BTreeSet<Uuid>,
    #[serde(default = "default_true")]
    pub(super) use_default_channels: bool,
}

fn default_location_count() -> u16 {
    1
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

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum TargetKindView {
    Http,
    Tcp,
    Dns,
    Icmp,
    Tls,
    Node,
}

impl From<TargetKind> for TargetKindView {
    fn from(value: TargetKind) -> Self {
        match value {
            TargetKind::Http => Self::Http,
            TargetKind::Tcp => Self::Tcp,
            TargetKind::Dns => Self::Dns,
            TargetKind::Icmp => Self::Icmp,
            TargetKind::Tls => Self::Tls,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct TargetView {
    id: Uuid,
    kind: TargetKindView,
    name: String,
    url: String,
    method: String,
    headers: BTreeMap<String, ConfigValueView>,
    body: Option<ConfigValueView>,
    accepted_statuses: Vec<StatusRangeInput>,
    follow_redirects: bool,
    max_redirects: u8,
    assertions: Vec<HttpAssertionModel>,
    skip_tls_verification: bool,
    tls_ca_secret_id: Option<Uuid>,
    tls_client_certificate_secret_id: Option<Uuid>,
    tls_client_private_key_secret_id: Option<Uuid>,
    interval_seconds: u64,
    timeout_seconds: u64,
    failure_threshold: u32,
    #[schema(minimum = 1, maximum = 32)]
    locations: u16,
    notification_channel_ids: BTreeSet<Uuid>,
    use_default_channels: bool,
    availability: String,
    consecutive_failures: u32,
    latest_evaluation: Option<EvaluationView>,
    history: Vec<EvaluationView>,
    paused: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct TrashedTargetView {
    #[serde(flatten)]
    #[schema(inline)]
    target: TargetView,
    deleted_at_ms: u64,
    purge_at_ms: u64,
}

impl TrashedTargetView {
    pub(super) fn from_trashed(target: &TrashedTarget, retention_ms: u64) -> Self {
        Self {
            target: TargetView::from_target_state(
                &target.state,
                target.locations,
                target.use_default_notifications,
            ),
            deleted_at_ms: target.deleted_at_ms,
            purge_at_ms: target.purge_at_ms(retention_ms),
        }
    }
}

impl TargetView {
    pub(super) fn from_state(application: &ApplicationState, state: &TargetState) -> Self {
        Self::from_target_state(
            state,
            application.target_location_count(state.target.id),
            !application
                .default_notifications_disabled
                .contains(&state.target.id),
        )
    }

    fn from_target_state(state: &TargetState, locations: u16, use_default_channels: bool) -> Self {
        let target = &state.target;
        Self {
            id: target.id.0,
            kind: target.kind().into(),
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
            assertions: target
                .http
                .assertions
                .iter()
                .map(HttpAssertionModel::from)
                .collect(),
            skip_tls_verification: target.http.skip_tls_verification,
            tls_ca_secret_id: target.http.tls_ca_secret.map(|id| id.0),
            tls_client_certificate_secret_id: target
                .http
                .tls_client_certificate_secret
                .map(|id| id.0),
            tls_client_private_key_secret_id: target
                .http
                .tls_client_private_key_secret
                .map(|id| id.0),
            interval_seconds: target.policy.interval_ms / 1_000,
            timeout_seconds: target.policy.timeout_ms / 1_000,
            failure_threshold: target.policy.failure_threshold,
            locations,
            notification_channel_ids: target.notification_channels.iter().map(|id| id.0).collect(),
            use_default_channels,
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

    pub(super) fn from_node(state: &NodeTargetState) -> Self {
        let target = &state.target;
        Self {
            id: target.id().0,
            kind: TargetKindView::Node,
            name: target.name.clone(),
            url: target.url.to_string(),
            method: "RPC".to_owned(),
            headers: BTreeMap::new(),
            body: None,
            accepted_statuses: Vec::new(),
            follow_redirects: false,
            max_redirects: 0,
            assertions: Vec::new(),
            skip_tls_verification: false,
            tls_ca_secret_id: None,
            tls_client_certificate_secret_id: None,
            tls_client_private_key_secret_id: None,
            interval_seconds: target.policy.interval_ms / 1_000,
            timeout_seconds: target.policy.timeout_ms / 1_000,
            failure_threshold: target.policy.failure_threshold,
            locations: 1,
            notification_channel_ids: BTreeSet::new(),
            use_default_channels: true,
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
            paused: false,
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

pub(super) fn availability_name(value: AvailabilityState) -> &'static str {
    match value {
        AvailabilityState::Unknown => "unknown",
        AvailabilityState::Up => "up",
        AvailabilityState::Down => "down",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::Value;
    use upgrid_raft::domain::{
        ApplicationState, EvaluationPolicy, HttpTarget, Target, TargetId, TargetState,
    };

    use super::*;

    #[test]
    fn target_view_reports_network_target_kind() {
        let state = ApplicationState::default();
        let target = Target {
            id: TargetId(Uuid::from_u128(1)),
            name: "Database".to_owned(),
            http: HttpTarget::get(Url::parse("tcp://database.internal:5432").unwrap()),
            policy: EvaluationPolicy::default(),
            notification_channels: BTreeSet::new(),
        };
        let view = TargetView::from_state(
            &state,
            &TargetState {
                target,
                availability: AvailabilityState::Unknown,
                consecutive_failures: 0,
                latest_evaluation: None,
                history: BTreeMap::new(),
                paused: false,
            },
        );

        let json = serde_json::to_value(view).unwrap();
        assert_eq!(json["kind"], Value::String("tcp".to_owned()));
        assert_eq!(
            json["url"],
            Value::String("tcp://database.internal:5432".to_owned())
        );
    }
}
