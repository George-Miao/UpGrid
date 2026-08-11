#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    PutSecret(Secret),
    CreateNotificationChannel {
        channel: NotificationChannel,
        generated_secret: Option<Secret>,
        is_default: bool,
    },
    CreateTarget {
        target: Target,
        use_default_notifications: bool,
    },
    UpdateTarget {
        target: Target,
        use_default_notifications: bool,
    },
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
    AuthorizeJoinToken {
        hash: JoinTokenHash,
        authorized_at_ms: u64,
    },
    AssignEvaluations(Vec<EvaluationAssignment>),
    SetTargetPaused {
        target_id: TargetId,
        paused: bool,
    },
    DeleteSecret(SecretId),
    DeleteNotificationChannel(NotificationChannelId),
    RevokeJoinToken(JoinTokenHash),
    PutLimitedJoinToken {
        hash: JoinTokenHash,
        expires_at_ms: u64,
        uses: u64,
    },
    SetNodeName {
        node_id: Uuid,
        name: String,
    },
    SetNotificationChannelDefault {
        channel_id: NotificationChannelId,
        is_default: bool,
    },
    SyncNodeTargets(Vec<NodeTarget>),
    RecordNodeEvaluation(Evaluation),
    UpdateNotificationChannel {
        channel: NotificationChannel,
        generated_secret: Option<Secret>,
        is_default: bool,
    },
    CreateIdentity(OperatorIdentity),
    UpdateIdentity(OperatorIdentity),
    DeleteIdentity(IdentityId),
    CreateApiToken(ApiToken),
    RevokeApiToken(ApiTokenId),
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
    JoinTokenAuthorized,
    TargetPauseSet {
        target_id: TargetId,
        paused: bool,
    },
    SecretDeleted(SecretId),
    NotificationChannelDeleted(NotificationChannelId),
    JoinTokenRevoked,
    NodeNameSet(Uuid),
    NotificationChannelDefaultSet(NotificationChannelId),
    NodeTargetsSynced,
    NodeEvaluationAccepted {
        availability: AvailabilityState,
        alerts: Vec<AlertId>,
    },
    NotificationChannelUpdated(NotificationChannelId),
    IdentityCreated(IdentityId),
    IdentityUpdated(IdentityId),
    IdentityDeleted(IdentityId),
    ApiTokenCreated(ApiTokenId),
    ApiTokenRevoked(ApiTokenId),
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
    InvalidNodeName(String),
    InvalidIdentity(String),
    IdentityAlreadyExists(IdentityId),
    IdentityNotFound(IdentityId),
    InvalidApiToken(String),
    ApiTokenAlreadyExists(ApiTokenId),
    ApiTokenNotFound(ApiTokenId),
}

impl Display for DomainError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTarget(message)
            | Self::InvalidSecret(message)
            | Self::InvalidNotificationChannel(message)
            | Self::InvalidEvaluation(message)
            | Self::InvalidAlert(message)
            | Self::InvalidIdentity(message)
            | Self::InvalidApiToken(message) => formatter.write_str(message),
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
            Self::InvalidJoinToken => {
                formatter.write_str("join token is invalid, expired, or revoked")
            }
            Self::InvalidNodeName(message) => formatter.write_str(message),
            Self::IdentityAlreadyExists(id) => {
                write!(formatter, "identity already exists: {}", id.0)
            }
            Self::IdentityNotFound(id) => write!(formatter, "identity not found: {}", id.0),
            Self::ApiTokenAlreadyExists(id) => {
                write!(formatter, "API token already exists: {}", id.0)
            }
            Self::ApiTokenNotFound(id) => write!(formatter, "API token not found: {}", id.0),
        }
    }
}

impl std::error::Error for DomainError {}
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    AlertId, ApiToken, ApiTokenId, AvailabilityState, Evaluation, EvaluationAssignment,
    EvaluationId, IdentityId, JoinTokenHash, NodeTarget, NotificationChannel,
    NotificationChannelId, OperatorIdentity, Secret, SecretId, Target, TargetId,
};
