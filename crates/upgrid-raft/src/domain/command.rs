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
    SetTargetPaused {
        target_id: TargetId,
        paused: bool,
    },
    DeleteSecret(SecretId),
    DeleteNotificationChannel(NotificationChannelId),
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
    TargetPauseSet {
        target_id: TargetId,
        paused: bool,
    },
    SecretDeleted(SecretId),
    NotificationChannelDeleted(NotificationChannelId),
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
            Self::InvalidJoinToken => {
                formatter.write_str("join link is invalid, expired, or already used")
            }
        }
    }
}

impl std::error::Error for DomainError {}
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use super::{
    AlertId, AvailabilityState, Evaluation, EvaluationAssignment, EvaluationId, JoinTokenHash,
    NotificationChannel, NotificationChannelId, Secret, SecretId, Target, TargetId,
};
