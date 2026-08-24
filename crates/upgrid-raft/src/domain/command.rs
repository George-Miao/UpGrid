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
    SetHistoryRollupRetention {
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
    DeleteUnreferencedSecrets,
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
    SetNodeDraining {
        node_id: Uuid,
        draining: bool,
        force: bool,
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
    AcknowledgeAlert {
        alert_id: AlertId,
        acknowledged_at_ms: u64,
    },
    RetryAlert {
        alert_id: AlertId,
        retry_at_ms: u64,
    },
    CreateTargetWithLocations {
        target: Target,
        use_default_notifications: bool,
        locations: u16,
    },
    UpdateTargetWithLocations {
        target: Target,
        use_default_notifications: bool,
        locations: u16,
    },
    TrashTarget {
        target_id: TargetId,
        deleted_at_ms: u64,
    },
    RestoreTarget {
        target_id: TargetId,
        restored_at_ms: u64,
    },
    PurgeTarget(TargetId),
    PruneTargetTrash {
        now_ms: u64,
    },
    SetTargetTrashRetention {
        retention_ms: u64,
        now_ms: u64,
    },
    SetPublicStatusEnabled {
        enabled: bool,
    },
    ReplaceConfiguredReachableAddresses {
        node_id: Uuid,
        addresses: BTreeSet<ReachableAddress>,
    },
    RenewReachabilityLeases(Vec<ReachableAddressLease>),
    VerifyReachableAddress {
        node_id: Uuid,
        address: ReachableAddress,
        verified_at_ms: u64,
    },
    RecordConnectivity {
        leases: Vec<ReachableAddressLease>,

        #[serde(default)]
        verified: Option<BTreeMap<Uuid, BTreeSet<ReachableAddress>>>,

        checked_at_ms: u64,
        failures: BTreeSet<DirectedRoute>,
    },
    ReserveJoinToken {
        hash: JoinTokenHash,
        reservation_id: Uuid,
        reservation_operation_id: Uuid,
        reserved_at_ms: u64,
        readmission: bool,
    },
    CompleteJoinTokenReservation {
        reservation_id: Uuid,
        reservation_operation_id: Uuid,
        accepted: bool,
        completed_at_ms: u64,
    },
    AbortPendingJoin {
        reservation_id: Uuid,
        reservation_operation_id: Uuid,
        completed_at_ms: u64,
    },
    AbortPendingReadmission {
        reservation_id: Uuid,
        reservation_operation_id: Uuid,
        completed_at_ms: u64,
    },
    ReplaceAdmissionConfiguredReachableAddresses {
        node_id: Uuid,
        addresses: BTreeSet<ReachableAddress>,
        reservation_operation_id: Uuid,
    },
    RenewAdmissionReachabilityLeases {
        reservation_id: Uuid,
        reservation_operation_id: Uuid,
        leases: Vec<ReachableAddressLease>,
    },
    VerifyAdmissionReachableAddress {
        node_id: Uuid,
        address: ReachableAddress,
        verified_at_ms: u64,
        reservation_operation_id: Uuid,
    },
}

impl Command {
    pub(crate) fn stamp_reachability_leases(&mut self, discovered_at_ms: u64) {
        let leases = match self {
            Self::RenewReachabilityLeases(leases)
            | Self::RecordConnectivity { leases, .. }
            | Self::RenewAdmissionReachabilityLeases { leases, .. } => leases,
            _ => return,
        };
        for lease in leases {
            lease.discovered_at_ms = discovered_at_ms;
            lease.expires_at_ms = discovered_at_ms.saturating_add(crate::REACHABILITY_LEASE_MS);
        }
    }
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
        #[serde(alias = "alert_deliveries")]
        alerts: Vec<AlertId>,
    },
    EvaluationDiscarded,
    AlertUpdated(AlertId),
    Noop,
    EvaluationAssigned(EvaluationId),
    HistoryRetentionSet(u64),
    HistoryRollupRetentionSet(u64),
    JoinTokenStored,
    JoinTokenAuthorized,
    TargetPauseSet {
        target_id: TargetId,
        paused: bool,
    },
    SecretDeleted(SecretId),
    UnreferencedSecretsDeleted(Vec<SecretId>),
    NotificationChannelDeleted(NotificationChannelId),
    JoinTokenRevoked,
    NodeNameSet(Uuid),
    NodeDrainSet {
        node_id: Uuid,
        draining: bool,
    },
    NotificationChannelDefaultSet(NotificationChannelId),
    NodeTargetsSynced,
    NodeEvaluationAccepted {
        availability: AvailabilityState,
        #[serde(alias = "alert_deliveries")]
        alerts: Vec<AlertId>,
    },
    NotificationChannelUpdated(NotificationChannelId),
    IdentityCreated(IdentityId),
    IdentityUpdated(IdentityId),
    IdentityDeleted(IdentityId),
    ApiTokenCreated(ApiTokenId),
    ApiTokenRevoked(ApiTokenId),
    EvaluationPending(EvaluationId),
    TargetTrashed(TargetId),
    TargetRestored(TargetId),
    TargetPurged(TargetId),
    TargetTrashRetentionSet(u64),
    TargetTrashPruned(u64),
    PublicStatusEnabledSet(bool),
    ConfiguredReachableAddressesReplaced(Uuid),
    ReachabilityLeasesRenewed,
    ReachableAddressVerified(Uuid),
    ConnectivityRecorded,
    JoinTokenReserved,
    JoinTokenReservationCompleted,
    AdmissionAccepted(Uuid),
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
    TrashedTargetNotFound(TargetId),
    NodeNotInMembership(Uuid),
    JoinAlreadyPending(Uuid),
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
            Self::TrashedTargetNotFound(id) => {
                write!(formatter, "trashed target not found: {}", id.0)
            }
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
            Self::JoinAlreadyPending(node_id) => {
                write!(
                    formatter,
                    "node {node_id} already has an admission in progress"
                )
            }
            Self::NodeNotInMembership(node_id) => {
                write!(formatter, "node is not in current membership: {node_id}")
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
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    AlertId, ApiToken, ApiTokenId, AvailabilityState, Evaluation, EvaluationAssignment,
    EvaluationId, IdentityId, JoinTokenHash, NodeTarget, NotificationChannel,
    NotificationChannelId, OperatorIdentity, Secret, SecretId, Target, TargetId,
};
use crate::{DirectedRoute, ReachableAddress, ReachableAddressLease};
