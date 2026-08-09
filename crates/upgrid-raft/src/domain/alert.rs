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
use serde::{Deserialize, Serialize};
use url::Url;

use super::{AlertKind, Evaluation, NotificationChannelId, TargetId};
