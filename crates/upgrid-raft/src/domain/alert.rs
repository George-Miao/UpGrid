use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

use super::{AlertKind, Evaluation, NotificationChannelId, TargetId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct AlertId {
    pub target_id: TargetId,
    pub channel_id: NotificationChannelId,
    pub evaluation_scheduled_at_ms: u64,
    pub kind: AlertKind,
}

impl<'de> Deserialize<'de> for AlertId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            #[derive(Deserialize)]
            struct Readable {
                target_id: TargetId,
                channel_id: NotificationChannelId,
                evaluation_scheduled_at_ms: u64,
                #[serde(default)]
                kind: AlertKind,
            }

            let readable = Readable::deserialize(deserializer)?;
            return Ok(Self {
                target_id: readable.target_id,
                channel_id: readable.channel_id,
                evaluation_scheduled_at_ms: readable.evaluation_scheduled_at_ms,
                kind: readable.kind,
            });
        }

        let (target_id, channel_id, evaluation_scheduled_at_ms, kind) =
            <(TargetId, NotificationChannelId, u64, AlertKind)>::deserialize(deserializer)?;
        Ok(Self {
            target_id,
            channel_id,
            evaluation_scheduled_at_ms,
            kind,
        })
    }
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
pub struct AvailabilityTransition {
    pub kind: AlertKind,
    pub target_name: String,
    pub target_url: Url,
    pub evaluation: Evaluation,
}

#[cfg(test)]
mod tests {
    use serde::Serialize;
    use uuid::Uuid;

    use super::*;

    #[derive(Serialize)]
    struct LegacyAlertId {
        target_id: TargetId,
        channel_id: NotificationChannelId,
        evaluation_scheduled_at_ms: u64,
    }

    #[test]
    fn legacy_json_alert_id_defaults_to_down() {
        let legacy = LegacyAlertId {
            target_id: TargetId(Uuid::from_u128(1)),
            channel_id: NotificationChannelId(Uuid::from_u128(2)),
            evaluation_scheduled_at_ms: 3,
        };
        let json = serde_json::to_vec(&legacy).unwrap();
        let decoded: AlertId = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded.kind, AlertKind::Down);
    }

    #[test]
    fn previous_postcard_alert_id_preserves_kind() {
        #[derive(Serialize)]
        struct PreviousAlertId {
            target_id: TargetId,
            channel_id: NotificationChannelId,
            evaluation_scheduled_at_ms: u64,
            kind: AlertKind,
        }

        let previous = PreviousAlertId {
            target_id: TargetId(Uuid::from_u128(1)),
            channel_id: NotificationChannelId(Uuid::from_u128(2)),
            evaluation_scheduled_at_ms: 3,
            kind: AlertKind::Recovered,
        };
        let postcard = postcard::to_extend(&previous, Vec::new()).unwrap();
        let decoded: AlertId = postcard::from_bytes(&postcard).unwrap();

        assert_eq!(decoded.kind, AlertKind::Recovered);
    }

    #[test]
    fn current_postcard_alert_id_preserves_kind() {
        let expected = AlertId {
            target_id: TargetId(Uuid::from_u128(1)),
            channel_id: NotificationChannelId(Uuid::from_u128(2)),
            evaluation_scheduled_at_ms: 3,
            kind: AlertKind::Recovered,
        };
        let postcard = postcard::to_extend(&expected, Vec::new()).unwrap();
        let decoded: AlertId = postcard::from_bytes(&postcard).unwrap();

        assert_eq!(decoded, expected);
    }
}
