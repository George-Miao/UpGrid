use std::collections::BTreeMap;

use serde_json::json;
use upgrid_config::Cipher;
use upgrid_raft::domain::{Alert, AlertKind, ApplicationState, ConfigValue};
use url::Url;

use super::{ChannelTarget, Request, resolve_value};

pub(super) struct Webhook<'a> {
    url: &'a Url,
    headers: &'a BTreeMap<String, ConfigValue>,
}

impl<'a> Webhook<'a> {
    pub(super) fn new(url: &'a Url, headers: &'a BTreeMap<String, ConfigValue>) -> Self {
        Self { url, headers }
    }
}

impl ChannelTarget for Webhook<'_> {
    fn request(
        &self,
        state: &ApplicationState,
        cipher: &Cipher,
        alert: &Alert,
    ) -> Result<Request, String> {
        let mut headers = self
            .headers
            .iter()
            .map(|(name, value)| {
                resolve_value(state, cipher, value).map(|value| (name.clone(), value))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        headers
            .entry("content-type".to_owned())
            .or_insert_with(|| "application/json".to_owned());
        let body = serde_json::to_vec(&json!({
            "alert_id": stable_alert_id(alert),
            "event": match alert.id.kind {
                AlertKind::Down => "down",
                AlertKind::Recovered => "recovered",
            },
            "target_id": alert.id.target_id.0,
            "target_name": alert.target_name,
            "target_url": alert.target_url,
            "evaluation": {
                "scheduled_at_ms": alert.evaluation.id.scheduled_at_ms,
                "succeeded": alert.evaluation.succeeded,
                "status_code": alert.evaluation.http.status_code,
                "diagnostic": alert.evaluation.diagnostic,
            },
        }))
        .map_err(|error| error.to_string())?;
        Ok(Request {
            url: self.url.clone(),
            headers,
            body,
        })
    }
}

fn stable_alert_id(alert: &Alert) -> String {
    format!(
        "{}:{}:{}:{}",
        alert.id.target_id.0,
        alert.id.channel_id.0,
        alert.id.evaluation_scheduled_at_ms,
        match alert.id.kind {
            AlertKind::Down => "down",
            AlertKind::Recovered => "recovered",
        }
    )
}
