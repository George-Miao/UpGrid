use std::collections::BTreeMap;

use serde_json::json;
use snafu::ResultExt;
use upgrid_config::Cipher;
use upgrid_raft::domain::{Alert, AlertKind, ApplicationState, ConfigValue};
use url::Url;

use super::{ChannelError, ChannelTarget, Delivery, Request, WebhookBodySnafu, resolve_value};

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
    ) -> Result<Delivery, ChannelError> {
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
        .context(WebhookBodySnafu)?;
        Ok(Delivery::Http(Request {
            url: self.url.clone(),
            headers,
            body,
        }))
    }
}

pub(super) fn test_request(
    url: &Url,
    headers: &BTreeMap<String, String>,
) -> Result<Request, ChannelError> {
    let mut headers = headers.clone();
    headers
        .entry("content-type".to_owned())
        .or_insert_with(|| "application/json".to_owned());
    let body = serde_json::to_vec(&json!({
        "event": "test",
        "message": "UpGrid notification channel test",
    }))
    .context(WebhookBodySnafu)?;
    Ok(Request {
        url: url.clone(),
        headers,
        body,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_identifies_itself_without_an_alert() {
        let url = Url::parse("https://example.com/hook").unwrap();
        let request = test_request(&url, &BTreeMap::new()).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();

        assert_eq!(request.url, url);
        assert_eq!(request.headers["content-type"], "application/json");
        assert_eq!(body["event"], "test");
        assert_eq!(body["message"], "UpGrid notification channel test");
    }
}
