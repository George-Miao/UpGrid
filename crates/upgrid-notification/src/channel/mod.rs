use std::collections::BTreeMap;

use http::StatusCode;
use snafu::{ResultExt, Snafu};
use upgrid_config::Cipher;
use upgrid_raft::domain::{
    Alert, AlertKind, ApplicationState, ConfigValue, ConfigValueError, NotificationChannelKind,
    resolve_config_value,
};
use url::Url;

mod telegram;
mod webhook;

/// Unsaved notification configuration used for a one-off delivery test.
#[derive(Clone, Debug)]
pub enum TestChannel {
    Telegram {
        bot_token: String,
        chat_id: String,
    },
    Webhook {
        url: Url,
        headers: BTreeMap<String, String>,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ChannelError {
    #[snafu(display("{source}"))]
    ConfigValue { source: ConfigValueError },

    #[snafu(display("invalid Telegram endpoint: {source}"))]
    TelegramUrl { source: url::ParseError },

    #[snafu(display("failed to encode Telegram request: {source}"))]
    TelegramBody { source: serde_json::Error },

    #[snafu(display("failed to encode webhook request: {source}"))]
    WebhookBody { source: serde_json::Error },
}

pub(crate) struct Request {
    pub(crate) url: Url,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Vec<u8>,
}

pub(crate) trait ChannelTarget {
    fn request(
        &self,
        state: &ApplicationState,
        cipher: &Cipher,
        alert: &Alert,
    ) -> Result<Request, ChannelError>;

    fn accepts(&self, status: StatusCode, _body: &[u8]) -> bool {
        status.is_success()
    }
}

pub(crate) fn target(kind: &NotificationChannelKind) -> Box<dyn ChannelTarget + '_> {
    match kind {
        NotificationChannelKind::Telegram { bot_token, chat_id } => {
            Box::new(telegram::Telegram::new(*bot_token, chat_id))
        }
        NotificationChannelKind::Webhook { url, headers } => {
            Box::new(webhook::Webhook::new(url, headers))
        }
    }
}

pub(crate) fn test_request(channel: &TestChannel) -> Result<Request, ChannelError> {
    match channel {
        TestChannel::Telegram { bot_token, chat_id } => telegram::test_request(bot_token, chat_id),
        TestChannel::Webhook { url, headers } => webhook::test_request(url, headers),
    }
}

pub(crate) fn test_accepts(channel: &TestChannel, status: StatusCode, body: &[u8]) -> bool {
    match channel {
        TestChannel::Telegram { .. } => telegram::accepts(status, body),
        TestChannel::Webhook { .. } => status.is_success(),
    }
}

pub(crate) fn alert_text(alert: &Alert) -> String {
    let status = alert.evaluation.http.status_code;
    match alert.id.kind {
        AlertKind::Down => {
            let detail = status.map_or_else(
                || {
                    alert
                        .evaluation
                        .diagnostic
                        .clone()
                        .unwrap_or_else(|| "Request failed".to_owned())
                },
                |status| format!("Request failed with status code {status}"),
            );
            format!("[{}] [🔴 Down] {detail}", alert.target_name)
        }
        AlertKind::Recovered => {
            let detail = status.map_or_else(
                || "Request succeeded".to_owned(),
                |status| {
                    let reason = StatusCode::from_u16(status)
                        .ok()
                        .and_then(|status| status.canonical_reason())
                        .unwrap_or("Unknown Status");
                    format!("{status} - {reason}")
                },
            );
            format!("[{}] [✅ Up] {detail}", alert.target_name)
        }
    }
}

fn resolve_value(
    state: &ApplicationState,
    cipher: &Cipher,
    value: &ConfigValue,
) -> Result<String, ChannelError> {
    resolve_config_value(state, cipher, value).context(ConfigValueSnafu)
}
