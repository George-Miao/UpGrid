use std::collections::BTreeMap;

use http::StatusCode;
use upgrid_config::Cipher;
use upgrid_raft::domain::{
    Alert, AlertKind, ApplicationState, ConfigValue, NotificationChannelKind,
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
    ) -> Result<Request, String>;

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

pub(crate) fn test_request(channel: &TestChannel) -> Result<Request, String> {
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

fn alert_text(alert: &Alert) -> String {
    format!(
        "UpGrid: {} is {}\n{}\n{}",
        alert.target_name,
        match alert.id.kind {
            AlertKind::Down => "DOWN",
            AlertKind::Recovered => "UP again",
        },
        alert.target_url,
        alert
            .evaluation
            .diagnostic
            .as_deref()
            .unwrap_or("evaluation succeeded"),
    )
}

fn resolve_value(
    state: &ApplicationState,
    cipher: &Cipher,
    value: &ConfigValue,
) -> Result<String, String> {
    match value {
        ConfigValue::Literal(value) => Ok(value.clone()),
        ConfigValue::Secret(id) => state
            .secrets
            .get(id)
            .ok_or_else(|| format!("secret {} no longer exists", id.0))
            .and_then(|secret| {
                cipher
                    .open(&secret.ciphertext)
                    .map_err(|error| format!("could not decrypt secret {}: {error}", id.0))
                    .and_then(|plaintext| {
                        String::from_utf8(plaintext)
                            .map_err(|_| format!("secret {} is not UTF-8", id.0))
                    })
            }),
    }
}
