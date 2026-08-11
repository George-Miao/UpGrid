use std::collections::BTreeMap;

use http::StatusCode;
use snafu::{ResultExt, Snafu};
use upgrid_config::Cipher;
use upgrid_raft::domain::{
    Alert, AlertKind, ApplicationState, ConfigValue, ConfigValueError, NotificationChannelKind,
    resolve_config_value,
};
use url::Url;

mod smtp;
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
    Smtp {
        host: String,
        port: u16,
        security: upgrid_raft::domain::SmtpSecurity,
        username: Option<String>,
        password: Option<String>,
        from: String,
        to: String,
    },
}

pub(crate) enum Delivery {
    Http(Request),
    Smtp(smtp::Request),
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
    #[snafu(display("invalid SMTP configuration: {message}"))]
    InvalidSmtp { message: &'static str },

    #[snafu(display("invalid email address: {source}"))]
    EmailAddress {
        source: lettre::address::AddressError,
    },

    #[snafu(display("failed to construct email: {source}"))]
    Email { source: lettre::error::Error },

    #[snafu(display("failed to configure SMTP transport: {source}"))]
    SmtpTransport {
        source: lettre::transport::smtp::Error,
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
    ) -> Result<Delivery, ChannelError>;

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
        NotificationChannelKind::Smtp {
            host,
            port,
            security,
            username,
            password,
            from,
            to,
        } => Box::new(smtp::Smtp::new(
            host,
            *port,
            *security,
            username.as_deref(),
            *password,
            from,
            to,
        )),
    }
}

pub(crate) fn test_request(channel: &TestChannel) -> Result<Delivery, ChannelError> {
    match channel {
        TestChannel::Telegram { bot_token, chat_id } => {
            telegram::test_request(bot_token, chat_id).map(Delivery::Http)
        }
        TestChannel::Webhook { url, headers } => {
            webhook::test_request(url, headers).map(Delivery::Http)
        }
        TestChannel::Smtp {
            host,
            port,
            security,
            username,
            password,
            from,
            to,
        } => smtp::test_request(
            host,
            *port,
            *security,
            username.as_deref(),
            password.as_deref(),
            from,
            to,
        )
        .map(Delivery::Smtp),
    }
}

pub(crate) fn test_accepts(channel: &TestChannel, status: StatusCode, body: &[u8]) -> bool {
    match channel {
        TestChannel::Telegram { .. } => telegram::accepts(status, body),
        TestChannel::Webhook { .. } | TestChannel::Smtp { .. } => status.is_success(),
    }
}
pub(crate) fn send_smtp(request: smtp::Request) -> Result<(), lettre::transport::smtp::Error> {
    smtp::send(request)
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
