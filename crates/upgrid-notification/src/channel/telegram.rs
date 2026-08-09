use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::json;
use upgrid_config::Cipher;
use upgrid_raft::domain::{Alert, ApplicationState, ConfigValue, SecretId};
use url::Url;

use super::{ChannelTarget, Request, alert_text, resolve_value};

pub(super) struct Telegram<'a> {
    bot_token: SecretId,
    chat_id: &'a str,
}

impl<'a> Telegram<'a> {
    pub(super) fn new(bot_token: SecretId, chat_id: &'a str) -> Self {
        Self { bot_token, chat_id }
    }
}

impl ChannelTarget for Telegram<'_> {
    fn request(
        &self,
        state: &ApplicationState,
        cipher: &Cipher,
        alert: &Alert,
    ) -> Result<Request, String> {
        let token = resolve_value(state, cipher, &ConfigValue::Secret(self.bot_token))?;
        request(&token, self.chat_id, &alert_text(alert))
    }

    fn accepts(&self, status: StatusCode, body: &[u8]) -> bool {
        accepts(status, body)
    }
}

pub(super) fn test_request(bot_token: &str, chat_id: &str) -> Result<Request, String> {
    request(bot_token, chat_id, "UpGrid notification channel test")
}

pub(super) fn accepts(status: StatusCode, body: &[u8]) -> bool {
    status.is_success()
        && serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|value| value.get("ok").and_then(|ok| ok.as_bool()))
            .unwrap_or(true)
}

fn request(bot_token: &str, chat_id: &str, text: &str) -> Result<Request, String> {
    let url = Url::parse(&format!(
        "https://api.telegram.org/bot{bot_token}/sendMessage"
    ))
    .map_err(|error| error.to_string())?;
    let body = serde_json::to_vec(&json!({ "chat_id": chat_id, "text": text }))
        .map_err(|error| error.to_string())?;
    Ok(Request {
        url,
        headers: BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
        body,
    })
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn requires_both_http_and_telegram_success() {
        let target = Telegram::new(SecretId(Uuid::from_u128(1)), "1234");

        assert!(target.accepts(StatusCode::OK, br#"{"ok":true}"#));
        assert!(!target.accepts(StatusCode::OK, br#"{"ok":false}"#));
        assert!(!target.accepts(StatusCode::BAD_REQUEST, br#"{"ok":true}"#));
    }

    #[test]
    fn test_message_uses_unsaved_credentials() {
        let request = test_request("bot-token", "chat-id").unwrap();
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();

        assert_eq!(
            request.url.as_str(),
            "https://api.telegram.org/botbot-token/sendMessage"
        );
        assert_eq!(body["chat_id"], "chat-id");
        assert_eq!(body["text"], "UpGrid notification channel test");
    }
}
