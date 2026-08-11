use super::*;

#[derive(Clone, Copy, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SmtpSecurityInput {
    None,
    StartTls,
    Tls,
}

impl From<SmtpSecurityInput> for SmtpSecurity {
    fn from(value: SmtpSecurityInput) -> Self {
        match value {
            SmtpSecurityInput::None => Self::None,
            SmtpSecurityInput::StartTls => Self::StartTls,
            SmtpSecurityInput::Tls => Self::Tls,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PutChannelRequest {
    Telegram {
        name: String,
        bot_token: String,
        chat_id: String,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
    Webhook {
        name: String,
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, ConfigValueInput>,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
    Smtp {
        name: String,
        host: String,
        port: u16,
        security: SmtpSecurityInput,
        username: Option<String>,
        password: Option<String>,
        from: String,
        to: String,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum UpdateChannelRequest {
    Telegram {
        name: String,
        #[serde(default)]
        bot_token: Option<String>,
        chat_id: String,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
    Webhook {
        name: String,
        url: String,
        headers: Option<BTreeMap<String, ConfigValueInput>>,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
    Smtp {
        name: String,
        host: String,
        port: u16,
        security: SmtpSecurityInput,
        username: Option<String>,
        password: Option<String>,
        from: String,
        to: String,
        #[serde(default, rename = "default")]
        is_default: bool,
    },
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum TestChannelRequest {
    Telegram {
        bot_token: String,
        chat_id: String,
    },
    Webhook {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    Smtp {
        host: String,
        port: u16,
        security: SmtpSecurityInput,
        username: Option<String>,
        password: Option<String>,
        from: String,
        to: String,
    },
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct SetChannelDefaultRequest {
    #[serde(rename = "default")]
    pub(super) is_default: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ChannelView {
    id: Uuid,
    name: String,
    kind: String,
    destination: String,
    headers: BTreeMap<String, ConfigValueView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    security: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<String>,
    #[serde(rename = "default")]
    is_default: bool,
}

impl ChannelView {
    pub(super) fn from_channel(channel: &NotificationChannel, is_default: bool) -> Self {
        let (kind, destination, headers, port, security, username, from, to) = match &channel.kind {
            NotificationChannelKind::Telegram { chat_id, .. } => (
                "telegram",
                chat_id.clone(),
                BTreeMap::new(),
                None,
                None,
                None,
                None,
                None,
            ),
            NotificationChannelKind::Webhook { url, headers } => (
                "webhook",
                url.to_string(),
                headers
                    .iter()
                    .map(|(name, value)| (name.clone(), ConfigValueView::from(value)))
                    .collect(),
                None,
                None,
                None,
                None,
                None,
            ),
            NotificationChannelKind::Smtp {
                host,
                port,
                security,
                username,
                from,
                to,
                ..
            } => (
                "smtp",
                host.clone(),
                BTreeMap::new(),
                Some(*port),
                Some(
                    match security {
                        SmtpSecurity::None => "none",
                        SmtpSecurity::StartTls => "start_tls",
                        SmtpSecurity::Tls => "tls",
                    }
                    .to_owned(),
                ),
                username.clone(),
                Some(from.clone()),
                Some(to.clone()),
            ),
        };
        Self {
            id: channel.id.0,
            name: channel.name.clone(),
            kind: kind.to_owned(),
            destination,
            headers,
            port,
            security,
            username,
            from,
            to,
            is_default,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smtp_channel_view_never_exposes_password() {
        let channel = NotificationChannel {
            id: NotificationChannelId(Uuid::from_u128(1)),
            name: "Email".to_owned(),
            kind: NotificationChannelKind::Smtp {
                host: "smtp.example.com".to_owned(),
                port: 587,
                security: SmtpSecurity::StartTls,
                username: Some("upgrid".to_owned()),
                password: Some(SecretId(Uuid::from_u128(2))),
                from: "upgrid@example.com".to_owned(),
                to: "on-call@example.com".to_owned(),
            },
        };

        let json = serde_json::to_value(ChannelView::from_channel(&channel, true)).unwrap();

        assert_eq!(json["kind"], "smtp");
        assert_eq!(json["destination"], "smtp.example.com");
        assert_eq!(json["security"], "start_tls");
        assert_eq!(json["username"], "upgrid");
        assert_eq!(json["default"], true);
        assert!(json.get("password").is_none());
    }

    #[test]
    fn smtp_update_allows_omitted_write_only_password() {
        let input: UpdateChannelRequest = serde_json::from_value(serde_json::json!({
            "type": "smtp",
            "name": "Email",
            "host": "smtp.example.com",
            "port": 587,
            "security": "start_tls",
            "username": "upgrid",
            "from": "upgrid@example.com",
            "to": "on-call@example.com"
        }))
        .unwrap();

        assert!(matches!(
            input,
            UpdateChannelRequest::Smtp { password: None, .. }
        ));
    }
}
