use snafu::Snafu;
use upgrid_raft::domain::resolve_config_value;

use super::*;

#[derive(Debug, Snafu)]
enum TestChannelInputError {
    #[snafu(display("Bot token is required for a new Telegram test"))]
    MissingBotToken,

    #[snafu(display("Notification channel type cannot be changed"))]
    KindChanged,
}

impl From<TestChannelInputError> for ApiError {
    fn from(error: TestChannelInputError) -> Self {
        Self::bad_request(error)
    }
}

fn stored_value(
    state: &WebState,
    snapshot: &ApplicationState,
    value: &ConfigValue,
) -> Result<String, ApiError> {
    resolve_config_value(snapshot, &state.cipher, value).map_err(ApiError::unavailable)
}

fn stored_headers(
    state: &WebState,
    snapshot: &ApplicationState,
    values: &BTreeMap<String, ConfigValue>,
) -> Result<BTreeMap<String, String>, ApiError> {
    values
        .iter()
        .map(|(name, value)| Ok((name.clone(), stored_value(state, snapshot, value)?)))
        .collect()
}

#[utoipa::path(
    post,
    path = "/api/v1/channels/test",
    request_body = TestChannelRequest,
    responses(
        (status = 204),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(crate) async fn test_channel(
    State(state): State<WebState>,
    Json(input): Json<TestChannelRequest>,
) -> Result<StatusCode, ApiError> {
    let TestChannelRequest {
        channel_id,
        channel,
    } = input;
    let snapshot = if channel_id.is_some() {
        Some(cluster_snapshot(state.cluster.read().await)?)
    } else {
        None
    };
    let existing = match (snapshot.as_ref(), channel_id) {
        (Some(snapshot), Some(id)) => Some(
            snapshot
                .notification_channels
                .get(&NotificationChannelId(id))
                .ok_or_else(|| ApiError::not_found(format!("channel not found: {id}")))?,
        ),
        _ => None,
    };

    let channel = match channel {
        TestChannelInput::Telegram { bot_token, chat_id } => {
            let stored = match existing.map(|channel| &channel.kind) {
                Some(NotificationChannelKind::Telegram { bot_token, .. }) => Some(*bot_token),
                Some(_) => return Err(TestChannelInputError::KindChanged.into()),
                None => None,
            };
            let bot_token = match (bot_token, stored, snapshot.as_ref()) {
                (Some(bot_token), ..) => bot_token,
                (None, Some(id), Some(snapshot)) => {
                    stored_value(&state, snapshot, &ConfigValue::Secret(id))?
                }
                _ => return Err(TestChannelInputError::MissingBotToken.into()),
            };
            upgrid_notification::TestChannel::Telegram { bot_token, chat_id }
        }
        TestChannelInput::Webhook { url, headers } => {
            let stored = match existing.map(|channel| &channel.kind) {
                Some(NotificationChannelKind::Webhook { headers, .. }) => Some(headers),
                Some(_) => return Err(TestChannelInputError::KindChanged.into()),
                None => None,
            };
            let headers = match (headers, stored, snapshot.as_ref()) {
                (Some(headers), ..) => headers,
                (None, Some(headers), Some(snapshot)) => stored_headers(&state, snapshot, headers)?,
                _ => BTreeMap::new(),
            };
            upgrid_notification::TestChannel::Webhook {
                url: Url::parse(&url).context(InvalidUrlSnafu)?,
                headers,
            }
        }
        TestChannelInput::Smtp {
            host,
            port,
            security,
            username,
            password,
            from,
            to,
        } => {
            let stored = match existing.map(|channel| &channel.kind) {
                Some(NotificationChannelKind::Smtp { password, .. }) => *password,
                Some(_) => return Err(TestChannelInputError::KindChanged.into()),
                None => None,
            };
            let password = match (password, username.as_ref(), stored, snapshot.as_ref()) {
                (Some(password), ..) => Some(password),
                (None, None, ..) => None,
                (None, Some(_), Some(id), Some(snapshot)) => {
                    Some(stored_value(&state, snapshot, &ConfigValue::Secret(id))?)
                }
                _ => None,
            };
            upgrid_notification::TestChannel::Smtp {
                host,
                port,
                security: security.into(),
                username,
                password,
                from,
                to,
            }
        }
    };
    match state.notifications.send(channel).await {
        Ok(()) => {}
        Err(error @ upgrid_notification::TestError::Unavailable) => {
            return Err(ApiError::unavailable(error));
        }
        Err(error) => return Err(ApiError::unprocessable(error)),
    }
    Ok(StatusCode::NO_CONTENT)
}
