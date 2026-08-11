use snafu::{ResultExt, Snafu};

use super::*;

#[derive(Debug, Snafu)]
enum ChannelInputError {
    #[snafu(display("{source}"))]
    InvalidUrl { source: url::ParseError },

    #[snafu(display("{source}"))]
    SealSecret { source: upgrid_config::CipherError },
}

impl From<ChannelInputError> for ApiError {
    fn from(error: ChannelInputError) -> Self {
        Self::bad_request(error)
    }
}

fn cluster_snapshot(
    result: std::result::Result<ApplicationState, ClusterError>,
) -> std::result::Result<ApplicationState, ApiError> {
    match result {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => Err(ApiError::unavailable(error)),
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum PutChannelRequest {
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
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum TestChannelRequest {
    Telegram {
        bot_token: String,
        chat_id: String,
    },
    Webhook {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct SetChannelDefaultRequest {
    #[serde(rename = "default")]
    is_default: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ChannelView {
    id: Uuid,
    name: String,
    kind: String,
    destination: String,
    #[serde(rename = "default")]
    is_default: bool,
}

impl ChannelView {
    fn from_channel(channel: &NotificationChannel, is_default: bool) -> Self {
        let (kind, destination) = match &channel.kind {
            NotificationChannelKind::Telegram { chat_id, .. } => ("telegram", chat_id.clone()),
            NotificationChannelKind::Webhook { url, .. } => ("webhook", url.to_string()),
        };
        Self {
            id: channel.id.0,
            name: channel.name.clone(),
            kind: kind.to_owned(),
            destination,
            is_default,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/channels",
    responses(
        (status = 200, body = [ChannelView]),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_channels(
    State(state): State<WebState>,
) -> Result<Json<Vec<ChannelView>>, ApiError> {
    let snapshot = cluster_snapshot(state.cluster.read().await)?;
    Ok(Json(
        snapshot
            .notification_channels
            .values()
            .map(|channel| {
                ChannelView::from_channel(
                    channel,
                    snapshot.default_notification_channels.contains(&channel.id),
                )
            })
            .collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/channels",
    request_body = PutChannelRequest,
    responses(
        (status = 201, body = ChannelView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn create_channel(
    State(state): State<WebState>,
    Json(input): Json<PutChannelRequest>,
) -> Result<(StatusCode, Json<ChannelView>), ApiError> {
    let id = NotificationChannelId(Uuid::now_v7());
    let (channel, generated_secret, is_default) = match input {
        PutChannelRequest::Telegram {
            name,
            bot_token,
            chat_id,
            is_default,
        } => {
            let secret_id = SecretId(Uuid::now_v7());
            (
                NotificationChannel {
                    id,
                    name,
                    kind: NotificationChannelKind::Telegram {
                        bot_token: secret_id,
                        chat_id,
                    },
                },
                Some(Secret {
                    id: secret_id,
                    name: format!("telegram-{}", id.0),
                    ciphertext: state
                        .cipher
                        .seal(bot_token.as_bytes())
                        .context(SealSecretSnafu)?,
                }),
                is_default,
            )
        }
        PutChannelRequest::Webhook {
            name,
            url,
            headers,
            is_default,
        } => (
            NotificationChannel {
                id,
                name,
                kind: NotificationChannelKind::Webhook {
                    url: Url::parse(&url).context(InvalidUrlSnafu)?,
                    headers: headers
                        .into_iter()
                        .map(|(key, value)| (key, ConfigValue::from(value)))
                        .collect(),
                },
            },
            None,
            is_default,
        ),
    };
    state
        .cluster
        .apply(Command::CreateNotificationChannel {
            channel,
            generated_secret,
            is_default,
        })
        .await?;
    let snapshot = cluster_snapshot(state.cluster.read().await)?;
    Ok((
        StatusCode::CREATED,
        Json(ChannelView::from_channel(
            snapshot
                .notification_channels
                .get(&id)
                .expect("created channel exists"),
            snapshot.default_notification_channels.contains(&id),
        )),
    ))
}

#[utoipa::path(
    put,
    path = "/api/v1/channels/{id}/default",
    params(("id" = Uuid, Path)),
    request_body = SetChannelDefaultRequest,
    responses(
        (status = 200, body = ChannelView),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn set_channel_default(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Json(input): Json<SetChannelDefaultRequest>,
) -> Result<Json<ChannelView>, ApiError> {
    let id = NotificationChannelId(id);
    state
        .cluster
        .apply(Command::SetNotificationChannelDefault {
            channel_id: id,
            is_default: input.is_default,
        })
        .await?;
    let snapshot = cluster_snapshot(state.cluster.read().await)?;
    let channel = snapshot
        .notification_channels
        .get(&id)
        .ok_or_else(|| ApiError::not_found(format!("channel not found: {}", id.0)))?;
    Ok(Json(ChannelView::from_channel(
        channel,
        snapshot.default_notification_channels.contains(&id),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/channels/test",
    request_body = TestChannelRequest,
    responses(
        (status = 204),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn test_channel(
    State(state): State<WebState>,
    Json(input): Json<TestChannelRequest>,
) -> Result<StatusCode, ApiError> {
    let channel = match input {
        TestChannelRequest::Telegram { bot_token, chat_id } => {
            upgrid_notification::TestChannel::Telegram { bot_token, chat_id }
        }
        TestChannelRequest::Webhook { url, headers } => upgrid_notification::TestChannel::Webhook {
            url: Url::parse(&url).context(InvalidUrlSnafu)?,
            headers,
        },
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

#[utoipa::path(
    delete,
    path = "/api/v1/channels/{id}",
    params(("id" = Uuid, Path)),
    responses(
        (status = 204),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn delete_channel(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state
        .cluster
        .apply(Command::DeleteNotificationChannel(NotificationChannelId(
            id,
        )))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
