use snafu::{ResultExt, Snafu};

use super::*;
mod model;
pub(super) mod test;

use model::*;

#[derive(Debug, Snafu)]
enum ChannelInputError {
    #[snafu(display("{source}"))]
    InvalidUrl { source: url::ParseError },

    #[snafu(display("{source}"))]
    SealSecret { source: upgrid_config::CipherError },

    #[snafu(display("Notification channel type cannot be changed"))]
    KindChanged,
    #[snafu(display("SMTP username and password must be configured together"))]
    InvalidSmtpAuth,
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
fn smtp_auth(
    state: &WebState,
    channel_id: NotificationChannelId,
    username: &Option<String>,
    password: Option<String>,
    existing_id: Option<SecretId>,
) -> Result<(Option<SecretId>, Option<Secret>), ChannelInputError> {
    match (username, password) {
        (None, None) => Ok((None, None)),
        (None, Some(_)) => Err(InvalidSmtpAuthSnafu.build()),
        (Some(_), None) if existing_id.is_none() => Err(InvalidSmtpAuthSnafu.build()),
        (Some(_), None) => Ok((existing_id, None)),
        (Some(_), Some(password)) => {
            let secret_id = existing_id.unwrap_or_else(|| SecretId(Uuid::now_v7()));
            Ok((
                Some(secret_id),
                Some(Secret {
                    id: secret_id,
                    name: format!("smtp-{}", channel_id.0),
                    ciphertext: state
                        .cipher
                        .seal(password.as_bytes())
                        .context(SealSecretSnafu)?,
                }),
            ))
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
        PutChannelRequest::Smtp {
            name,
            host,
            port,
            security,
            username,
            password,
            from,
            to,
            is_default,
        } => {
            let (password, generated_secret) = smtp_auth(&state, id, &username, password, None)?;
            (
                NotificationChannel {
                    id,
                    name,
                    kind: NotificationChannelKind::Smtp {
                        host,
                        port,
                        security: security.into(),
                        username,
                        password,
                        from,
                        to,
                    },
                },
                generated_secret,
                is_default,
            )
        }
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
    path = "/api/v1/channels/{id}",
    params(("id" = Uuid, Path)),
    request_body = UpdateChannelRequest,
    responses(
        (status = 200, body = ChannelView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn update_channel(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Json(input): Json<UpdateChannelRequest>,
) -> Result<Json<ChannelView>, ApiError> {
    let id = NotificationChannelId(id);
    let snapshot = cluster_snapshot(state.cluster.read().await)?;
    let existing = snapshot
        .notification_channels
        .get(&id)
        .ok_or_else(|| ApiError::not_found(format!("channel not found: {}", id.0)))?;
    let (channel, generated_secret, is_default) = match (input, &existing.kind) {
        (
            UpdateChannelRequest::Telegram {
                name,
                bot_token,
                chat_id,
                is_default,
            },
            NotificationChannelKind::Telegram {
                bot_token: secret_id,
                ..
            },
        ) => {
            let generated_secret = bot_token
                .map(|token| -> Result<Secret, ChannelInputError> {
                    Ok(Secret {
                        id: *secret_id,
                        name: format!("telegram-{}", id.0),
                        ciphertext: state
                            .cipher
                            .seal(token.as_bytes())
                            .context(SealSecretSnafu)?,
                    })
                })
                .transpose()?;
            (
                NotificationChannel {
                    id,
                    name,
                    kind: NotificationChannelKind::Telegram {
                        bot_token: *secret_id,
                        chat_id,
                    },
                },
                generated_secret,
                is_default,
            )
        }
        (
            UpdateChannelRequest::Webhook {
                name,
                url,
                headers,
                is_default,
            },
            NotificationChannelKind::Webhook {
                headers: existing_headers,
                ..
            },
        ) => (
            NotificationChannel {
                id,
                name,
                kind: NotificationChannelKind::Webhook {
                    url: Url::parse(&url).context(InvalidUrlSnafu)?,
                    headers: headers
                        .map(|headers| {
                            headers
                                .into_iter()
                                .map(|(key, value)| (key, ConfigValue::from(value)))
                                .collect()
                        })
                        .unwrap_or_else(|| existing_headers.clone()),
                },
            },
            None,
            is_default,
        ),
        (
            UpdateChannelRequest::Smtp {
                name,
                host,
                port,
                security,
                username,
                password,
                from,
                to,
                is_default,
            },
            NotificationChannelKind::Smtp {
                password: existing_password,
                ..
            },
        ) => {
            let (password, generated_secret) =
                smtp_auth(&state, id, &username, password, *existing_password)?;
            (
                NotificationChannel {
                    id,
                    name,
                    kind: NotificationChannelKind::Smtp {
                        host,
                        port,
                        security: security.into(),
                        username,
                        password,
                        from,
                        to,
                    },
                },
                generated_secret,
                is_default,
            )
        }
        _ => return Err(ChannelInputError::KindChanged.into()),
    };
    state
        .cluster
        .apply(Command::UpdateNotificationChannel {
            channel,
            generated_secret,
            is_default,
        })
        .await?;
    let snapshot = cluster_snapshot(state.cluster.read().await)?;
    let channel = snapshot
        .notification_channels
        .get(&id)
        .expect("updated channel exists");
    Ok(Json(ChannelView::from_channel(
        channel,
        snapshot.default_notification_channels.contains(&id),
    )))
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
