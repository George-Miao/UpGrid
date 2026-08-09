use std::cmp::Reverse;

use super::*;

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

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct TransitionView {
    target_id: Uuid,
    kind: String,
    target_name: String,
    scheduled_at_ms: u64,
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
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot
            .notification_channels
            .values()
            .map(ChannelView::from)
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
    let channel = match input {
        PutChannelRequest::Telegram {
            name,
            bot_token,
            chat_id,
        } => {
            let secret_id = SecretId(Uuid::now_v7());
            state
                .cluster
                .apply(Command::PutSecret(Secret {
                    id: secret_id,
                    name: format!("telegram-{}", id.0),
                    ciphertext: state
                        .cipher
                        .seal(bot_token.as_bytes())
                        .map_err(ApiError::bad_request)?,
                }))
                .await?;
            NotificationChannel {
                id,
                name,
                kind: NotificationChannelKind::Telegram {
                    bot_token: secret_id,
                    chat_id,
                },
            }
        }
        PutChannelRequest::Webhook { name, url, headers } => NotificationChannel {
            id,
            name,
            kind: NotificationChannelKind::Webhook {
                url: Url::parse(&url).map_err(ApiError::bad_request)?,
                headers: headers
                    .into_iter()
                    .map(|(key, value)| (key, ConfigValue::from(value)))
                    .collect(),
            },
        },
    };
    state
        .cluster
        .apply(Command::PutNotificationChannel(channel))
        .await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok((
        StatusCode::CREATED,
        Json(ChannelView::from(
            snapshot
                .notification_channels
                .get(&id)
                .expect("created channel exists"),
        )),
    ))
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
            url: Url::parse(&url).map_err(ApiError::bad_request)?,
            headers,
        },
    };
    state
        .notifications
        .send(channel)
        .await
        .map_err(|error| match error {
            upgrid_notification::TestError::Unavailable => ApiError::unavailable(error),
            upgrid_notification::TestError::Failed(_) => ApiError::unprocessable(error),
        })?;
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

#[utoipa::path(
    get,
    path = "/api/v1/secrets",
    responses(
        (status = 200, body = [SecretView]),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_secrets(
    State(state): State<WebState>,
) -> Result<Json<Vec<SecretView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot.secrets.values().map(SecretView::from).collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/secrets",
    request_body = PutSecretRequest,
    responses(
        (status = 201, body = SecretView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn create_secret(
    State(state): State<WebState>,
    Json(input): Json<PutSecretRequest>,
) -> Result<(StatusCode, Json<SecretView>), ApiError> {
    let id = SecretId(Uuid::now_v7());
    state
        .cluster
        .apply(Command::PutSecret(Secret {
            id,
            name: input.name,
            ciphertext: state
                .cipher
                .seal(input.value.as_bytes())
                .map_err(ApiError::bad_request)?,
        }))
        .await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok((
        StatusCode::CREATED,
        Json(SecretView::from(
            snapshot.secrets.get(&id).expect("created secret exists"),
        )),
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/secrets/{id}",
    params(("id" = Uuid, Path)),
    responses(
        (status = 204),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn delete_secret(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state
        .cluster
        .apply(Command::DeleteSecret(SecretId(id)))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/v1/alerts",
    responses(
        (status = 200, body = [AlertView]),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_alerts(
    State(state): State<WebState>,
) -> Result<Json<Vec<AlertView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let alerts = snapshot
        .alerts
        .values()
        .rev()
        .map(|alert| AlertView {
            target_id: alert.id.target_id.0,
            channel_id: alert.id.channel_id.0,
            kind: match alert.id.kind {
                AlertKind::Down => "down",
                AlertKind::Recovered => "recovered",
            }
            .to_owned(),
            target_name: alert.target_name.clone(),
            scheduled_at_ms: alert.id.evaluation_scheduled_at_ms,
            delivery: match &alert.delivery {
                AlertDelivery::Pending { .. } => "pending",
                AlertDelivery::Delivered { .. } => "delivered",
                AlertDelivery::Failed { .. } => "failed",
            }
            .to_owned(),
        })
        .collect();
    Ok(Json(alerts))
}

#[utoipa::path(
    get,
    path = "/api/v1/transitions",
    responses(
        (status = 200, body = [TransitionView]),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_transitions(
    State(state): State<WebState>,
) -> Result<Json<Vec<TransitionView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let mut transitions = snapshot
        .transitions
        .values()
        .map(|transition| TransitionView {
            target_id: transition.evaluation.id.target_id.0,
            kind: match transition.kind {
                AlertKind::Down => "down",
                AlertKind::Recovered => "recovered",
            }
            .to_owned(),
            target_name: transition.target_name.clone(),
            scheduled_at_ms: transition.evaluation.id.scheduled_at_ms,
        })
        .collect::<Vec<_>>();
    transitions.sort_by_key(|transition| Reverse(transition.scheduled_at_ms));
    Ok(Json(transitions))
}

#[utoipa::path(
    get,
    path = "/api/v1/cluster",
    responses(
        (status = 200, body = ClusterView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn get_cluster(
    State(state): State<WebState>,
) -> Result<Json<ClusterView>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let status = state
        .cluster
        .status()
        .await
        .map_err(ApiError::unavailable)?;
    Ok(Json(ClusterView {
        leader_node_id: status.leader_node_id,
        local_node_id: status.local_node_id,
        members: status
            .members
            .into_iter()
            .map(|(id, raft_url)| ClusterMemberView {
                id,
                name: snapshot
                    .node_names
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| upgrid_config::friendly_node_name(id)),
                raft_url,
                leader: status.leader_node_id == Some(id),
                local: status.local_node_id == id,
            })
            .collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/events",
    responses(
        (
            status = 200,
            description = "SSE stream of state versions",
            body = String,
            content_type = "text/event-stream",
        ),
        (status = 401, body = ErrorBody),
    )
)]
pub(super) async fn events(
    State(state): State<WebState>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let mut receiver = state.cluster.subscribe();
    let initial = state.cluster.version();
    let stream = async_stream::stream! {
        yield Ok(Event::default().event("state").data(initial.to_string()));
        while receiver.changed().await.is_ok() {
            let version = *receiver.borrow_and_update();
            yield Ok(Event::default().event("state").data(version.to_string()));
        }
    };
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}
