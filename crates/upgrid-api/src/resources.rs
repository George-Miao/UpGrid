use super::*;

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
    path = "/api/v1/join-tokens",
    responses(
        (status = 200, body = [JoinTokenView]),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_join_tokens(
    State(state): State<WebState>,
) -> Result<Json<Vec<JoinTokenView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot
            .join_tokens
            .iter()
            .map(|(hash, expires_at_ms)| JoinTokenView {
                id: encode_join_token_id(hash),
                expires_at_ms: *expires_at_ms,
            })
            .collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/join-tokens",
    request_body = CreateJoinTokenRequest,
    responses(
        (status = 201, body = CreatedJoinTokenView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn create_join_token(
    State(state): State<WebState>,
    Json(input): Json<CreateJoinTokenRequest>,
) -> Result<(StatusCode, Json<CreatedJoinTokenView>), ApiError> {
    if input.expires_in_seconds == 0 || input.expires_in_seconds > 24 * 60 * 60 {
        return Err(ApiError::bad_request(
            "join link lifetime must be between 1 second and 24 hours",
        ));
    }
    let token = generate_join_token().map_err(ApiError::unavailable)?;
    let hash = hash_join_token(&token);
    let link =
        JoinLink::issue(&state.raft_url, &state.cipher, token).map_err(ApiError::bad_request)?;
    let expires_at_ms = now_ms().saturating_add(input.expires_in_seconds.saturating_mul(1_000));
    state
        .cluster
        .apply(Command::PutJoinToken {
            hash,
            expires_at_ms,
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedJoinTokenView {
            id: encode_join_token_id(&hash),
            url: link.to_string(),
            expires_at_ms,
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/join-tokens/{id}",
    params(("id" = String, Path)),
    responses(
        (status = 204),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn revoke_join_token(
    State(state): State<WebState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let hash = decode_join_token_id(&id)?;
    state.cluster.apply(Command::RevokeJoinToken(hash)).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn encode_join_token_id(hash: &upgrid_raft::domain::JoinTokenHash) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash.0)
}

fn decode_join_token_id(id: &str) -> Result<upgrid_raft::domain::JoinTokenHash, ApiError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(id)
        .map_err(|_| ApiError::bad_request("invalid Join Token ID"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| ApiError::bad_request("invalid Join Token ID"))?;
    Ok(upgrid_raft::domain::JoinTokenHash(bytes))
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
