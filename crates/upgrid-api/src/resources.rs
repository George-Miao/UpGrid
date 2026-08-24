use std::cmp::Reverse;

use futures_util::SinkExt;

use super::*;

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct AvailabilityTransitionView {
    target_id: Uuid,
    kind: String,
    target_name: String,
    scheduled_at_ms: u64,
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
    let referenced = snapshot.referenced_secret_ids();
    Ok(Json(
        snapshot
            .secrets
            .values()
            .map(|secret| SecretView::new(secret, referenced.contains(&secret.id)))
            .collect(),
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
        Json(SecretView::new(
            snapshot.secrets.get(&id).expect("created secret exists"),
            false,
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
    delete,
    path = "/api/v1/secrets/unreferenced",
    responses(
        (status = 200, body = SecretCleanupView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn delete_unreferenced_secrets(
    State(state): State<WebState>,
) -> Result<Json<SecretCleanupView>, ApiError> {
    let result = state
        .cluster
        .apply(Command::DeleteUnreferencedSecrets)
        .await?;
    let CommandResult::UnreferencedSecretsDeleted(ids) = result else {
        unreachable!("cleanup command returns cleanup result");
    };
    Ok(Json(SecretCleanupView {
        deleted_ids: ids.into_iter().map(|id| id.0).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/alerts",
    params(AlertFilters),
    responses(
        (status = 200, body = [AlertView]),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_alerts(
    State(state): State<WebState>,
    Query(filters): Query<AlertFilters>,
) -> Result<Json<Vec<AlertView>>, ApiError> {
    let limit = filters.limit.unwrap_or(500);
    if !(1..=500).contains(&limit) {
        return Err(ApiError::bad_request(
            "alert limit must be between 1 and 500",
        ));
    }
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let mut alerts = snapshot
        .alerts
        .values()
        .filter(|alert| {
            filters
                .target_id
                .is_none_or(|id| alert.id.target_id.0 == id)
                && filters
                    .channel_id
                    .is_none_or(|id| alert.id.channel_id.0 == id)
                && filters.kind.is_none_or(|kind| alert.id.kind == kind.into())
                && filters.delivery.is_none_or(|delivery| {
                    matches!(
                        (&alert.delivery, delivery),
                        (AlertDelivery::Pending { .. }, AlertDeliveryParam::Pending)
                            | (
                                AlertDelivery::Delivered { .. },
                                AlertDeliveryParam::Delivered
                            )
                            | (AlertDelivery::Failed { .. }, AlertDeliveryParam::Failed)
                    )
                })
                && filters.acknowledged.is_none_or(|acknowledged| {
                    snapshot.alert_acknowledgements.contains_key(&alert.id) == acknowledged
                })
                && filters
                    .from_ms
                    .is_none_or(|from_ms| alert.id.evaluation_scheduled_at_ms >= from_ms)
                && filters
                    .to_ms
                    .is_none_or(|to_ms| alert.id.evaluation_scheduled_at_ms <= to_ms)
        })
        .collect::<Vec<_>>();
    alerts.sort_unstable_by(|left, right| {
        right
            .id
            .evaluation_scheduled_at_ms
            .cmp(&left.id.evaluation_scheduled_at_ms)
            .then_with(|| right.id.cmp(&left.id))
    });
    let alerts = alerts
        .into_iter()
        .take(limit)
        .map(|alert| {
            let (delivery, attempts, next_attempt_at_ms, completed_at_ms, diagnostic) =
                match &alert.delivery {
                    AlertDelivery::Pending {
                        attempts,
                        next_attempt_at_ms,
                    } => ("pending", *attempts, Some(*next_attempt_at_ms), None, None),
                    AlertDelivery::Delivered { delivered_at_ms } => {
                        ("delivered", 0, None, Some(*delivered_at_ms), None)
                    }
                    AlertDelivery::Failed {
                        failed_at_ms,
                        diagnostic,
                    } => (
                        "failed",
                        0,
                        None,
                        Some(*failed_at_ms),
                        Some(diagnostic.clone()),
                    ),
                };
            AlertView {
                target_id: alert.id.target_id.0,
                channel_id: alert.id.channel_id.0,
                kind: match alert.id.kind {
                    AlertKind::Down => "down",
                    AlertKind::Recovered => "recovered",
                }
                .to_owned(),
                target_name: alert.target_name.clone(),
                channel_name: snapshot
                    .notification_channels
                    .get(&alert.id.channel_id)
                    .map_or_else(
                        || "Deleted channel".to_owned(),
                        |channel| channel.name.clone(),
                    ),
                scheduled_at_ms: alert.id.evaluation_scheduled_at_ms,
                delivery: delivery.to_owned(),
                attempts,
                next_attempt_at_ms,
                completed_at_ms,
                diagnostic,
                acknowledged_at_ms: snapshot.alert_acknowledgements.get(&alert.id).copied(),
            }
        })
        .collect();
    Ok(Json(alerts))
}

#[utoipa::path(
    post,
    path = "/api/v1/alerts/acknowledge",
    request_body = AlertActionRequest,
    responses(
        (status = 204),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn acknowledge_alert(
    State(state): State<WebState>,
    Json(input): Json<AlertActionRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .cluster
        .apply(Command::AcknowledgeAlert {
            alert_id: input.alert_id(),
            acknowledged_at_ms: now_ms(),
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/api/v1/alerts/retry",
    request_body = AlertActionRequest,
    responses(
        (status = 204),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn retry_alert(
    State(state): State<WebState>,
    Json(input): Json<AlertActionRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .cluster
        .apply(Command::RetryAlert {
            alert_id: input.alert_id(),
            retry_at_ms: now_ms(),
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/v1/availability-transitions",
    responses(
        (status = 200, body = [AvailabilityTransitionView]),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_availability_transitions(
    State(state): State<WebState>,
) -> Result<Json<Vec<AvailabilityTransitionView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let mut transitions = snapshot
        .availability_transitions
        .values()
        .map(|transition| AvailabilityTransitionView {
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
    let mut active_assignments = BTreeMap::<Uuid, usize>::new();
    for assignment in snapshot.assignments.values() {
        *active_assignments
            .entry(assignment.executor_node_id)
            .or_default() += 1;
    }
    let connectivity_failures = snapshot
        .connectivity_failures
        .iter()
        .map(|route| DirectedRouteView {
            source_node_id: route.source,
            destination_node_id: route.destination,
        })
        .collect::<Vec<_>>();
    Ok(Json(ClusterView {
        leader_node_id: status.leader_node_id,
        local_node_id: status.local_node_id,
        degraded: snapshot.connectivity_degraded(),
        connectivity_failures,
        members: status
            .member_ids
            .into_iter()
            .map(|id| ClusterMemberView {
                id,
                name: snapshot
                    .node_names
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| upgrid_config::friendly_node_name(id)),
                reachable_addresses: snapshot
                    .node_reachability
                    .get(&id)
                    .map(|reachability| {
                        reachability
                            .reachable(status.local_node_id, upgrid_config::now_ms())
                            .into_iter()
                            .map(|address| address.to_string())
                            .collect()
                    })
                    .unwrap_or_default(),
                leader: status.leader_node_id == Some(id),
                local: status.local_node_id == id,
                draining: snapshot.draining_nodes.contains(&id),
                active_assignments: active_assignments.get(&id).copied().unwrap_or_default(),
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
    let (mut sender, stream) = futures_channel::mpsc::channel(1);
    compio::runtime::spawn(async move {
        if sender
            .send(Ok(Event::default()
                .event("state")
                .data(initial.to_string())))
            .await
            .is_err()
        {
            return;
        }
        loop {
            let event =
                match compio::time::timeout(Duration::from_secs(15), receiver.changed()).await {
                    Ok(Ok(())) => {
                        let version = *receiver.borrow_and_update();
                        Event::default().event("state").data(version.to_string())
                    }
                    Ok(Err(_)) => break,
                    Err(_) => Event::default().comment("keep-alive"),
                };
            if sender.send(Ok(event)).await.is_err() {
                break;
            }
        }
    })
    .detach();
    Sse::new(stream)
}
