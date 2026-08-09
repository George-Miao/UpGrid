use super::*;

#[utoipa::path(
    get,
    path = "/api/v1/targets",
    responses(
        (status = 200, body = [TargetView]),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn list_targets(
    State(state): State<WebState>,
) -> Result<Json<Vec<TargetView>>, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot
            .targets
            .values()
            .map(|target| TargetView::from_state(&snapshot, target))
            .collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/targets/{id}",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, body = TargetView),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn get_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<Json<TargetView>, ApiError> {
    let id = TargetId(id);
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    snapshot
        .targets
        .get(&id)
        .map(|target| TargetView::from_state(&snapshot, target))
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("target not found: {}", id.0)))
}

#[utoipa::path(
    post,
    path = "/api/v1/targets",
    request_body = PutTargetRequest,
    responses(
        (status = 201, body = TargetView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 409, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn create_target(
    State(state): State<WebState>,
    Json(input): Json<PutTargetRequest>,
) -> Result<(StatusCode, Json<TargetView>), ApiError> {
    let id = TargetId(Uuid::now_v7());
    let use_default_channels = input.use_default_channels;
    let target = target_from_input(id, input)?;
    state.cluster.apply(Command::CreateTarget(target)).await?;
    if !use_default_channels {
        state
            .cluster
            .apply(Command::SetTargetDefaultNotifications {
                target_id: id,
                enabled: false,
            })
            .await?;
    }
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let view = snapshot
        .targets
        .get(&id)
        .map(|target| TargetView::from_state(&snapshot, target))
        .expect("created target exists");
    Ok((StatusCode::CREATED, Json(view)))
}

#[utoipa::path(
    put,
    path = "/api/v1/targets/{id}",
    params(("id" = Uuid, Path)),
    request_body = PutTargetRequest,
    responses(
        (status = 200, body = TargetView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn update_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Json(input): Json<PutTargetRequest>,
) -> Result<Json<TargetView>, ApiError> {
    let id = TargetId(id);
    let use_default_channels = input.use_default_channels;
    let target = target_from_input(id, input)?;
    state.cluster.apply(Command::UpdateTarget(target)).await?;
    state
        .cluster
        .apply(Command::SetTargetDefaultNotifications {
            target_id: id,
            enabled: use_default_channels,
        })
        .await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(
        snapshot
            .targets
            .get(&id)
            .map(|target| TargetView::from_state(&snapshot, target))
            .expect("updated target exists"),
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/targets/{id}",
    params(("id" = Uuid, Path)),
    responses(
        (status = 204),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn delete_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state
        .cluster
        .apply(Command::DeleteTarget(TargetId(id)))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/api/v1/targets/{id}/pause",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, body = TargetView),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn pause_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<Json<TargetView>, ApiError> {
    set_target_paused(&state, TargetId(id), true).await
}

#[utoipa::path(
    post,
    path = "/api/v1/targets/{id}/resume",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, body = TargetView),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn resume_target(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
) -> Result<Json<TargetView>, ApiError> {
    set_target_paused(&state, TargetId(id), false).await
}

async fn set_target_paused(
    state: &WebState,
    id: TargetId,
    paused: bool,
) -> Result<Json<TargetView>, ApiError> {
    state
        .cluster
        .apply(Command::SetTargetPaused {
            target_id: id,
            paused,
        })
        .await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    snapshot
        .targets
        .get(&id)
        .map(|target| TargetView::from_state(&snapshot, target))
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("target not found: {}", id.0)))
}

pub(super) fn target_from_input(id: TargetId, input: PutTargetRequest) -> Result<Target, ApiError> {
    let url = Url::parse(&input.url).map_err(ApiError::bad_request)?;
    Ok(Target {
        id,
        name: input.name,
        http: HttpTarget {
            url,
            method: input.method,
            headers: input
                .headers
                .into_iter()
                .map(|(key, value)| (key, ConfigValue::from(value)))
                .collect(),
            body: input.body.map(ConfigValue::from),
            accepted_statuses: input
                .accepted_statuses
                .into_iter()
                .map(|range| StatusRange::new(range.start, range.end))
                .collect(),
            follow_redirects: input.follow_redirects,
            max_redirects: input.max_redirects,
            body_contains: input.body_contains,
            skip_tls_verification: input.skip_tls_verification,
        },
        policy: EvaluationPolicy {
            interval_ms: input.interval_seconds.saturating_mul(1_000),
            timeout_ms: input.timeout_seconds.saturating_mul(1_000),
            failure_threshold: input.failure_threshold,
        },
        notification_channels: input
            .notification_channel_ids
            .into_iter()
            .map(NotificationChannelId)
            .collect(),
    })
}
