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
            .node_targets
            .values()
            .map(TargetView::from_node)
            .chain(
                snapshot
                    .targets
                    .values()
                    .map(|target| TargetView::from_state(&snapshot, target)),
            )
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
    state
        .cluster
        .apply(Command::CreateTarget {
            target,
            use_default_notifications: use_default_channels,
        })
        .await?;
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
    state
        .cluster
        .apply(Command::UpdateTarget {
            target,
            use_default_notifications: use_default_channels,
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
    let expected_kind = TargetKind::from(input.kind);
    let actual_kind = TargetKind::from_scheme(url.scheme());
    if actual_kind != Some(expected_kind) {
        return Err(ApiError::bad_request(format!(
            "target kind {} does not match URL scheme {}",
            expected_kind.as_str(),
            url.scheme()
        )));
    }
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

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::*;

    #[test]
    fn target_input_accepts_every_explicit_kind() {
        for (index, (kind, url, expected)) in [
            ("http", "https://example.com/health", TargetKind::Http),
            ("tcp", "tcp://database.internal:5432", TargetKind::Tcp),
            ("dns", "dns://service.internal", TargetKind::Dns),
            ("icmp", "icmp://192.0.2.1", TargetKind::Icmp),
            ("tls", "tls://example.com:443", TargetKind::Tls),
        ]
        .into_iter()
        .enumerate()
        {
            let input = serde_json::from_value(json!({
                "name": kind,
                "kind": kind,
                "url": url,
            }))
            .unwrap();

            let target = target_from_input(TargetId(Uuid::from_u128(index as u128)), input)
                .unwrap_or_else(|error| panic!("valid {kind} target rejected: {}", error.message));

            assert_eq!(target.kind(), expected);
        }
    }

    #[test]
    fn target_input_defaults_legacy_requests_to_http() {
        let input = serde_json::from_value(json!({
            "name": "Legacy",
            "url": "https://example.com/health",
        }))
        .unwrap();

        let target = target_from_input(TargetId(Uuid::from_u128(1)), input)
            .unwrap_or_else(|error| panic!("legacy HTTP target rejected: {}", error.message));

        assert_eq!(target.kind(), TargetKind::Http);
    }

    #[test]
    fn target_input_rejects_kind_and_scheme_mismatch() {
        let input = serde_json::from_value(json!({
            "name": "Database",
            "kind": "tcp",
            "url": "dns://database.internal",
        }))
        .unwrap();

        let error = target_from_input(TargetId(Uuid::from_u128(1)), input).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("target kind tcp does not match URL scheme dns")
        );
    }
}
