use axum::extract::Request;
use axum::middleware::Next;

use super::*;

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct StatusView {
    targets: Vec<StatusTargetView>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct StatusTargetView {
    kind: TargetKindView,
    name: String,
    availability: String,
    consecutive_failures: u32,
    latest_evaluation: Option<StatusEvaluationView>,
    paused: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct StatusEvaluationView {
    scheduled_at_ms: u64,
    succeeded: bool,
    status_code: Option<u16>,
    latency_ms: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ManageSettingsView {
    public_status_enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct UpdateManageSettingsRequest {
    public_status_enabled: bool,
}

pub(super) async fn require_public_status_enabled(
    State(state): State<WebState>,
    request: Request,
    next: Next,
) -> Response {
    match state.cluster.read().await {
        Ok(application) if application.public_status_enabled => next.run(request).await,
        Ok(_) => ApiError::unauthorized().into_response(),
        Err(error) => ApiError::unavailable(error).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/status",
    responses(
        (status = 200, body = StatusView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    ),
    security()
)]
pub(super) async fn get_status(
    State(state): State<WebState>,
) -> Result<Json<StatusView>, ApiError> {
    let application = state.cluster.read().await.map_err(ApiError::unavailable)?;
    if !application.public_status_enabled {
        return Err(ApiError::unauthorized());
    }
    Ok(Json(StatusView::from(&application)))
}

#[utoipa::path(
    get,
    path = "/api/v1/settings",
    responses(
        (status = 200, body = ManageSettingsView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn get_settings(
    State(state): State<WebState>,
) -> Result<Json<ManageSettingsView>, ApiError> {
    let application = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(ManageSettingsView::from(&application)))
}

#[utoipa::path(
    put,
    path = "/api/v1/settings",
    request_body = UpdateManageSettingsRequest,
    responses(
        (status = 200, body = ManageSettingsView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn update_settings(
    State(state): State<WebState>,
    Json(input): Json<UpdateManageSettingsRequest>,
) -> Result<Json<ManageSettingsView>, ApiError> {
    state
        .cluster
        .apply(Command::SetPublicStatusEnabled {
            enabled: input.public_status_enabled,
        })
        .await?;
    let application = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(Json(ManageSettingsView::from(&application)))
}

impl From<&ApplicationState> for StatusView {
    fn from(application: &ApplicationState) -> Self {
        Self {
            targets: application
                .node_targets
                .values()
                .map(StatusTargetView::from_node)
                .chain(
                    application
                        .targets
                        .values()
                        .map(StatusTargetView::from_state),
                )
                .collect(),
        }
    }
}

impl StatusTargetView {
    fn from_state(state: &TargetState) -> Self {
        Self {
            kind: state.target.kind().into(),
            name: state.target.name.clone(),
            availability: availability_name(state.availability).to_owned(),
            consecutive_failures: state.consecutive_failures,
            latest_evaluation: state
                .latest_evaluation
                .as_ref()
                .map(StatusEvaluationView::from),
            paused: state.paused,
        }
    }

    fn from_node(state: &NodeTargetState) -> Self {
        Self {
            kind: TargetKindView::Node,
            name: state.target.name.clone(),
            availability: availability_name(state.availability).to_owned(),
            consecutive_failures: state.consecutive_failures,
            latest_evaluation: state
                .latest_evaluation
                .as_ref()
                .map(StatusEvaluationView::from),
            paused: false,
        }
    }
}

impl From<&ApplicationState> for ManageSettingsView {
    fn from(application: &ApplicationState) -> Self {
        Self {
            public_status_enabled: application.public_status_enabled,
        }
    }
}

impl From<&upgrid_raft::domain::Evaluation> for StatusEvaluationView {
    fn from(value: &upgrid_raft::domain::Evaluation) -> Self {
        Self {
            scheduled_at_ms: value.id.scheduled_at_ms,
            succeeded: value.succeeded,
            status_code: value.http.status_code,
            latency_ms: value.http.latency_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn public_status_serializes_only_safe_fields() {
        let status = StatusView {
            targets: vec![StatusTargetView {
                kind: TargetKindView::Http,
                name: "Public endpoint".to_owned(),
                availability: "up".to_owned(),
                consecutive_failures: 0,
                latest_evaluation: Some(StatusEvaluationView {
                    scheduled_at_ms: 42,
                    succeeded: true,
                    status_code: Some(204),
                    latency_ms: 17,
                }),
                paused: false,
            }],
        };

        assert_eq!(
            serde_json::to_value(status).unwrap(),
            json!({
                "targets": [{
                    "kind": "http",
                    "name": "Public endpoint",
                    "availability": "up",
                    "consecutive_failures": 0,
                    "latest_evaluation": {
                        "scheduled_at_ms": 42,
                        "succeeded": true,
                        "status_code": 204,
                        "latency_ms": 17
                    },
                    "paused": false
                }]
            }),
        );
    }
}
