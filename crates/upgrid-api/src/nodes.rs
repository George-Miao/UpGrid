use super::*;

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct RenameNodeRequest {
    name: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct NodeNameView {
    id: Uuid,
    name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct SetNodeDrainRequest {
    draining: bool,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct RemoveNodeQuery {
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct NodeLifecycleView {
    id: Uuid,
    draining: bool,
    active_assignments: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct RemovedNodeView {
    id: Uuid,
    status: &'static str,
    replacement: &'static str,
}

#[utoipa::path(
    put,
    path = "/api/v1/nodes/{id}",
    params(("id" = Uuid, Path)),
    request_body = RenameNodeRequest,
    responses(
        (status = 200, body = NodeNameView),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn rename_node(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Json(input): Json<RenameNodeRequest>,
) -> Result<Json<NodeNameView>, ApiError> {
    let status = state
        .cluster
        .status()
        .await
        .map_err(ApiError::unavailable)?;
    if !status.member_ids.contains(&id) {
        return Err(ApiError::not_found(format!("Node not found: {id}")));
    }
    state
        .cluster
        .apply(Command::SetNodeName {
            node_id: id,
            name: input.name,
        })
        .await?;
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let name = snapshot
        .node_names
        .get(&id)
        .cloned()
        .expect("Renamed node has a replicated name");
    Ok(Json(NodeNameView { id, name }))
}

#[utoipa::path(
    put,
    path = "/api/v1/nodes/{id}/drain",
    params(("id" = Uuid, Path)),
    request_body = SetNodeDrainRequest,
    responses(
        (status = 200, body = NodeLifecycleView),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 422, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn set_node_drain(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Json(input): Json<SetNodeDrainRequest>,
) -> Result<Json<NodeLifecycleView>, ApiError> {
    require_member(&state, id).await?;
    state
        .cluster
        .apply(Command::SetNodeDraining {
            node_id: id,
            draining: input.draining,
            force: input.force,
        })
        .await?;
    node_lifecycle(&state, id).await.map(Json)
}

#[utoipa::path(
    delete,
    path = "/api/v1/nodes/{id}",
    params(
        ("id" = Uuid, Path),
        ("force" = bool, Query, description = "Release assignments and remove a failed node without waiting for drain completion"),
    ),
    responses(
        (status = 200, body = RemovedNodeView),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 409, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn remove_node(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Query(query): Query<RemoveNodeQuery>,
) -> Result<Json<RemovedNodeView>, ApiError> {
    let status = require_member(&state, id).await?;
    if status.local_node_id == id {
        return Err(conflict(
            "A node cannot remove itself; send this request to another cluster member",
        ));
    }
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    let active_assignments = snapshot
        .assignments
        .values()
        .filter(|assignment| assignment.executor_node_id == id)
        .count();
    if !query.force {
        if !snapshot.draining_nodes.contains(&id) {
            return Err(conflict("Drain the node before removing it"));
        }
        if active_assignments != 0 {
            return Err(conflict(format!(
                "Node still has {active_assignments} active evaluation assignments"
            )));
        }
    } else {
        state
            .cluster
            .apply(Command::SetNodeDraining {
                node_id: id,
                draining: true,
                force: true,
            })
            .await?;
    }
    state.cluster.remove_node(id).await?;
    if let Err(error) = state
        .cluster
        .apply(Command::SetNodeDraining {
            node_id: id,
            draining: false,
            force: false,
        })
        .await
    {
        tracing::warn!(%error, %id, "Could not clear removed node drain metadata");
    }
    Ok(Json(RemovedNodeView {
        id,
        status: "removed",
        replacement: "Create a one-use join token, start a fresh node, and join it to the cluster.",
    }))
}

async fn require_member(state: &WebState, id: Uuid) -> Result<upgrid_raft::Status, ApiError> {
    let status = state
        .cluster
        .status()
        .await
        .map_err(ApiError::unavailable)?;
    if !status.member_ids.contains(&id) {
        return Err(ApiError::not_found(format!("Node not found: {id}")));
    }
    Ok(status)
}

async fn node_lifecycle(state: &WebState, id: Uuid) -> Result<NodeLifecycleView, ApiError> {
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    Ok(NodeLifecycleView {
        id,
        draining: snapshot.draining_nodes.contains(&id),
        active_assignments: snapshot
            .assignments
            .values()
            .filter(|assignment| assignment.executor_node_id == id)
            .count(),
    })
}

fn conflict(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        message: message.into(),
    }
}
