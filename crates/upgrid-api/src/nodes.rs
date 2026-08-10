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
    if !status.members.contains_key(&id) {
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
        .expect("renamed Node has a replicated name");
    Ok(Json(NodeNameView { id, name }))
}
