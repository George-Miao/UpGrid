use base64::Engine as _;

use super::*;

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
                remaining_uses: snapshot.join_token_uses.get(hash).copied(),
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
    if input.expires_in_seconds == 0 {
        return Err(ApiError::bad_request(
            "join token lifetime must be positive",
        ));
    }
    if input.max_uses == Some(0) {
        return Err(ApiError::bad_request(
            "join token usage limit must be positive",
        ));
    }
    let token = generate_join_token().map_err(ApiError::unavailable)?;
    let hash = hash_join_token(&token);
    let link = JoinLink::issue(&state.raft_url, &state.cipher, &state.quic_ca_key, token)
        .map_err(ApiError::bad_request)?;
    let expires_at_ms = input
        .expires_in_seconds
        .checked_mul(1_000)
        .and_then(|duration| now_ms().checked_add(duration))
        .ok_or_else(|| ApiError::bad_request("join token lifetime is too large"))?;
    let command = match input.max_uses {
        Some(uses) => Command::PutLimitedJoinToken {
            hash,
            expires_at_ms,
            uses,
        },
        None => Command::PutJoinToken {
            hash,
            expires_at_ms,
        },
    };
    state.cluster.apply(command).await?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedJoinTokenView {
            id: encode_join_token_id(&hash),
            url: link.to_string(),
            expires_at_ms,
            remaining_uses: input.max_uses,
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

#[utoipa::path(
    get,
    path = "/api/v1/setup",
    responses(
        (status = 200, body = SetupView),
        (status = 401, body = ErrorBody),
    )
)]
pub(super) async fn get_setup(State(state): State<WebState>) -> Result<Json<SetupView>, ApiError> {
    let snapshot = state
        .cluster
        .local_read()
        .await
        .map_err(ApiError::unavailable)?;
    Ok(Json(setup_view(
        &state,
        snapshot.notification_channels.len(),
        snapshot.targets.len(),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/setup/next",
    responses(
        (status = 200, body = SetupView),
        (status = 401, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn advance_setup(
    State(state): State<WebState>,
) -> Result<Json<SetupView>, ApiError> {
    state.oobe.advance().map_err(ApiError::unavailable)?;
    get_setup(State(state)).await
}

#[utoipa::path(
    post,
    path = "/api/v1/cluster/join",
    request_body = JoinClusterRequest,
    responses(
        (status = 202, body = JoinClusterView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 409, body = ErrorBody),
    )
)]
pub(super) async fn join_cluster(
    Json(_input): Json<JoinClusterRequest>,
) -> Result<(StatusCode, Json<JoinClusterView>), ApiError> {
    Err(ApiError {
        status: StatusCode::CONFLICT,
        message: "This node already belongs to a cluster".to_owned(),
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/setup/new-cluster",
    request_body = CreateClusterRequest,
    responses(
        (status = 202, body = JoinClusterView),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 409, body = ErrorBody),
    )
)]
pub(super) async fn create_cluster(
    Json(_input): Json<CreateClusterRequest>,
) -> Result<(StatusCode, Json<JoinClusterView>), ApiError> {
    Err(ApiError {
        status: StatusCode::CONFLICT,
        message: "This node already belongs to a cluster".to_owned(),
    })
}

fn setup_view(state: &WebState, channel_count: usize, target_count: usize) -> SetupView {
    let phase = state.oobe.phase();
    SetupView {
        setup: phase != OobePhase::Complete,
        phase: phase.into(),
        path: phase.path().to_owned(),
        cluster_ready: true,
        node_name: state.node_name.clone(),
        warning: state.startup_warning.clone(),
        channel_count,
        target_count,
    }
}

fn encode_join_token_id(hash: &upgrid_raft::domain::JoinTokenHash) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash.0)
}

fn decode_join_token_id(id: &str) -> Result<upgrid_raft::domain::JoinTokenHash, ApiError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(id)
        .map_err(|_| ApiError::bad_request("Invalid join token ID"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| ApiError::bad_request("Invalid join token ID"))?;
    Ok(upgrid_raft::domain::JoinTokenHash(bytes))
}
