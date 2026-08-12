use std::collections::BTreeMap;

use super::*;

const DEFAULT_HISTORY_RANGE_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_HISTORY_RANGE_MS: u64 = 366 * 24 * 60 * 60 * 1_000;
const DEFAULT_HISTORY_LIMIT: usize = 168;
const MAX_HISTORY_LIMIT: usize = 1_000;

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(super) struct HistoryQuery {
    from_ms: Option<u64>,
    to_ms: Option<u64>,
    cursor_ms: Option<u64>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct HistoryPage {
    items: Vec<EvaluationRollupView>,
    next_cursor_ms: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct EvaluationRollupView {
    bucket_start_ms: u64,
    bucket_end_ms: u64,
    samples: u64,
    successes: u64,
    failures: u64,
    availability_percent: f64,
    latency_total_ms: u64,
    latency_average_ms: u64,
    latency_min_ms: u64,
    latency_max_ms: u64,
}

impl From<&upgrid_raft::domain::EvaluationRollup> for EvaluationRollupView {
    fn from(rollup: &upgrid_raft::domain::EvaluationRollup) -> Self {
        Self {
            bucket_start_ms: rollup.bucket_start_ms,
            bucket_end_ms: rollup
                .bucket_start_ms
                .saturating_add(upgrid_raft::domain::HISTORY_ROLLUP_INTERVAL_MS),
            samples: rollup.samples,
            successes: rollup.successes,
            failures: rollup.failures,
            availability_percent: if rollup.samples == 0 {
                0.0
            } else {
                rollup.successes as f64 / rollup.samples as f64 * 100.0
            },
            latency_total_ms: rollup.latency_total_ms,
            latency_average_ms: rollup
                .latency_total_ms
                .checked_div(rollup.samples)
                .unwrap_or_default(),
            latency_min_ms: rollup.latency_min_ms,
            latency_max_ms: rollup.latency_max_ms,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/targets/{id}/history",
    params(("id" = Uuid, Path), HistoryQuery),
    responses(
        (status = 200, body = HistoryPage),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody),
    )
)]
pub(super) async fn get_target_history(
    State(state): State<WebState>,
    Path(id): Path<Uuid>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryPage>, ApiError> {
    let range = HistoryRange::try_from(query)?;
    let target_id = TargetId(id);
    let snapshot = state.cluster.read().await.map_err(ApiError::unavailable)?;
    if !snapshot.targets.contains_key(&target_id) && !snapshot.node_targets.contains_key(&target_id)
    {
        return Err(ApiError::not_found(format!("target not found: {id}")));
    }
    Ok(Json(page(snapshot.history_rollups.get(&target_id), range)))
}

#[derive(Debug, Clone, Copy)]
struct HistoryRange {
    from_ms: u64,
    to_ms: u64,
    cursor_ms: Option<u64>,
    limit: usize,
}

impl TryFrom<HistoryQuery> for HistoryRange {
    type Error = ApiError;

    fn try_from(query: HistoryQuery) -> Result<Self, Self::Error> {
        let to_ms = query.to_ms.unwrap_or_else(now_ms);
        let from_ms = query
            .from_ms
            .unwrap_or_else(|| to_ms.saturating_sub(DEFAULT_HISTORY_RANGE_MS));
        if from_ms > to_ms {
            return Err(ApiError::bad_request(
                "history from_ms must not exceed to_ms",
            ));
        }
        if to_ms - from_ms > MAX_HISTORY_RANGE_MS {
            return Err(ApiError::bad_request(
                "history time range must not exceed 366 days",
            ));
        }
        if query
            .cursor_ms
            .is_some_and(|cursor| cursor < from_ms || cursor > to_ms)
        {
            return Err(ApiError::bad_request(
                "history cursor_ms must fall within the requested time range",
            ));
        }
        let limit = query.limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
        if !(1..=MAX_HISTORY_LIMIT).contains(&limit) {
            return Err(ApiError::bad_request(
                "history limit must be between 1 and 1000",
            ));
        }
        Ok(Self {
            from_ms,
            to_ms,
            cursor_ms: query.cursor_ms,
            limit,
        })
    }
}

fn page(
    rollups: Option<&BTreeMap<u64, upgrid_raft::domain::EvaluationRollup>>,
    range: HistoryRange,
) -> HistoryPage {
    let Some(rollups) = rollups else {
        return HistoryPage {
            items: Vec::new(),
            next_cursor_ms: None,
        };
    };
    let mut items = rollups
        .range(range.from_ms..=range.to_ms)
        .filter(|(bucket_start_ms, _)| {
            range
                .cursor_ms
                .is_none_or(|cursor_ms| **bucket_start_ms > cursor_ms)
        })
        .take(range.limit + 1)
        .map(|(_, rollup)| EvaluationRollupView::from(rollup))
        .collect::<Vec<_>>();
    let has_more = items.len() > range.limit;
    if has_more {
        items.pop();
    }
    let next_cursor_ms = has_more.then(|| {
        items
            .last()
            .expect("a full history page contains one item")
            .bucket_start_ms
    });
    HistoryPage {
        items,
        next_cursor_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rollup(bucket_start_ms: u64) -> upgrid_raft::domain::EvaluationRollup {
        upgrid_raft::domain::EvaluationRollup {
            bucket_start_ms,
            samples: 1,
            successes: 1,
            failures: 0,
            latency_total_ms: 20,
            latency_min_ms: 20,
            latency_max_ms: 20,
        }
    }

    #[test]
    fn pages_history_chronologically_with_an_exclusive_cursor() {
        let rollups = [0, 1, 2]
            .into_iter()
            .map(|start| (start, rollup(start)))
            .collect();
        let first = page(
            Some(&rollups),
            HistoryRange {
                from_ms: 0,
                to_ms: 2,
                cursor_ms: None,
                limit: 2,
            },
        );
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.bucket_start_ms)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(first.next_cursor_ms, Some(1));

        let second = page(
            Some(&rollups),
            HistoryRange {
                from_ms: 0,
                to_ms: 2,
                cursor_ms: first.next_cursor_ms,
                limit: 2,
            },
        );
        assert_eq!(second.items[0].bucket_start_ms, 2);
        assert_eq!(second.next_cursor_ms, None);
    }

    #[test]
    fn rejects_unbounded_history_queries() {
        let result = HistoryRange::try_from(HistoryQuery {
            from_ms: Some(0),
            to_ms: Some(MAX_HISTORY_RANGE_MS + 1),
            cursor_ms: None,
            limit: Some(10),
        });
        assert!(result.is_err());
    }
}
