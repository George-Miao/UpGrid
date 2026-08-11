use std::time::Duration;

use compio::time::sleep;
use snafu::{ResultExt, Snafu};
use upgrid_config::now_ms;
use upgrid_raft::domain::{
    Command, Evaluation, EvaluationId, EvaluationPolicy, HttpEvaluationMetadata, NodeTarget,
    TargetId,
};
use upgrid_raft::{ClusterError, Handle};
use url::Url;

use crate::scheduler::slot_at_or_before_ms;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("could not read replicated Node state: {source}"))]
    Read { source: ClusterError },

    #[snafu(display("could not read Cluster status: {source}"))]
    Status { source: ClusterError },

    #[snafu(display("invalid URL for Node {node_id}: {source}"))]
    NodeUrl {
        node_id: uuid::Uuid,
        source: url::ParseError,
    },

    #[snafu(display("could not update replicated Node state: {source}"))]
    Apply { source: ClusterError },
}

const INTERVAL_MS: u64 = 10_000;
const FAILURE_THRESHOLD: u32 = 3;

pub(super) async fn run(cluster: Handle) {
    loop {
        if let Err(error) = evaluate(&cluster).await {
            tracing::warn!(%error, "could not evaluate Cluster Nodes");
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn evaluate(cluster: &Handle) -> Result<(), Error> {
    if !cluster.is_leader().await {
        return Ok(());
    }
    let state = cluster.read().await.context(ReadSnafu)?;
    let status = cluster.status().await.context(StatusSnafu)?;
    let targets = status
        .members
        .iter()
        .map(|(node_id, url)| {
            Ok(NodeTarget {
                node_id: *node_id,
                name: state
                    .node_names
                    .get(node_id)
                    .cloned()
                    .unwrap_or_else(|| format!("Node {}", &node_id.to_string()[..8])),
                url: Url::parse(url).context(NodeUrlSnafu { node_id: *node_id })?,
                policy: EvaluationPolicy {
                    interval_ms: INTERVAL_MS,
                    timeout_ms: 2_000,
                    failure_threshold: FAILURE_THRESHOLD,
                },
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let current = state
        .node_targets
        .values()
        .map(|target| &target.target)
        .collect::<Vec<_>>();
    if current != targets.iter().collect::<Vec<_>>() {
        cluster
            .apply(Command::SyncNodeTargets(targets.clone()))
            .await
            .context(ApplySnafu)?;
    }

    let now = now_ms();
    for target in targets {
        let target_id = TargetId(target.node_id);
        let Some(scheduled_at_ms) = slot_at_or_before_ms(target_id, INTERVAL_MS, now) else {
            continue;
        };
        if state
            .node_targets
            .get(&target_id)
            .and_then(|target| target.latest_evaluation.as_ref())
            .is_some_and(|latest| latest.id.scheduled_at_ms >= scheduled_at_ms)
        {
            continue;
        }
        let started_at_ms = now_ms();
        let result = cluster
            .probe_node(target.node_id, target.url.to_string())
            .await;
        let recorded_at_ms = now_ms();
        let evaluation = Evaluation {
            id: EvaluationId {
                target_id,
                scheduled_at_ms,
            },
            recorded_at_ms,
            executor_node_id: cluster.node_id,
            succeeded: result.is_ok(),
            http: HttpEvaluationMetadata {
                status_code: None,
                latency_ms: recorded_at_ms.saturating_sub(started_at_ms),
                received_bytes: 0,
                final_url: target.url,
            },
            diagnostic: result.err().map(|error| error.to_string()),
        };
        cluster
            .apply(Command::RecordNodeEvaluation(evaluation))
            .await
            .context(ApplySnafu)?;
    }
    Ok(())
}
