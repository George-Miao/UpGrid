use std::collections::{BTreeMap, BTreeSet};

use url::Url;
use uuid::Uuid;

use super::*;

impl ApplicationState {
    pub(super) fn record_evaluation(
        &mut self,
        evaluation: Evaluation,
    ) -> Result<CommandResult, DomainError> {
        evaluation.validate()?;
        let id = evaluation.id;
        let Some(target) = self.targets.get(&id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if target.paused || duplicate(&target.history, &target.latest_evaluation, &evaluation) {
            self.discard_evaluation_batch(id);
            return Ok(CommandResult::EvaluationDiscarded);
        }

        let evaluation = if self.evaluation_batches.contains_key(&id) {
            let key = EvaluationAssignmentKey {
                id,
                executor_node_id: evaluation.executor_node_id,
            };
            if self.assignments.remove(&key).is_none() {
                return Ok(CommandResult::EvaluationDiscarded);
            }
            let batch = self
                .evaluation_batches
                .get_mut(&id)
                .expect("checked evaluation batch must exist");
            if batch
                .results
                .insert(evaluation.executor_node_id, evaluation)
                .is_some()
            {
                return Ok(CommandResult::EvaluationDiscarded);
            }
            if batch.results.len() < usize::from(batch.expected_results) {
                return Ok(CommandResult::EvaluationPending(id));
            }
            let batch = self
                .evaluation_batches
                .remove(&id)
                .expect("complete evaluation batch must exist");
            self.assignments.retain(|key, _| key.id != id);
            aggregate(batch.results)
        } else {
            self.assignments.remove(&EvaluationAssignmentKey {
                id,
                executor_node_id: evaluation.executor_node_id,
            });
            evaluation
        };

        let target = self
            .targets
            .get_mut(&id.target_id)
            .expect("validated evaluation Target must exist");
        let transition = update_availability(
            &mut target.availability,
            &mut target.consecutive_failures,
            target.target.policy.failure_threshold,
            evaluation.succeeded,
        );
        let mut channels = target.target.notification_channels.clone();
        if !self.default_notifications_disabled.contains(&id.target_id) {
            channels.extend(self.default_notification_channels.iter().copied());
        }
        let name = target.target.name.clone();
        let url = target.target.http.url.clone();
        let availability = target.availability;
        retain_evaluation(
            &mut target.latest_evaluation,
            &mut target.history,
            &evaluation,
            self.history_retention_ms,
        );
        let alerts = self.record_transition(transition, name, url, evaluation, channels);
        Ok(CommandResult::EvaluationAccepted {
            availability,
            alerts,
        })
    }

    fn discard_evaluation_batch(&mut self, id: EvaluationId) {
        self.assignments.retain(|key, _| key.id != id);
        self.evaluation_batches.remove(&id);
    }

    pub(super) fn record_node_evaluation(
        &mut self,
        evaluation: Evaluation,
    ) -> Result<CommandResult, DomainError> {
        evaluation.validate()?;
        let Some(target) = self.node_targets.get_mut(&evaluation.id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if duplicate(&target.history, &target.latest_evaluation, &evaluation) {
            return Ok(CommandResult::EvaluationDiscarded);
        }

        let transition = update_availability(
            &mut target.availability,
            &mut target.consecutive_failures,
            target.target.policy.failure_threshold,
            evaluation.succeeded,
        );
        let name = target.target.name.clone();
        let url = target.target.url.clone();
        let availability = target.availability;
        retain_evaluation(
            &mut target.latest_evaluation,
            &mut target.history,
            &evaluation,
            self.history_retention_ms,
        );
        let channels = self.default_notification_channels.clone();
        let alerts = self.record_transition(transition, name, url, evaluation, channels);
        Ok(CommandResult::NodeEvaluationAccepted {
            availability,
            alerts,
        })
    }

    fn record_transition(
        &mut self,
        transition: Option<AlertKind>,
        target_name: String,
        target_url: Url,
        evaluation: Evaluation,
        channels: BTreeSet<NotificationChannelId>,
    ) -> Vec<AlertId> {
        let cutoff = evaluation
            .recorded_at_ms
            .saturating_sub(self.history_retention_ms);
        self.transitions
            .retain(|_, item| item.evaluation.recorded_at_ms >= cutoff);
        let Some(kind) = transition else {
            return Vec::new();
        };
        self.transitions
            .entry(evaluation.id)
            .or_insert_with(|| AvailabilityTransition {
                kind,
                target_name: target_name.clone(),
                target_url: target_url.clone(),
                evaluation: evaluation.clone(),
            });
        channels
            .into_iter()
            .map(|channel_id| {
                let id = AlertId {
                    target_id: evaluation.id.target_id,
                    channel_id,
                    evaluation_scheduled_at_ms: evaluation.id.scheduled_at_ms,
                    kind,
                };
                self.alerts.entry(id).or_insert_with(|| Alert {
                    id,
                    target_name: target_name.clone(),
                    target_url: target_url.clone(),
                    evaluation: evaluation.clone(),
                    delivery: AlertDelivery::Pending {
                        attempts: 0,
                        next_attempt_at_ms: evaluation.recorded_at_ms,
                    },
                });
                id
            })
            .collect()
    }
}

fn aggregate(results: BTreeMap<Uuid, Evaluation>) -> Evaluation {
    let evaluations = results.into_values().collect::<Vec<_>>();
    let representative = evaluations
        .iter()
        .position(|evaluation| !evaluation.succeeded)
        .unwrap_or(0);
    let mut aggregate = evaluations[representative].clone();
    aggregate.recorded_at_ms = evaluations
        .iter()
        .map(|evaluation| evaluation.recorded_at_ms)
        .max()
        .unwrap_or(aggregate.recorded_at_ms);
    aggregate.http.latency_ms = evaluations
        .iter()
        .map(|evaluation| evaluation.http.latency_ms)
        .max()
        .unwrap_or(aggregate.http.latency_ms);
    aggregate.http.received_bytes = evaluations
        .iter()
        .map(|evaluation| evaluation.http.received_bytes)
        .fold(0_u64, u64::saturating_add);
    let failed = evaluations
        .iter()
        .filter(|evaluation| !evaluation.succeeded)
        .collect::<Vec<_>>();
    aggregate.succeeded = failed.is_empty();
    aggregate.diagnostic = (!failed.is_empty()).then(|| {
        let details = failed
            .iter()
            .map(|evaluation| {
                format!(
                    "{}: {}",
                    evaluation.executor_node_id,
                    evaluation
                        .diagnostic
                        .as_deref()
                        .unwrap_or("probe requirements were not met"),
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        truncate_diagnostic(format!(
            "{}/{} locations failed: {details}",
            failed.len(),
            evaluations.len(),
        ))
    });
    aggregate
}

fn truncate_diagnostic(value: String) -> String {
    if value.len() <= MAX_DIAGNOSTIC_BYTES {
        return value;
    }
    let mut end = MAX_DIAGNOSTIC_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn duplicate(
    history: &BTreeMap<u64, Evaluation>,
    latest: &Option<Evaluation>,
    evaluation: &Evaluation,
) -> bool {
    history.contains_key(&evaluation.id.scheduled_at_ms)
        || latest
            .as_ref()
            .is_some_and(|latest| latest.id.scheduled_at_ms >= evaluation.id.scheduled_at_ms)
}

fn update_availability(
    availability: &mut AvailabilityState,
    failures: &mut u32,
    threshold: u32,
    succeeded: bool,
) -> Option<AlertKind> {
    let previous = *availability;
    if succeeded {
        *failures = 0;
        *availability = AvailabilityState::Up;
    } else {
        *failures = failures.saturating_add(1);
        if *failures >= threshold {
            *availability = AvailabilityState::Down;
        }
    }
    match (previous, *availability) {
        (AvailabilityState::Down, AvailabilityState::Up) => Some(AlertKind::Recovered),
        (before, AvailabilityState::Down) if before != AvailabilityState::Down => {
            Some(AlertKind::Down)
        }
        _ => None,
    }
}

fn retain_evaluation(
    latest: &mut Option<Evaluation>,
    history: &mut BTreeMap<u64, Evaluation>,
    evaluation: &Evaluation,
    retention_ms: u64,
) {
    *latest = Some(evaluation.clone());
    history.insert(evaluation.id.scheduled_at_ms, evaluation.clone());
    let cutoff = evaluation.recorded_at_ms.saturating_sub(retention_ms);
    history.retain(|_, item| item.recorded_at_ms >= cutoff);
}
