use std::collections::{BTreeMap, BTreeSet};

use url::Url;

use super::*;

impl ApplicationState {
    pub(super) fn record_evaluation(
        &mut self,
        evaluation: Evaluation,
    ) -> Result<CommandResult, DomainError> {
        evaluation.validate()?;
        self.assignments.remove(&evaluation.id);
        let Some(target) = self.targets.get_mut(&evaluation.id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if target.paused || duplicate(&target.history, &target.latest_evaluation, &evaluation) {
            return Ok(CommandResult::EvaluationDiscarded);
        }

        let transition = update_availability(
            &mut target.availability,
            &mut target.consecutive_failures,
            target.target.policy.failure_threshold,
            evaluation.succeeded,
        );
        let mut channels = target.target.notification_channels.clone();
        if !self
            .default_notifications_disabled
            .contains(&evaluation.id.target_id)
        {
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
