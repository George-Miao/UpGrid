use std::collections::{BTreeMap, BTreeSet};

use url::Url;
use uuid::Uuid;

use super::{
    AlertKind, ApplicationState, CommandResult, Evaluation, EvaluationId, HttpEvaluationMetadata,
    TargetId,
};
use crate::{DirectedRoute, ReachableAddressLease};

pub(crate) const CONNECTIVITY_FAILURE_THRESHOLD: u8 = 3;

impl ApplicationState {
    pub(super) fn record_connectivity(
        &mut self,
        leases: Vec<ReachableAddressLease>,
        verified: Option<BTreeMap<Uuid, BTreeSet<crate::ReachableAddress>>>,
        checked_at_ms: u64,
        failures: BTreeSet<DirectedRoute>,
    ) -> Result<CommandResult, super::DomainError> {
        let legacy = verified.is_none();
        let mut renewed = BTreeMap::<Uuid, BTreeSet<_>>::new();
        for lease in leases {
            let reachability = self.node_reachability.entry(lease.node_id).or_default();
            reachability.expire(lease.discovered_at_ms);
            if legacy {
                renewed
                    .entry(lease.node_id)
                    .or_default()
                    .insert(lease.address.clone());
            }
            reachability.renew(
                lease.address,
                lease.source,
                lease.discovered_at_ms,
                lease.expires_at_ms,
            );
        }
        for (node_id, addresses) in verified.unwrap_or(renewed) {
            let reachability = self.node_reachability.entry(node_id).or_default();
            for address in addresses {
                reachability.verify(&address, checked_at_ms);
            }
        }
        for reachability in self.node_reachability.values_mut() {
            reachability.expire(checked_at_ms);
        }

        let same_scan = self.connectivity_failure_counts.len() == failures.len()
            && failures
                .iter()
                .all(|route| self.connectivity_failure_counts.contains_key(route));
        if same_scan {
            if failures.is_empty() {
                self.connectivity_success_count = self
                    .connectivity_success_count
                    .saturating_add(1)
                    .min(CONNECTIVITY_FAILURE_THRESHOLD);
            } else {
                for count in self.connectivity_failure_counts.values_mut() {
                    *count = count.saturating_add(1);
                }
                self.connectivity_success_count = 0;
            }
        } else {
            self.connectivity_failure_counts = failures
                .iter()
                .map(|route| (*route, 1))
                .collect::<BTreeMap<_, _>>();
            self.connectivity_success_count = u8::from(failures.is_empty());
        }

        let scan_count = if failures.is_empty() {
            self.connectivity_success_count
        } else {
            self.connectivity_failure_counts
                .values()
                .next()
                .copied()
                .unwrap_or_default()
        };
        if scan_count >= CONNECTIVITY_FAILURE_THRESHOLD {
            let was_degraded = self.connectivity_degraded();
            self.connectivity_failures = failures;
            let degraded = !self.connectivity_failures.is_empty();
            self.connectivity_degraded = Some(degraded);
            if !was_degraded && degraded {
                self.record_connectivity_alert(AlertKind::Down, checked_at_ms);
            } else if was_degraded && !degraded {
                self.record_connectivity_alert(AlertKind::Recovered, checked_at_ms);
            }
        }
        Ok(CommandResult::ConnectivityRecorded)
    }

    pub(crate) fn connectivity_scan_requires_record(
        &self,
        failures: &BTreeSet<DirectedRoute>,
        now_ms: u64,
    ) -> bool {
        if self
            .node_reachability
            .values()
            .any(|reachability| reachability.has_expired_lease(now_ms))
        {
            return true;
        }
        let same_scan = self.connectivity_failure_counts.len() == failures.len()
            && failures
                .iter()
                .all(|route| self.connectivity_failure_counts.contains_key(route));
        if !same_scan {
            return true;
        }
        if failures.is_empty() {
            self.connectivity_degraded()
                && self.connectivity_success_count < CONNECTIVITY_FAILURE_THRESHOLD
        } else {
            self.connectivity_failure_counts
                .values()
                .next()
                .is_none_or(|count| *count < CONNECTIVITY_FAILURE_THRESHOLD)
        }
    }

    pub(crate) fn reset_connectivity_scan(&mut self) {
        self.connectivity_failure_counts.clear();
        self.connectivity_degraded =
            Some(self.connectivity_degraded() || !self.connectivity_failures.is_empty());
        self.connectivity_success_count = 0;
    }

    #[cfg(test)]
    pub(crate) fn connectivity_route_state(
        &self,
        node_id: Uuid,
    ) -> (
        BTreeSet<DirectedRoute>,
        std::collections::BTreeMap<DirectedRoute, u8>,
    ) {
        let involves_node =
            |route: &&DirectedRoute| route.source == node_id || route.destination == node_id;
        let failures = self
            .connectivity_failures
            .iter()
            .filter(involves_node)
            .copied()
            .collect();
        let counts = self
            .connectivity_failure_counts
            .iter()
            .filter(|(route, _)| route.source == node_id || route.destination == node_id)
            .map(|(route, count)| (*route, *count))
            .collect();
        (failures, counts)
    }

    fn record_connectivity_alert(&mut self, kind: AlertKind, recorded_at_ms: u64) {
        let target_id = TargetId(Uuid::nil());
        let next_scheduled_at_ms = self
            .availability_transitions
            .keys()
            .rfind(|id| id.target_id == target_id)
            .map(|id| id.scheduled_at_ms.saturating_add(1))
            .unwrap_or(recorded_at_ms);
        let first_scheduled_at_ms = recorded_at_ms.max(next_scheduled_at_ms);
        let evaluation_id = (first_scheduled_at_ms..=u64::MAX)
            .chain(0..first_scheduled_at_ms)
            .map(|scheduled_at_ms| EvaluationId {
                target_id,
                scheduled_at_ms,
            })
            .find(|id| !self.availability_transitions.contains_key(id))
            .expect("the cluster alert evaluation ID space is exhausted");
        let target_url = Url::parse("up://cluster:1").expect("the cluster alert URL is static");
        let succeeded = kind == AlertKind::Recovered;
        let diagnostic = (!succeeded).then(|| {
            format!(
                "{} directed cluster route(s) are unavailable",
                self.connectivity_failures.len()
            )
        });
        let evaluation = Evaluation {
            id: evaluation_id,
            recorded_at_ms: evaluation_id.scheduled_at_ms,
            executor_node_id: Uuid::nil(),
            succeeded,
            http: HttpEvaluationMetadata {
                status_code: None,
                latency_ms: 0,
                received_bytes: 0,
                final_url: target_url.clone(),
            },
            diagnostic,
        };
        self.record_availability_transition(
            Some(kind),
            "Cluster connectivity".to_owned(),
            target_url,
            evaluation,
            self.notification_channels.keys().copied().collect(),
        );
    }
}

#[cfg(test)]
mod tests;
