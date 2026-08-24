use std::collections::BTreeSet;
use std::time::Duration;

use compio::time::sleep;
use upgrid_rpc::Context;

use super::deadline::before_deadline;
use super::{JoinError, UpgridServer};
use crate::ReachableAddress;
use crate::domain::Command;
use crate::rpc::runtime::ConnectivityReport;

fn verified_addresses(
    report: &ConnectivityReport,
) -> impl Iterator<Item = (uuid::Uuid, ReachableAddress)> + '_ {
    report.verified.iter().flat_map(|(node_id, addresses)| {
        addresses.iter().cloned().map(|address| (*node_id, address))
    })
}

impl UpgridServer {
    pub(super) async fn wait_for_connectivity(
        &self,
        context: &Context,
        nodes: &BTreeSet<uuid::Uuid>,
        reservation_id: uuid::Uuid,
        reservation_operation_id: uuid::Uuid,
    ) -> Result<(), JoinError> {
        let report = self
            .connectivity_report(context, nodes, reservation_id, reservation_operation_id)
            .await?;
        let failures = report.failures;
        if failures.is_empty() {
            Ok(())
        } else {
            Err(JoinError::Connectivity(failures.into_iter().collect()))
        }
    }

    pub(super) async fn connectivity_report(
        &self,
        context: &Context,
        nodes: &BTreeSet<uuid::Uuid>,
        reservation_id: uuid::Uuid,
        reservation_operation_id: uuid::Uuid,
    ) -> Result<ConnectivityReport, JoinError> {
        let mut final_report = None;
        for attempt in 0..3 {
            let report = before_deadline(context, self.rpc.admission_connectivity(nodes)).await?;
            let (admission_leases, member_leases): (Vec<_>, Vec<_>) = report
                .candidate_discoveries
                .iter()
                .cloned()
                .map(|discovery| discovery.into_lease(crate::REACHABILITY_LEASE_MS))
                .partition(|lease| lease.node_id == reservation_id);
            if !admission_leases.is_empty() {
                self.apply_before_deadline(
                    context,
                    Command::RenewAdmissionReachabilityLeases {
                        reservation_id,
                        reservation_operation_id,
                        leases: admission_leases,
                    },
                )
                .await?;
            }
            if !member_leases.is_empty() {
                self.apply_before_deadline(
                    context,
                    Command::RenewReachabilityLeases(member_leases),
                )
                .await?;
            }
            let verified_at_ms = upgrid_config::now_ms();
            for (node_id, address) in verified_addresses(&report) {
                let command = if node_id == reservation_id {
                    Command::VerifyAdmissionReachableAddress {
                        node_id,
                        address,
                        verified_at_ms,
                        reservation_operation_id,
                    }
                } else {
                    Command::VerifyReachableAddress {
                        node_id,
                        address,
                        verified_at_ms,
                    }
                };
                self.apply_before_deadline(context, command).await?;
            }
            let complete = report.failures.is_empty();
            final_report = Some(report);
            if complete {
                break;
            }
            if attempt < 2 {
                before_deadline(context, sleep(Duration::from_millis(150))).await?;
            }
        }
        Ok(final_report.expect("connectivity runs at least once"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    #[test]
    fn admission_retains_verified_routes_for_every_node() {
        let joining_id = uuid::Uuid::from_u128(1);
        let member_id = uuid::Uuid::from_u128(2);
        let first = ReachableAddress::parse("up://first.example:11451").unwrap();
        let second = ReachableAddress::parse("up://second.example:11451").unwrap();
        let member = ReachableAddress::parse("up://member.example:11451").unwrap();
        let report = ConnectivityReport {
            failures: BTreeSet::new(),
            verified: BTreeMap::from([
                (joining_id, BTreeSet::from([first.clone(), second.clone()])),
                (member_id, BTreeSet::from([member.clone()])),
            ]),
            candidate_discoveries: Vec::new(),
        };

        assert_eq!(
            verified_addresses(&report).collect::<BTreeSet<_>>(),
            BTreeSet::from([
                (joining_id, first),
                (joining_id, second),
                (member_id, member),
            ])
        );
    }
}
