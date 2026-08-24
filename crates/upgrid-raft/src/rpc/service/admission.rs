use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use compio::runtime::spawn;
use compio::time::{sleep, timeout};
use futures_channel::oneshot;
use openraft::ChangeMembers;
use openraft::async_runtime::watch::WatchReceiver as _;
use upgrid_config::now_ms;
use upgrid_rpc::Context;

use super::{JoinError, UpgridServer};
use crate::domain::{Command, CommandResult};
use crate::raft::{NodeIdentity, NodeRegistration, Req};
use crate::token::{admission_acceptance_id, admission_operation_id, hash_join_token};
use crate::{
    DirectedRoute, DiscoverySource, ReachableAddress, ReachableAddressCandidate,
    ReachableAddressLease,
};

mod connectivity;
mod deadline;

use deadline::{before_deadline, settle_before_deadline};

const ADMISSION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const ADMISSION_OPERATION_TIMEOUT: Duration = Duration::from_secs(20);

fn operation_context(caller: Context) -> Context {
    Context::with_deadline(
        caller
            .deadline()
            .min(Instant::now() + ADMISSION_OPERATION_TIMEOUT),
    )
}

impl UpgridServer {
    pub(super) async fn admit(
        self,
        context: Context,
        remote: NodeRegistration,
        token: String,
    ) -> Result<(), JoinError> {
        let (reply, response) = oneshot::channel();
        let operation_context = operation_context(context);
        spawn(async move {
            let result = self.run_admission(operation_context, remote, token).await;
            let _ = reply.send(result);
        })
        .detach();
        response.await.map_err(|_| JoinError::RuntimeStopped)?
    }

    async fn run_admission(
        &self,
        context: Context,
        mut remote: NodeRegistration,
        token: String,
    ) -> Result<(), JoinError> {
        if let Some(leader_id) = self.join_forward_target() {
            let addresses = self.rpc.reachable_addresses(leader_id);
            if addresses.is_empty() {
                return Err(JoinError::Connection(
                    crate::ConnectionFailure::NoReachableAddress { node_id: leader_id },
                ));
            }
            return Err(JoinError::Redirect {
                node_id: leader_id,
                addresses,
            });
        }
        remote.candidates.push(ReachableAddressCandidate {
            address: self.connection_reachable_address_candidate(),
            source: DiscoverySource::Node {
                discovering_node_id: self.self_id,
            },
        });
        let _membership_change = before_deadline(&context, self.membership_changes.lock()).await?;
        let remote_id = remote.id;
        let token_hash = hash_join_token(&token);
        let reservation_operation_id = admission_operation_id(remote_id, token_hash);
        let acceptance_id = admission_acceptance_id(reservation_operation_id);
        if self.admission_was_accepted(acceptance_id, remote_id) {
            return Ok(());
        }
        let already_known = self.member_is_voter(remote_id) == Some(true);
        let reserved_at_ms = now_ms();
        let reservation = Req::new(Command::ReserveJoinToken {
            hash: token_hash,
            reservation_id: remote_id,
            reservation_operation_id,
            reserved_at_ms,
            readmission: already_known,
        });
        self.apply_request_before_deadline(&context, reservation)
            .await?;
        let result = self
            .finish_admission(
                &context,
                &remote,
                reserved_at_ms,
                already_known,
                reservation_operation_id,
            )
            .await;
        let retained_incomplete_member =
            result.is_err() && !already_known && self.member_is_voter(remote_id).is_some();
        if result.is_err()
            && !already_known
            && !retained_incomplete_member
            && let Err(error) = self.remove_pending_member(remote_id).await
        {
            return Err(error);
        }
        if retained_incomplete_member {
            return result;
        }
        let completed_at_ms = now_ms();
        let completion = match (result.is_err(), already_known) {
            (true, false) => Req::new(Command::AbortPendingJoin {
                reservation_id: remote_id,
                reservation_operation_id,
                completed_at_ms,
            }),
            (true, true) => Req::new(Command::AbortPendingReadmission {
                reservation_id: remote_id,
                reservation_operation_id,
                completed_at_ms,
            }),
            (false, _) => Req {
                operation_id: acceptance_id,
                submitted_at_ms: completed_at_ms,
                command: Command::CompleteJoinTokenReservation {
                    reservation_id: remote_id,
                    reservation_operation_id,
                    accepted: true,
                    completed_at_ms,
                },
            },
        };
        self.apply_cleanup(completion).await?;
        result
    }

    fn admission_was_accepted(&self, acceptance_id: uuid::Uuid, node_id: uuid::Uuid) -> bool {
        matches!(
            self.rpc.processed_operation_result(acceptance_id),
            Some(Ok(CommandResult::AdmissionAccepted(accepted))) if accepted == node_id
        )
    }

    async fn finish_admission(
        &self,
        context: &Context,
        remote: &NodeRegistration,
        reserved_at_ms: u64,
        already_known: bool,
        reservation_operation_id: uuid::Uuid,
    ) -> Result<(), JoinError> {
        let remote_id = remote.id;
        if remote.configured_explicit {
            self.apply_before_deadline(
                context,
                Command::ReplaceAdmissionConfiguredReachableAddresses {
                    node_id: remote_id,
                    addresses: remote.configured.clone(),
                    reservation_operation_id,
                },
            )
            .await?;
        }
        let expires_at_ms = reserved_at_ms.saturating_add(crate::REACHABILITY_LEASE_MS);
        let leases = remote
            .candidates
            .iter()
            .map(|candidate| ReachableAddressLease {
                node_id: remote_id,
                address: candidate.address.clone(),
                source: candidate.source.clone(),
                discovered_at_ms: reserved_at_ms,
                expires_at_ms,
            })
            .collect::<Vec<_>>();
        self.apply_before_deadline(
            context,
            Command::RenewAdmissionReachabilityLeases {
                reservation_id: remote_id,
                reservation_operation_id,
                leases,
            },
        )
        .await?;
        if !already_known {
            self.preflight_connectivity(context, remote, reservation_operation_id)
                .await?;
        }
        self.verify_remote_addresses(context, remote_id, reservation_operation_id)
            .await?;
        if !already_known {
            self.add_learner_when_ready(context, remote).await?;
        }
        self.verify_connectivity_and_promote(context, remote_id, reservation_operation_id)
            .await
    }

    async fn preflight_connectivity(
        &self,
        context: &Context,
        remote: &NodeRegistration,
        reservation_operation_id: uuid::Uuid,
    ) -> Result<(), JoinError> {
        let nodes = BTreeSet::from([self.self_id, remote.id]);
        let report = self
            .connectivity_report(context, &nodes, remote.id, reservation_operation_id)
            .await?;
        if report.failures.is_empty() {
            Ok(())
        } else {
            Err(JoinError::Connectivity(
                report.failures.into_iter().collect(),
            ))
        }
    }

    async fn verify_remote_addresses(
        &self,
        context: &Context,
        remote_id: uuid::Uuid,
        reservation_operation_id: uuid::Uuid,
    ) -> Result<(), JoinError> {
        let verified = before_deadline(context, self.rpc.probe_node(remote_id)).await?;
        if verified.is_empty() {
            return Err(JoinError::Connectivity(vec![DirectedRoute {
                source: self.self_id,
                destination: remote_id,
            }]));
        }
        let verified_at_ms = now_ms();
        for address in verified {
            self.apply_before_deadline(
                context,
                Command::VerifyAdmissionReachableAddress {
                    node_id: remote_id,
                    address,
                    verified_at_ms,
                    reservation_operation_id,
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn verify_connectivity_and_promote(
        &self,
        context: &Context,
        remote_id: uuid::Uuid,
        reservation_operation_id: uuid::Uuid,
    ) -> Result<(), JoinError> {
        let metrics = self.raft.metrics();
        let mut voters = metrics
            .borrow_watched()
            .committed_membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();
        let mut nodes = self
            .raft
            .metrics()
            .borrow_watched()
            .committed_membership_config
            .nodes()
            .map(|(node_id, _)| *node_id)
            .collect::<BTreeSet<_>>();
        nodes.insert(remote_id);
        self.wait_for_connectivity(context, &nodes, remote_id, reservation_operation_id)
            .await?;

        if !voters.contains(&remote_id) {
            voters.insert(remote_id);
            self.promote_learner_before_deadline(context, &voters)
                .await?;
        }
        Ok(())
    }

    fn member_is_voter(&self, node_id: uuid::Uuid) -> Option<bool> {
        let metrics = self.raft.metrics();
        let current = metrics.borrow_watched();
        if !current
            .committed_membership_config
            .nodes()
            .any(|(candidate_id, _)| *candidate_id == node_id)
        {
            return None;
        }
        Some(
            current
                .committed_membership_config
                .membership()
                .voter_ids()
                .any(|voter_id| voter_id == node_id),
        )
    }

    async fn remove_pending_member(&self, node_id: uuid::Uuid) -> Result<(), JoinError> {
        let cleanup_context = Context::with_deadline(Instant::now() + ADMISSION_CLEANUP_TIMEOUT);
        let mut last_error = None;
        loop {
            let Some(was_voter) = self.member_is_voter(node_id) else {
                return Ok(());
            };
            if Instant::now() >= cleanup_context.deadline() {
                return Err(last_error.unwrap_or(JoinError::Deadline));
            }
            if let Some(leader_id) = self.join_forward_target() {
                let attempt_timeout = cleanup_context
                    .deadline()
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(1));
                let client = match timeout(attempt_timeout, self.rpc.client_to(leader_id)).await {
                    Ok(Ok(client)) => client,
                    Ok(Err(error)) => {
                        last_error = Some(JoinError::Connection(error.into()));
                        self.rpc.invalidate_node(leader_id);
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(_) => {
                        last_error = Some(JoinError::Deadline);
                        self.rpc.invalidate_node(leader_id);
                        continue;
                    }
                };
                let context = Context::with_deadline(Instant::now() + attempt_timeout);
                match client.remove_node(context, node_id).await {
                    Ok(Ok(())) | Ok(Err(crate::MembershipError::NodeNotFound(_))) => return Ok(()),
                    Ok(Err(error)) => {
                        last_error = Some(JoinError::Membership(error));
                    }
                    Err(error) => {
                        last_error = Some(JoinError::Connection(error.into()));
                        self.rpc.invalidate_node(leader_id);
                    }
                }
                sleep(Duration::from_millis(50)).await;
                continue;
            }

            let result = if was_voter {
                let metrics = self.raft.metrics();
                let voters = metrics
                    .borrow_watched()
                    .committed_membership_config
                    .membership()
                    .voter_ids()
                    .filter(|voter_id| *voter_id != node_id)
                    .collect::<BTreeSet<_>>();
                self.promote_learner_before_deadline(&cleanup_context, &voters)
                    .await
            } else {
                self.remove_learner_before_deadline(&cleanup_context, node_id)
                    .await
            };
            if let Err(error) = result
                && self.member_is_voter(node_id) == Some(was_voter)
            {
                last_error = Some(error);
                sleep(Duration::from_millis(50)).await;
            }
        }
    }

    fn join_forward_target(&self) -> Option<uuid::Uuid> {
        let metrics = self.raft.metrics();
        let current = metrics.borrow_watched();
        current
            .current_leader
            .filter(|leader_id| *leader_id != self.self_id)
    }

    async fn add_learner_when_ready(
        &self,
        context: &Context,
        remote: &NodeRegistration,
    ) -> Result<(), JoinError> {
        loop {
            let result = settle_before_deadline(
                context,
                self.raft
                    .add_learner(remote.id, NodeIdentity::default(), false),
            )
            .await?;
            match result {
                Ok(_) => return Ok(()),
                Err(error) if super::is_membership_change_in_progress(&error) => {
                    before_deadline(context, sleep(Duration::from_millis(50))).await?;
                }
                Err(error) => return Err(JoinError::Raft(error)),
            }
        }
    }

    async fn remove_learner_before_deadline(
        &self,
        context: &Context,
        node_id: uuid::Uuid,
    ) -> Result<(), JoinError> {
        loop {
            let result = before_deadline(
                context,
                self.raft.change_membership(
                    ChangeMembers::RemoveNodes(BTreeSet::from([node_id])),
                    false,
                ),
            )
            .await?;
            match result {
                Ok(_) => return Ok(()),
                Err(error) if super::is_membership_change_in_progress(&error) => {
                    before_deadline(context, sleep(Duration::from_millis(50))).await?;
                }
                Err(error) => return Err(JoinError::Raft(error)),
            }
        }
    }

    async fn promote_learner_before_deadline(
        &self,
        context: &Context,
        voters: &BTreeSet<uuid::Uuid>,
    ) -> Result<(), JoinError> {
        loop {
            let result =
                settle_before_deadline(context, self.raft.change_membership(voters.clone(), false))
                    .await?;
            match result {
                Ok(_) => return Ok(()),
                Err(error) if super::is_membership_change_in_progress(&error) => {
                    before_deadline(context, sleep(Duration::from_millis(50))).await?;
                }
                Err(error) => return Err(JoinError::Raft(error)),
            }
        }
    }

    async fn apply_before_deadline(
        &self,
        context: &Context,
        command: Command,
    ) -> Result<(), JoinError> {
        settle_before_deadline(context, self.apply(command)).await?
    }

    async fn apply_request_before_deadline(
        &self,
        context: &Context,
        request: Req,
    ) -> Result<(), JoinError> {
        settle_before_deadline(context, self.apply_request(request)).await?
    }

    async fn apply_cleanup(&self, request: Req) -> Result<(), JoinError> {
        let response = self
            .rpc
            .write_to_leader(request, Instant::now() + ADMISSION_CLEANUP_TIMEOUT)
            .await
            .map_err(|error| JoinError::Connection(error.into()))?;
        response.result.map_err(JoinError::Rejected)?;
        Ok(())
    }

    async fn apply(&self, command: Command) -> Result<(), JoinError> {
        self.apply_request(Req::new(command)).await
    }

    async fn apply_request(&self, request: Req) -> Result<(), JoinError> {
        let response = self
            .raft
            .client_write(request)
            .await
            .map_err(JoinError::Raft)?;
        response.data.result.map_err(JoinError::Rejected)?;
        Ok(())
    }

    pub(super) fn connection_reachable_address_candidate(&self) -> ReachableAddress {
        ReachableAddress::parse(&format!("up://{}", self.source_endpoint.current()))
            .expect("an authenticated node connection gives a valid reachable address candidate")
    }
}

#[cfg(test)]
mod tests;
