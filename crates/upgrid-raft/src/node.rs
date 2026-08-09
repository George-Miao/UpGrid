//! Raft Node lifecycle and forwarded Cluster operations.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use compio::runtime::{JoinHandle, spawn};
use compio::time::{sleep, timeout};
use openraft::async_runtime::watch::WatchReceiver as _;
use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::{Config, ReadPolicy};
use snafu::ResultExt;
use tarpc::context::Context;
use tracing::{debug, info};
use upgrid_config::Cipher;
use upgrid_transport::{RpcTransport, secure_endpoint};
use url::Url;

use crate::UpgridNode;
use crate::error::*;
use crate::raft::{Identity, Raft, Req, Res};
use crate::rpc::{JoinError, Rpc, UpgridNetwork};
use crate::state_machine::StateMachine;
use crate::storage::InMemStore;

pub struct Node {
    id: Identity,
    raft: Raft,
    state_machine: Rc<StateMachine>,
    rpc: Rpc,
    deployment_key_fingerprint: [u8; 32],
    _server_handle: JoinHandle<()>,
    _metrics_handle: JoinHandle<()>,
}

impl Node {
    pub fn data_membership_urls(data_dir: &Path) -> Result<BTreeSet<String>> {
        let state_path = data_dir.join("raft-state.postcard");
        let state_machine =
            StateMachine::open(&state_path).context(StateMachineOpenSnafu { path: state_path })?;
        Ok(state_machine
            .state_machine
            .borrow()
            .last_membership
            .nodes()
            .map(|(_, node)| node.to_string())
            .collect())
    }

    pub fn node_id(&self) -> uuid::Uuid {
        self.id.id
    }

    #[cfg(test)]
    pub async fn new<U, E>(advertise_url: U) -> Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let id = Identity::new(advertise_url)?;
        Self::with_identity(id).await
    }

    #[cfg(test)]
    pub async fn with_identity(id: Identity) -> Result<Self> {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .expect("test deployment key should be valid");
        Self::build(
            id,
            InMemStore::new(),
            Rc::new(StateMachine::default()),
            &cipher,
        )
        .await
    }

    pub async fn open(id: Identity, data_dir: &Path, cipher: &Cipher) -> Result<Self> {
        let log_path = data_dir.join("raft-log.redb");
        let legacy_log_path = data_dir.join("raft-log.postcard");
        let storage = InMemStore::open(&log_path, &legacy_log_path)
            .context(RaftLogOpenSnafu { path: log_path })?;
        let state_path = data_dir.join("raft-state.postcard");
        let state_machine = Rc::new(
            StateMachine::open(&state_path).context(StateMachineOpenSnafu { path: state_path })?,
        );
        Self::build(id, storage, state_machine, cipher).await
    }

    async fn build(
        id: Identity,
        storage: InMemStore<crate::raft::TC>,
        state_machine: Rc<StateMachine>,
        cipher: &Cipher,
    ) -> Result<Self> {
        let endpoint = secure_endpoint(id.node.host().to_owned(), id.node.port(), cipher).await?;

        let deployment_key_fingerprint = cipher.fingerprint();
        let rpc = Rpc::new(RpcTransport::new(endpoint), deployment_key_fingerprint);
        let network = UpgridNetwork::new(id.clone(), rpc.clone());
        let config = raft_config();

        let raft = Raft::new(
            id.id,
            Arc::new(config),
            network,
            storage,
            state_machine.clone(),
        )
        .await
        .context(RaftCreationSnafu)?;

        rpc.init_raft(raft.clone());

        let metrics_handle = watch_cluster_events(raft.clone());

        let server_handle = spawn(rpc.clone().run());

        Ok(Self {
            id,
            raft,
            state_machine,
            rpc,
            deployment_key_fingerprint,
            _server_handle: server_handle,
            _metrics_handle: metrics_handle,
        })
    }

    pub async fn join<U, E>(&self, remote: U, token: &str) -> Result<()>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let remote = UpgridNode::new(remote)?;
        debug!(%remote, "requesting Cluster membership");

        let client = self.rpc.client(&remote).await?;
        let remote_fingerprint = client
            .deployment_key_fingerprint(Context::current())
            .await
            .context(RpcSnafu)?;
        if remote_fingerprint != self.deployment_key_fingerprint {
            return Err(Error::DeploymentKeyMismatch);
        }

        let result = client
            .ask_to_join(Context::current(), self.id.clone(), token.to_owned())
            .await
            .context(RpcSnafu)?;
        let result = match result {
            Err(JoinError::Raft(RaftError::APIError(ClientWriteError::ForwardToLeader(
                forward,
            )))) if forward.leader_node.is_some() => self
                .rpc
                .client(&forward.leader_node.expect("checked above"))
                .await?
                .ask_to_join(Context::current(), self.id.clone(), token.to_owned())
                .await
                .context(RpcSnafu)?,
            result => result,
        };
        match result {
            Ok(()) => {}
            Err(JoinError::Raft(source)) => return Err(Error::RaftJoin { source }),
            Err(source) => return Err(Error::JoinRejected { source }),
        }

        Ok(())
    }

    pub fn has_membership(&self) -> bool {
        self.state_machine
            .state_machine
            .borrow()
            .last_membership
            .nodes()
            .next()
            .is_some()
    }

    pub async fn start_cluster(&self) -> Result<()> {
        let mut map = BTreeMap::new();
        map.insert(self.id.id, self.id.node.clone());
        let res = self.raft.initialize(map).await;

        if let Err(RaftError::APIError(InitializeError::NotAllowed(_))) = &res {
            Ok(())
        } else {
            res.context(RaftInitializeSnafu)
        }
    }

    pub fn local_application_state(&self) -> crate::domain::ApplicationState {
        self.state_machine.application_state()
    }

    pub fn applied_index(&self) -> Option<u64> {
        self.state_machine.applied_index()
    }

    pub async fn write(&self, request: Req) -> Result<Res, String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut leader = None;
        let mut last_transport_error = None;
        loop {
            if Instant::now() >= deadline {
                return Err(last_transport_error.unwrap_or_else(|| {
                    "leadership could not be established before deadline".to_owned()
                }));
            }

            let result = if let Some(forwarded_to) = leader.take() {
                let attempt_timeout = deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(1));
                let client = match timeout(attempt_timeout, self.rpc.client(&forwarded_to)).await {
                    Ok(Ok(client)) => client,
                    Ok(Err(error)) => {
                        last_transport_error = Some(error.to_string());
                        self.rpc.invalidate(&forwarded_to);
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(_) => {
                        last_transport_error = Some(format!(
                            "timed out connecting to forwarded leader {forwarded_to}"
                        ));
                        self.rpc.invalidate(&forwarded_to);
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let mut context = Context::current();
                context.deadline = Instant::now()
                    + deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_secs(1));
                match client.client_write(context, request.clone()).await {
                    Ok(result) => result,
                    Err(error) => {
                        last_transport_error = Some(error.to_string());
                        self.rpc.invalidate(&forwarded_to);
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
            } else {
                self.raft.client_write(request.clone()).await
            };
            match result {
                Ok(response) => return Ok(response.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward)))
                    if Instant::now() < deadline =>
                {
                    leader = forward.leader_node;
                    if leader.is_none() {
                        sleep(Duration::from_millis(50)).await;
                    }
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }

    pub async fn read(&self) -> Result<crate::domain::ApplicationState, String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                return Err(last_error.unwrap_or_else(|| {
                    "linearizable read unavailable before deadline".to_owned()
                }));
            }
            match self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
                Ok(_) => return Ok(self.local_application_state()),
                Err(error) => {
                    let Some(leader) = error
                        .forward_to_leader()
                        .and_then(|forward| forward.leader_node.clone())
                    else {
                        last_error = Some(format!("linearizable read unavailable: {error}"));
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    };
                    let attempt_timeout = deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_secs(1));
                    let client = match timeout(attempt_timeout, self.rpc.client(&leader)).await {
                        Ok(Ok(client)) => client,
                        Ok(Err(error)) => {
                            last_error = Some(error.to_string());
                            self.rpc.invalidate(&leader);
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        Err(_) => {
                            last_error =
                                Some(format!("timed out connecting to forwarded leader {leader}"));
                            self.rpc.invalidate(&leader);
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    let mut context = Context::current();
                    context.deadline = Instant::now()
                        + deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_secs(1));
                    let read_log_id = match client.read_index(context).await {
                        Ok(Ok(log_id)) => log_id,
                        Ok(Err(error)) => {
                            last_error = Some(error.to_string());
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        Err(error) => {
                            last_error = Some(error.to_string());
                            self.rpc.invalidate(&leader);
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    if let Some(log_id) = read_log_id {
                        self.raft
                            .wait(Some(
                                deadline
                                    .saturating_duration_since(Instant::now())
                                    .min(Duration::from_secs(1)),
                            ))
                            .applied_index_at_least(
                                Some(log_id.index()),
                                "cluster API read barrier",
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    return Ok(self.local_application_state());
                }
            }
        }
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.id.id)
    }

    pub async fn probe_node(&self, node_id: uuid::Uuid, url: &str) -> Result<(), String> {
        if node_id == self.id.id {
            return Ok(());
        }
        let node = UpgridNode::new(url).map_err(|error| error.to_string())?;
        let result = timeout(Duration::from_secs(2), async {
            let client = self
                .rpc
                .client(&node)
                .await
                .map_err(|error| error.to_string())?;
            client
                .ping(Context::current())
                .await
                .map_err(|error| error.to_string())
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                self.rpc.invalidate(&node);
                Err("Node RPC timed out".to_owned())
            }
        }
    }

    pub fn voters(&self) -> BTreeSet<uuid::Uuid> {
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    pub fn cluster_topology(&self) -> (Option<uuid::Uuid>, BTreeMap<uuid::Uuid, String>) {
        let metrics = self.raft.metrics();
        let current = metrics.borrow_watched();
        let members = current
            .membership_config
            .nodes()
            .map(|(node_id, node)| (*node_id, node.to_string()))
            .collect();
        (current.current_leader, members)
    }
}

fn watch_cluster_events(raft: Raft) -> JoinHandle<()> {
    spawn(async move {
        let mut metrics = raft.metrics();
        let mut previous_leader = None;
        let mut previous_members = BTreeSet::new();

        loop {
            let (leader, members) = {
                let current = metrics.borrow_watched();
                let members = current
                    .membership_config
                    .nodes()
                    .map(|(node_id, _)| *node_id)
                    .collect::<BTreeSet<_>>();
                (current.current_leader, members)
            };

            for node_id in members.difference(&previous_members) {
                info!(%node_id, "Node joined Cluster");
            }
            for node_id in previous_members.difference(&members) {
                info!(%node_id, "Node exited Cluster");
            }
            if leader != previous_leader {
                info!(?previous_leader, ?leader, "Cluster leader changed");
            }

            previous_leader = leader;
            previous_members = members;
            if metrics.changed().await.is_err() {
                break;
            }
        }
    })
}

fn raft_config() -> Config {
    Config {
        cluster_name: "UpGrid".to_string(),
        heartbeat_interval: 1_000,
        election_timeout_min: 3_000,
        election_timeout_max: 6_000,
        install_snapshot_timeout: 10_000,
        ..Config::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_timing_allows_for_network_and_disk_latency() {
        let config = raft_config();
        assert!(
            config.heartbeat_interval >= 500,
            "AppendEntries must not inherit OpenRaft's 50 ms default deadline"
        );
        assert!(config.election_timeout_min >= config.heartbeat_interval * 3);
        assert!(config.election_timeout_max >= config.election_timeout_min * 2);
        config.validate().unwrap();
    }
}
