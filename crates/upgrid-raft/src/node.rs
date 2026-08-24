//! Raft Node lifecycle and forwarded Cluster operations.
mod admission_reaper;
mod publication;
mod runtime;

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use admission_reaper::watch_expired_admissions;
use compio::runtime::{JoinHandle, spawn};
use compio::time::{sleep, timeout};
use openraft::ReadPolicy;
use openraft::error::{InitializeError, RaftError};
use runtime::{
    bootstrap_address, raft_config, watch_cluster_events, watch_connectivity,
    watch_reachable_address_candidates,
};
use snafu::ResultExt;
use tracing::debug;
use upgrid_config::{Cipher, LocalAddress, QuicCaKey};
use upgrid_rpc::Context;
use upgrid_transport::RpcTransport;

use crate::ReachableAddress;
use crate::cluster::{ClusterError, DomainSnafu};
use crate::database::{RaftDatabase, StateRepository};
use crate::error::*;
use crate::raft::{NodeRegistration, Raft, Req, Res};
use crate::rpc::{JoinError, Rpc, UpgridNetwork};
use crate::state_machine::StateMachine;
use crate::storage::InMemStore;

const ADMISSION_TIMEOUT: Duration = Duration::from_secs(20);

pub struct NodeNetworkConfig {
    local_addresses: BTreeSet<LocalAddress>,
    configured: BTreeSet<ReachableAddress>,
    configured_explicit: bool,
    candidates: Vec<crate::ReachableAddressCandidate>,
}

impl NodeNetworkConfig {
    pub fn new(
        local_addresses: BTreeSet<LocalAddress>,
        configured: BTreeSet<ReachableAddress>,
        configured_explicit: bool,
        candidates: Vec<crate::ReachableAddressCandidate>,
    ) -> Self {
        Self {
            local_addresses,
            configured,
            configured_explicit,
            candidates,
        }
    }
}

fn local_reachable_address(address: &LocalAddress) -> Option<ReachableAddress> {
    let host = match address.host {
        IpAddr::V4(host) if host.is_unspecified() => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        IpAddr::V6(host) if host.is_unspecified() => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        host => host,
    };
    ReachableAddress::from_host_port(host.to_string(), address.port)
}

pub struct Node {
    id: uuid::Uuid,
    registration: Option<NodeRegistration>,
    configured: BTreeSet<ReachableAddress>,
    configured_explicit: bool,
    candidates: Vec<crate::ReachableAddressCandidate>,
    raft: Raft,
    state_machine: Rc<StateMachine>,
    rpc: Rpc,
    deployment_key_fingerprint: [u8; 32],
    _server_handle: JoinHandle<()>,
    _connectivity_handle: JoinHandle<()>,
    _admission_reaper_handle: JoinHandle<()>,
    _candidate_renewal_handle: JoinHandle<()>,
    _metrics_handle: JoinHandle<()>,
}

impl Node {
    pub fn data_membership_addresses(
        data_dir: &Path,
        observer_node_id: uuid::Uuid,
    ) -> Result<Option<BTreeMap<uuid::Uuid, BTreeSet<ReachableAddress>>>> {
        let path = data_dir.join("raft.sqlite3");
        let database = Rc::new(
            RaftDatabase::open(data_dir)
                .map_err(std::io::Error::from)
                .context(DatabaseOpenSnafu { path: path.clone() })?,
        );
        let (state, ..) = StateRepository::new(database)
            .load()
            .map_err(std::io::Error::from)
            .context(DatabaseOpenSnafu { path })?;
        let now_ms = upgrid_config::now_ms();
        let members = state
            .last_membership
            .nodes()
            .map(|(node_id, identity)| {
                let mut addresses = state
                    .application
                    .node_reachability
                    .get(node_id)
                    .map(|reachability| reachability.reachable(observer_node_id, now_ms))
                    .unwrap_or_default();
                addresses.extend(identity.legacy_address().cloned());
                (*node_id, addresses)
            })
            .collect::<BTreeMap<_, _>>();
        Ok((!members.is_empty()).then_some(members))
    }

    pub fn node_id(&self) -> uuid::Uuid {
        self.id
    }

    #[cfg(test)]
    pub async fn new(advertise_url: impl AsRef<str>) -> Result<Self> {
        let registration = NodeRegistration::new(advertise_url)?;
        Self::with_identity(registration).await
    }

    #[cfg(test)]
    pub async fn with_identity(registration: NodeRegistration) -> Result<Self> {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .expect("test deployment key should be valid");
        let quic_ca_key = QuicCaKey::derive(&cipher);
        let local_address = registration
            .bootstrap
            .host()
            .parse()
            .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let local_address = LocalAddress {
            host: local_address,
            port: registration.bootstrap.port(),
        };
        let id = registration.id;
        let configured = registration.configured.clone();
        let configured_explicit = registration.configured_explicit;
        let candidates = registration.candidates.clone();
        let network = NodeNetworkConfig::new(
            BTreeSet::from([local_address]),
            configured,
            configured_explicit,
            candidates,
        );
        Self::build(
            id,
            Some(registration),
            network,
            InMemStore::new(),
            Rc::new(StateMachine::default()),
            &cipher,
            &quic_ca_key,
        )
        .await
    }

    pub async fn open(
        id: uuid::Uuid,
        network: NodeNetworkConfig,
        data_dir: &Path,
        cipher: &Cipher,
        quic_ca_key: &QuicCaKey,
    ) -> Result<Self> {
        let registration = bootstrap_address(id, &network.configured, &network.candidates)
            .ok()
            .or_else(|| {
                network
                    .local_addresses
                    .iter()
                    .find_map(local_reachable_address)
            })
            .map(|bootstrap| {
                NodeRegistration::from_network(
                    id,
                    bootstrap,
                    network.configured.clone(),
                    network.configured_explicit,
                    network.candidates.clone(),
                )
            });
        let path = data_dir.join("raft.sqlite3");
        let database = Rc::new(
            RaftDatabase::open(data_dir)
                .map_err(std::io::Error::from)
                .context(DatabaseOpenSnafu { path })?,
        );
        let storage = InMemStore::open(database.clone()).context(DatabaseOpenSnafu {
            path: data_dir.join("raft.sqlite3"),
        })?;
        let state_machine = Rc::new(StateMachine::open(database).context(DatabaseOpenSnafu {
            path: data_dir.join("raft.sqlite3"),
        })?);
        Self::build(
            id,
            registration,
            network,
            storage,
            state_machine,
            cipher,
            quic_ca_key,
        )
        .await
    }

    async fn build(
        id: uuid::Uuid,
        registration: Option<NodeRegistration>,
        network: NodeNetworkConfig,
        storage: InMemStore<crate::raft::TC>,
        state_machine: Rc<StateMachine>,
        cipher: &Cipher,
        quic_ca_key: &QuicCaKey,
    ) -> Result<Self> {
        let NodeNetworkConfig {
            local_addresses,
            configured,
            configured_explicit,
            candidates,
        } = network;
        let transport = RpcTransport::bind(&local_addresses, id, quic_ca_key).await?;
        let deployment_key_fingerprint = cipher.fingerprint();
        let rpc = Rpc::new(
            id,
            transport,
            state_machine.clone(),
            deployment_key_fingerprint,
        );
        let network = UpgridNetwork::new(id, rpc.clone());
        let config = raft_config();

        let raft = Raft::new(
            id,
            Arc::new(config),
            network,
            storage,
            state_machine.clone(),
        )
        .await
        .context(RaftCreationSnafu)?;

        rpc.init_raft(raft.clone());

        let connectivity_handle =
            watch_connectivity(id, raft.clone(), rpc.clone(), state_machine.clone());
        let admission_reaper_handle =
            watch_expired_admissions(id, raft.clone(), rpc.clone(), state_machine.clone());
        let candidate_renewal_handle =
            watch_reachable_address_candidates(id, raft.clone(), rpc.clone());
        let metrics_handle = watch_cluster_events(raft.clone(), rpc.clone());

        let server_handle = spawn(rpc.clone().run());

        Ok(Self {
            id,
            registration,
            configured,
            configured_explicit,
            candidates,
            raft,
            state_machine,
            rpc,
            deployment_key_fingerprint,
            _server_handle: server_handle,
            _connectivity_handle: connectivity_handle,
            _admission_reaper_handle: admission_reaper_handle,
            _candidate_renewal_handle: candidate_renewal_handle,
            _metrics_handle: metrics_handle,
        })
    }

    pub async fn join(
        &self,
        mut remote_node_id: uuid::Uuid,
        remote: impl AsRef<str>,
        token: &str,
    ) -> Result<()> {
        let deadline = Instant::now() + ADMISSION_TIMEOUT;
        let mut remote_addresses = vec![ReachableAddress::parse(remote.as_ref())?];

        match timeout(ADMISSION_TIMEOUT, async {
            loop {
                debug!(
                    routes = remote_addresses.len(),
                    "requesting cluster membership"
                );
                let client = self
                    .rpc
                    .client_to_addresses(remote_node_id, remote_addresses)
                    .await?;
                let remote_fingerprint = client
                    .deployment_key_fingerprint(Context::with_deadline(deadline))
                    .await
                    .context(RpcSnafu)?;
                if remote_fingerprint != self.deployment_key_fingerprint {
                    return Err(Error::DeploymentKeyMismatch);
                }

                let result = client
                    .ask_to_join(
                        Context::with_deadline(deadline),
                        self.registration
                            .clone()
                            .ok_or(Error::NoReachableAddress { node_id: self.id })?,
                        token.to_owned(),
                    )
                    .await
                    .context(RpcSnafu)?;
                match result {
                    Ok(()) => return Ok(()),
                    Err(JoinError::Redirect { node_id, addresses }) => {
                        remote_node_id = node_id;
                        remote_addresses = addresses;
                    }
                    Err(JoinError::Raft(source)) => return Err(Error::RaftJoin { source }),
                    Err(source) => return Err(Error::JoinRejected { source }),
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(Error::JoinRejected {
                source: JoinError::Deadline,
            }),
        }
    }

    pub fn has_membership(&self) -> bool {
        self.state_machine
            .state_machine
            .borrow()
            .last_membership
            .membership()
            .voter_ids()
            .any(|node_id| node_id == self.id)
    }

    pub async fn start_cluster(&self) -> Result<()> {
        let mut map = BTreeMap::new();
        map.insert(self.id, crate::NodeIdentity::default());
        let result = self.raft.initialize(map).await;
        if !matches!(
            &result,
            Err(RaftError::APIError(InitializeError::NotAllowed(_)))
        ) {
            result.context(RaftInitializeSnafu)?;
        }
        self.publish_configured_reachable_addresses().await
    }

    pub fn local_application_state(&self) -> crate::domain::ApplicationState {
        self.state_machine.application_state()
    }

    pub fn applied_index(&self) -> Option<u64> {
        self.state_machine.applied_index()
    }

    pub async fn apply(
        &self,
        command: crate::domain::Command,
    ) -> Result<crate::domain::CommandResult, ClusterError> {
        let response = self
            .write(Req::new(command))
            .await
            .context(crate::cluster::OperationSnafu)?;
        response.result.context(DomainSnafu)
    }

    pub(crate) async fn write(&self, request: Req) -> Result<Res> {
        self.rpc
            .write_to_leader(request, Instant::now() + Duration::from_secs(5))
            .await
    }

    pub async fn read(&self) -> Result<crate::domain::ApplicationState> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                return Err(last_error.unwrap_or(Error::LinearizableReadDeadline));
            }
            match self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
                Ok(_) => return Ok(self.local_application_state()),
                Err(source) => {
                    let Some(leader_id) = source
                        .forward_to_leader()
                        .and_then(|forward| forward.leader_id)
                    else {
                        last_error = Some(Error::LinearizableRead { source });
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    };
                    let attempt_timeout = deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_secs(1));
                    let client = match timeout(attempt_timeout, self.rpc.client_to(leader_id)).await
                    {
                        Ok(Ok(client)) => client,
                        Ok(Err(error)) => {
                            last_error = Some(error);
                            self.rpc.invalidate_node(leader_id);
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        Err(_) => {
                            last_error = Some(Error::LinearizableReadDeadline);
                            self.rpc.invalidate_node(leader_id);
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    let context = Context::with_deadline(
                        Instant::now()
                            + deadline
                                .saturating_duration_since(Instant::now())
                                .min(Duration::from_secs(1)),
                    );
                    let read_log_id = match client.read_index(context).await {
                        Ok(Ok(log_id)) => log_id,
                        Ok(Err(source)) => {
                            last_error = Some(Error::LinearizableRead { source });
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        Err(source) => {
                            last_error = Some(Error::RpcError { source });
                            self.rpc.invalidate_node(leader_id);
                            sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    self.raft
                        .wait(Some(
                            deadline
                                .saturating_duration_since(Instant::now())
                                .min(Duration::from_secs(1)),
                        ))
                        .applied_index_at_least(
                            Some(read_log_id.index()),
                            "cluster API read barrier",
                        )
                        .await
                        .context(ReadBarrierSnafu)?;
                    return Ok(self.local_application_state());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
