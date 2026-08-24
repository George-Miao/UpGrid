mod discovery;

use std::collections::{BTreeMap, BTreeSet};

pub(crate) use discovery::start as start_discovery;
use snafu::ResultExt;
use upgrid_config::{
    Cipher, Config, JoinIntent, Oobe, OobePhase, QuicCaKey, load_discovery_urls,
    load_or_create_cipher, load_or_create_node_id, load_or_create_node_name,
    load_or_create_quic_ca_key, load_pending_join, load_reachable_addresses, now_ms,
    remove_pending_join, store_discovery_urls, store_pending_join, store_reachable_addresses,
};
use upgrid_raft::domain::{Command, IdentityId, OperatorIdentity, PasswordVerifier};
use upgrid_raft::{Node, NodeNetworkConfig, ReachableAddress};
use uuid::Uuid;

use crate::error::{DataDirectorySnafu, Error, Result};

pub struct Ready {
    pub config: Config,
    pub node: Node,
    pub cipher: Cipher,
    pub quic_ca_key: QuicCaKey,
    pub node_name: String,
    pub oobe: Oobe,
    pub startup_warning: Option<String>,
    pub bootstrapping: bool,
}

pub async fn prepare(mut config: Config) -> Result<Ready> {
    std::fs::create_dir_all(&config.data_dir).context(DataDirectorySnafu {
        path: config.data_dir.clone(),
    })?;
    if config.discovery_urls_explicit {
        store_discovery_urls(&config.data_dir, &config.discovery_urls)?;
    } else if let Some(stored) = load_discovery_urls(&config.data_dir)? {
        config.discovery_urls = stored;
        config.discovery_urls_explicit = true;
    }
    if !config.reachable_addresses_explicit
        && let Some(stored) = load_reachable_addresses(&config.data_dir)?
    {
        config.reachable_addresses = stored;
        config.reachable_addresses_explicit = true;
    }
    let node_id = load_or_create_node_id(&config.data_dir)?;
    let mut reachable_addresses = config.reachable_addresses.clone();
    if config.reachable_addresses_explicit {
        store_reachable_addresses(&config.data_dir, &config.reachable_addresses)?;
    }
    let mut node_name =
        load_or_create_node_name(&config.data_dir, config.node_name.as_deref(), node_id)?;
    let oobe = Oobe::open(&config.data_dir)?;
    let pending_join = load_pending_join(&config.data_dir)?;
    let manual_cipher = config
        .deployment_key
        .take()
        .map(|key| Cipher::parse(&key))
        .transpose()?;
    let manual_quic_ca_key = config
        .quic_ca_key
        .take()
        .map(|key| QuicCaKey::parse(&key))
        .transpose()?;

    let persisted_members = Node::data_membership_addresses(&config.data_dir, node_id)?;
    if let Some(persisted_members) = persisted_members {
        let discovered_addresses = discovery::addresses(&config.discovery_urls).await;
        let cipher = load_or_create_cipher(&config.data_dir, manual_cipher.as_ref(), false)?;
        let quic_ca_key =
            load_or_create_quic_ca_key(&config.data_dir, manual_quic_ca_key.as_ref(), &cipher)?;
        let node = Node::open(
            node_id,
            NodeNetworkConfig::new(
                config.local_addresses.clone(),
                reachable_addresses.clone(),
                config.reachable_addresses_explicit,
                discovered_addresses.clone(),
            ),
            &config.data_dir,
            &cipher,
            &quic_ca_key,
        )
        .await?;
        let (startup_warning, complete_oobe) = if node.has_membership() {
            let warning = config
                .join
                .as_ref()
                .and_then(|link| ignored_join_warning(&persisted_members, link));
            let complete_oobe = pending_join
                .as_ref()
                .is_none_or(|pending| pending.complete_oobe);
            (warning, complete_oobe)
        } else {
            let (link, complete_oobe) = match config.join.take() {
                Some(JoinIntent::Valid(link)) => (link, true),
                Some(JoinIntent::Invalid) => return Err(Error::JoinTokenInvalid),
                None => {
                    let pending = pending_join.ok_or(Error::IncompleteAdmission)?;
                    (Box::new(pending.link), pending.complete_oobe)
                }
            };
            if link.cipher().encoded() != cipher.encoded() {
                return Err(Error::JoinDeploymentKeyMismatch);
            }
            if link.quic_ca_key() != &quic_ca_key {
                return Err(Error::JoinQuicCaKeyMismatch);
            }
            store_pending_join(&config.data_dir, &link, complete_oobe)?;
            node.join(link.issuer_node_id(), link.remote().as_str(), link.token())
                .await?;
            (None, complete_oobe)
        };
        node.retry_reachability_publication();
        seed_initial_identity(&node, &config.username, &config.password).await?;
        oobe.set(if complete_oobe {
            OobePhase::Complete
        } else {
            OobePhase::Channel
        })?;
        remove_pending_join(&config.data_dir)?;
        config.node_name = Some(node_name.clone());
        return Ok(Ready {
            config,
            node,
            cipher,
            quic_ca_key,
            node_name,
            oobe,
            startup_warning,
            bootstrapping: false,
        });
    }

    let explicit = config.join.is_some()
        || config.new_cluster
        || pending_join
            .as_ref()
            .is_some_and(|pending| pending.complete_oobe);
    let join = config.join.take().or_else(|| {
        (!config.new_cluster)
            .then_some(pending_join)
            .flatten()
            .map(|pending| JoinIntent::Valid(Box::new(pending.link)))
    });
    let choice = match join {
        Some(JoinIntent::Valid(link)) => upgrid_api::OobeChoice::Join {
            node_name: node_name.clone(),
            link,
            network: upgrid_api::OobeNetworkSources {
                reachable_addresses: BTreeSet::new(),
                reachable_addresses_explicit: false,
                discovery_urls: BTreeSet::new(),
                discovery_urls_explicit: false,
            },
        },
        Some(JoinIntent::Invalid) => {
            return Err(Error::JoinTokenInvalid);
        }
        None if config.new_cluster => upgrid_api::OobeChoice::NewCluster {
            node_name: node_name.clone(),
            admin_username: config.username.clone(),
            admin_password: config.password.clone(),
            network: upgrid_api::OobeNetworkSources {
                reachable_addresses: BTreeSet::new(),
                reachable_addresses_explicit: false,
                discovery_urls: BTreeSet::new(),
                discovery_urls_explicit: false,
            },
        },
        None => upgrid_api::wait_for_oobe(&config, &node_name)?,
    };
    let (join, initial_identity, chosen_network) = match choice {
        upgrid_api::OobeChoice::NewCluster {
            node_name: chosen,
            admin_username,
            admin_password,
            network,
        } => {
            node_name = chosen;
            (None, Some((admin_username, admin_password)), network)
        }
        upgrid_api::OobeChoice::Join {
            node_name: chosen,
            link,
            network,
        } => {
            node_name = chosen;
            (Some(*link), None, network)
        }
    };
    config.reachable_addresses_explicit = merge_reachable_addresses(
        &mut reachable_addresses,
        config.reachable_addresses_explicit,
        chosen_network.reachable_addresses_explicit,
        chosen_network.reachable_addresses,
    );
    config.reachable_addresses = reachable_addresses.clone();
    if config.reachable_addresses_explicit {
        store_reachable_addresses(&config.data_dir, &config.reachable_addresses)?;
    }
    config.discovery_urls_explicit = merge_discovery_urls(
        &mut config.discovery_urls,
        config.discovery_urls_explicit,
        chosen_network.discovery_urls_explicit,
        chosen_network.discovery_urls,
    );
    if config.discovery_urls_explicit {
        store_discovery_urls(&config.data_dir, &config.discovery_urls)?;
    }
    let discovered_addresses = discovery::addresses(&config.discovery_urls).await;
    let bootstrapping = join.is_none();
    let configured_cipher = match (join.as_ref(), manual_cipher) {
        (Some(link), Some(manual)) if link.cipher().encoded() != manual.encoded() => {
            return Err(Error::JoinDeploymentKeyMismatch);
        }
        (Some(link), _) => Some(link.cipher().clone()),
        (None, manual) => manual,
    };
    let configured_quic_ca_key = match (join.as_ref(), manual_quic_ca_key) {
        (Some(link), Some(manual)) if link.quic_ca_key() != &manual => {
            return Err(Error::JoinQuicCaKeyMismatch);
        }
        (Some(link), _) => Some(link.quic_ca_key().clone()),
        (None, manual) => manual,
    };
    if let Some(link) = &join {
        store_pending_join(&config.data_dir, link, explicit)?;
    }
    let cipher =
        load_or_create_cipher(&config.data_dir, configured_cipher.as_ref(), join.is_some())?;
    let quic_ca_key =
        load_or_create_quic_ca_key(&config.data_dir, configured_quic_ca_key.as_ref(), &cipher)?;
    let node = Node::open(
        node_id,
        NodeNetworkConfig::new(
            config.local_addresses.clone(),
            reachable_addresses,
            config.reachable_addresses_explicit,
            discovered_addresses,
        ),
        &config.data_dir,
        &cipher,
        &quic_ca_key,
    )
    .await?;
    if let Some(link) = join {
        node.join(link.issuer_node_id(), link.remote().as_str(), link.token())
            .await?;
    } else {
        node.start_cluster().await?;
    }
    if let Some((username, password)) = initial_identity {
        seed_initial_identity(&node, &username, &password).await?;
    }
    oobe.set(if explicit {
        OobePhase::Complete
    } else {
        OobePhase::Channel
    })?;
    remove_pending_join(&config.data_dir)?;
    config.node_name = Some(node_name.clone());
    Ok(Ready {
        config,
        node,
        cipher,
        quic_ca_key,
        node_name,
        oobe,
        startup_warning: None,
        bootstrapping,
    })
}

fn merge_reachable_addresses(
    current: &mut BTreeSet<ReachableAddress>,
    current_is_explicit: bool,
    additions_are_explicit: bool,
    additions: BTreeSet<ReachableAddress>,
) -> bool {
    if !additions_are_explicit {
        return current_is_explicit;
    }
    if !current_is_explicit {
        current.clear();
    }
    current.extend(additions);
    true
}

fn merge_discovery_urls(
    current: &mut BTreeSet<url::Url>,
    current_is_explicit: bool,
    additions_are_explicit: bool,
    additions: BTreeSet<url::Url>,
) -> bool {
    if !additions_are_explicit {
        return current_is_explicit;
    }
    if !current_is_explicit {
        current.clear();
    }
    current.extend(additions);
    true
}

async fn seed_initial_identity(node: &Node, username: &str, password: &str) -> Result<()> {
    if !node.local_application_state().identities.is_empty() {
        return Ok(());
    }
    let identity = OperatorIdentity {
        id: IdentityId(Uuid::now_v7()),
        username: username.to_owned(),
        password: PasswordVerifier::create(password)?,
        auth_version: 1,
        created_at_ms: now_ms(),
    };
    if let Err(error) = node.apply(Command::CreateIdentity(identity)).await
        && node.local_application_state().identities.is_empty()
    {
        return Err(error.into());
    }
    Ok(())
}

fn ignored_join_warning(
    persisted_members: &BTreeMap<Uuid, BTreeSet<ReachableAddress>>,
    configured: &JoinIntent,
) -> Option<String> {
    let JoinIntent::Valid(link) = configured else {
        return Some(
            "Configured join token is invalid and was ignored because this node already belongs \
             to a cluster."
                .to_owned(),
        );
    };
    let Ok(remote) = ReachableAddress::new(link.remote().clone()) else {
        return Some(
            "Configured join token cannot identify a cluster member and was ignored because this \
             node already belongs to a cluster."
                .to_owned(),
        );
    };
    let issuer_matches = persisted_members
        .get(&link.issuer_node_id())
        .is_some_and(|addresses| addresses.contains(&remote));
    if issuer_matches {
        return None;
    }
    Some(format!(
        "Configured join token issuer {} does not own {remote} in this node's current cluster. \
         The token was ignored.",
        link.issuer_node_id(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_address_replaces_only_non_explicit_addresses() {
        let initial = ReachableAddress::parse("up://initial.example:11451").unwrap();
        let addition = ReachableAddress::parse("up://node.example:11451").unwrap();
        let mut implicit = BTreeSet::from([initial.clone()]);

        assert!(merge_reachable_addresses(
            &mut implicit,
            false,
            true,
            BTreeSet::from([addition.clone()]),
        ));
        assert_eq!(implicit, BTreeSet::from([addition.clone()]));

        let mut cleared = BTreeSet::from([initial.clone()]);
        assert!(merge_reachable_addresses(
            &mut cleared,
            false,
            true,
            BTreeSet::new(),
        ));
        assert!(cleared.is_empty());

        let mut explicit = BTreeSet::from([initial.clone()]);
        assert!(merge_reachable_addresses(
            &mut explicit,
            true,
            true,
            BTreeSet::from([addition.clone()]),
        ));
        assert_eq!(explicit, BTreeSet::from([initial, addition]));
    }

    #[test]
    fn configured_address_accepts_a_translated_port() {
        let address = ReachableAddress::parse("up://translated.example:443").unwrap();

        assert_eq!(address.port(), 443);
    }

    #[test]
    fn setup_discovery_urls_extend_configured_urls() {
        let configured: url::Url = "https://config.example/nodes".parse().unwrap();
        let addition: url::Url = "https://setup.example/nodes".parse().unwrap();
        let mut urls = BTreeSet::from([configured.clone()]);

        assert!(merge_discovery_urls(
            &mut urls,
            true,
            true,
            BTreeSet::from([addition.clone()]),
        ));
        assert_eq!(urls, BTreeSet::from([configured.clone(), addition]));

        let mut implicit = BTreeSet::from([configured]);
        assert!(merge_discovery_urls(
            &mut implicit,
            false,
            true,
            BTreeSet::new(),
        ));
        assert!(implicit.is_empty());
    }

    #[test]
    fn restart_join_token_must_match_issuer_and_address() {
        let issuer = Uuid::from_u128(1);
        let other = Uuid::from_u128(2);
        let address = ReachableAddress::parse("up://node.example:11451").unwrap();
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
        let quic_ca_key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
        let link = upgrid_config::JoinLink::issue(
            &address.to_string(),
            issuer,
            &cipher,
            &quic_ca_key,
            "token".to_owned(),
        )
        .unwrap();
        let intent = JoinIntent::Valid(Box::new(link));

        let matching = BTreeMap::from([(issuer, BTreeSet::from([address.clone()]))]);
        assert_eq!(ignored_join_warning(&matching, &intent), None);

        let wrong_issuer = BTreeMap::from([(other, BTreeSet::from([address]))]);
        assert!(ignored_join_warning(&wrong_issuer, &intent).is_some());
    }
}
