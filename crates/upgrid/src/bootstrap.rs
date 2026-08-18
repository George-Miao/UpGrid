use snafu::ResultExt;
use upgrid_config::{
    Cipher, Config, JoinIntent, Oobe, OobePhase, QuicCaKey, load_or_create_cipher,
    load_or_create_node_id, load_or_create_node_name, load_or_create_quic_ca_key, now_ms,
};
use upgrid_raft::domain::{Command, IdentityId, OperatorIdentity, PasswordVerifier};
use upgrid_raft::{Identity, Node, UpgridNode};
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
    let node_id = load_or_create_node_id(&config.data_dir)?;
    let mut node_name =
        load_or_create_node_name(&config.data_dir, config.node_name.as_deref(), node_id)?;
    let oobe = Oobe::open(&config.data_dir)?;
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

    let persisted_members = Node::data_membership_urls(&config.data_dir)?;
    if !persisted_members.is_empty() {
        let cipher = load_or_create_cipher(&config.data_dir, manual_cipher.as_ref(), false)?;
        let quic_ca_key =
            load_or_create_quic_ca_key(&config.data_dir, manual_quic_ca_key.as_ref(), &cipher)?;
        let identity = Identity::with_id(node_id, config.raft_url.as_str())?;
        let node = Node::open(identity, &config.data_dir, &cipher, &quic_ca_key).await?;
        seed_initial_identity(&node, &config.username, &config.password).await?;
        let startup_warning = config
            .join
            .as_ref()
            .and_then(|link| ignored_join_warning(&persisted_members, link));
        oobe.set(OobePhase::Complete)?;
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

    let explicit = config.join.is_some() || config.new_cluster;
    let choice = match config.join.take() {
        Some(JoinIntent::Valid(link)) => upgrid_api::OobeChoice::Join {
            node_name: node_name.clone(),
            link,
        },
        Some(JoinIntent::Invalid) => {
            return Err(Error::JoinTokenInvalid);
        }
        None if config.new_cluster => upgrid_api::OobeChoice::NewCluster {
            node_name: node_name.clone(),
            admin_username: config.username.clone(),
            admin_password: config.password.clone(),
        },
        None => upgrid_api::wait_for_oobe(&config, &node_name)?,
    };
    let (join, initial_identity) = match choice {
        upgrid_api::OobeChoice::NewCluster {
            node_name: chosen,
            admin_username,
            admin_password,
        } => {
            node_name = chosen;
            (None, Some((admin_username, admin_password)))
        }
        upgrid_api::OobeChoice::Join {
            node_name: chosen,
            link,
        } => {
            node_name = chosen;
            (Some(*link), None)
        }
    };
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
    let cipher =
        load_or_create_cipher(&config.data_dir, configured_cipher.as_ref(), join.is_some())?;
    let quic_ca_key =
        load_or_create_quic_ca_key(&config.data_dir, configured_quic_ca_key.as_ref(), &cipher)?;
    let identity = Identity::with_id(node_id, config.raft_url.as_str())?;
    let node = Node::open(identity, &config.data_dir, &cipher, &quic_ca_key).await?;
    if let Some(link) = join {
        node.join(link.remote().as_str(), link.token()).await?;
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
    persisted_members: &std::collections::BTreeSet<String>,
    configured: &JoinIntent,
) -> Option<String> {
    let JoinIntent::Valid(link) = configured else {
        return Some(
            "Configured join token is invalid and was ignored because this node already belongs \
             to a cluster."
                .to_owned(),
        );
    };
    let Ok(remote) = UpgridNode::new(link.remote().clone()).map(|node| node.to_string()) else {
        return Some(
            "Configured join token cannot identify a cluster member and was ignored because this \
             node already belongs to a cluster."
                .to_owned(),
        );
    };
    if persisted_members.contains(&remote) {
        return None;
    }
    Some(format!(
        "Configured join token points to {remote}, which is not a member of this node's current \
         cluster. The token was ignored."
    ))
}
