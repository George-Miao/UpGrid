use upgrid_config::{
    AppResult, Cipher, Config, JoinIntent, Oobe, OobePhase, load_or_create_cipher,
    load_or_create_node_id, load_or_create_node_name,
};
use upgrid_raft::{Identity, Node, UpgridNode};

pub struct Ready {
    pub config: Config,
    pub node: Node,
    pub cipher: Cipher,
    pub node_name: String,
    pub oobe: Oobe,
    pub startup_warning: Option<String>,
    pub bootstrapping: bool,
}

pub async fn prepare(mut config: Config) -> AppResult<Ready> {
    std::fs::create_dir_all(&config.data_dir)?;
    let node_id = load_or_create_node_id(&config.data_dir)?;
    let mut node_name =
        load_or_create_node_name(&config.data_dir, config.node_name.as_deref(), node_id)?;
    let oobe = Oobe::open(&config.data_dir)?;
    let manual_cipher = config
        .secret_key
        .take()
        .map(|key| Cipher::parse(&key))
        .transpose()?;

    let persisted_members = Node::data_membership_urls(&config.data_dir)?;
    if !persisted_members.is_empty() {
        let cipher = load_or_create_cipher(&config.data_dir, manual_cipher.as_ref(), false)?;
        let identity = Identity::with_id(node_id, config.raft_url.as_str())?;
        let node = Node::open(identity, &config.data_dir, &cipher).await?;
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
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "configured Join Token is invalid",
            )
            .into());
        }
        None if config.new_cluster => upgrid_api::OobeChoice::NewCluster {
            node_name: node_name.clone(),
        },
        None => upgrid_api::wait_for_oobe(&config, &node_name)?,
    };
    let join = match choice {
        upgrid_api::OobeChoice::NewCluster { node_name: chosen } => {
            node_name = chosen;
            None
        }
        upgrid_api::OobeChoice::Join {
            node_name: chosen,
            link,
        } => {
            node_name = chosen;
            Some(*link)
        }
    };
    let bootstrapping = join.is_none();
    let configured_cipher = match (join.as_ref(), manual_cipher) {
        (Some(link), Some(manual)) if link.cipher().encoded() != manual.encoded() => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "configured deployment key does not match the Join Token",
            )
            .into());
        }
        (Some(link), _) => Some(link.cipher().clone()),
        (None, manual) => manual,
    };
    let cipher =
        load_or_create_cipher(&config.data_dir, configured_cipher.as_ref(), join.is_some())?;
    let identity = Identity::with_id(node_id, config.raft_url.as_str())?;
    let node = Node::open(identity, &config.data_dir, &cipher).await?;
    if let Some(link) = join {
        node.join(link.remote().clone(), link.token()).await?;
    } else {
        node.start_cluster().await?;
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
        node_name,
        oobe,
        startup_warning: None,
        bootstrapping,
    })
}

fn ignored_join_warning(
    persisted_members: &std::collections::BTreeSet<String>,
    configured: &JoinIntent,
) -> Option<String> {
    let JoinIntent::Valid(link) = configured else {
        return Some(
            "Configured Join Token is invalid and was ignored because this Node already belongs \
             to a Cluster."
                .to_owned(),
        );
    };
    let Ok(remote) = UpgridNode::new(link.remote().clone()).map(|node| node.to_string()) else {
        return Some(
            "Configured Join Token cannot identify a Cluster member and was ignored because this \
             Node already belongs to a Cluster."
                .to_owned(),
        );
    };
    if persisted_members.contains(&remote) {
        return None;
    }
    Some(format!(
        "Configured Join Token points to {remote}, which is not a member of this Node's current \
         Cluster. The token was ignored."
    ))
}
