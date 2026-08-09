use std::path::Path;
use std::{fs, io};

use uuid::Uuid;

use crate::{AppResult, durable};

const ADJECTIVES: [&str; 24] = [
    "amber", "brave", "bright", "calm", "clever", "cool", "eager", "fair", "gentle", "happy",
    "kind", "lively", "merry", "nimble", "proud", "quiet", "rapid", "steady", "sunny", "swift",
    "tidy", "vivid", "warm", "wise",
];
const NOUNS: [&str; 24] = [
    "badger", "bear", "cedar", "comet", "crane", "dolphin", "falcon", "finch", "fox", "heron",
    "lynx", "maple", "otter", "owl", "panda", "pine", "raven", "seal", "sparrow", "tiger", "tulip",
    "whale", "willow", "wolf",
];

pub fn friendly_node_name(id: Uuid) -> String {
    let bytes = id.as_bytes();
    let adjective = ADJECTIVES[usize::from(bytes[10]) % ADJECTIVES.len()];
    let noun = NOUNS[usize::from(bytes[11]) % NOUNS.len()];
    format!("{adjective}-{noun}")
}

pub fn load_or_create_node_name(
    data_dir: &Path,
    configured: Option<&str>,
    id: Uuid,
) -> AppResult<String> {
    let path = data_dir.join("node-name");
    if let Some(configured) = configured {
        let name = validate(configured)?;
        durable::replace(&path, name.as_bytes())?;
        return Ok(name);
    }
    match fs::read_to_string(&path) {
        Ok(name) => validate(&name),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let name = friendly_node_name(id);
            durable::replace(&path, name.as_bytes())?;
            Ok(name)
        }
        Err(error) => Err(error.into()),
    }
}

fn validate(name: &str) -> AppResult<String> {
    let name = name.trim();
    if name.is_empty() || name.len() > 64 || name.chars().any(char::is_control) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node name must contain 1 to 64 printable characters",
        )
        .into());
    }
    Ok(name.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_name_is_friendly_and_stable() {
        let id = Uuid::parse_str("019fe5a5-b5d6-7570-9b4d-31d8a6a8ff64").unwrap();
        let name = friendly_node_name(id);
        assert_eq!(name, friendly_node_name(id));
        assert_eq!(name.split('-').count(), 2);
    }

    #[test]
    fn configured_name_replaces_the_generated_name() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let id = Uuid::now_v7();
        let generated = load_or_create_node_name(&directory, None, id).unwrap();
        let configured = load_or_create_node_name(&directory, Some("edge-shanghai"), id).unwrap();
        assert_ne!(generated, configured);
        assert_eq!(configured, "edge-shanghai");
        assert_eq!(
            load_or_create_node_name(&directory, None, id).unwrap(),
            configured
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
