use std::collections::BTreeSet;
use std::path::Path;
use std::{fs, io};

use snafu::ResultExt;
use url::Url;

use crate::error::{DiscoveryUrlSnafu, ReadSnafu, WriteSnafu};
use crate::{Error, Result, durable};

const FILE_NAME: &str = "discovery-urls";
pub const MAX_DISCOVERY_SERVICES: usize = 8;

pub fn is_supported_discovery_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

pub fn load_discovery_urls(data_dir: &Path) -> Result<Option<BTreeSet<Url>>> {
    let path = data_dir.join(FILE_NAME);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source).context(ReadSnafu { path }),
    };
    let urls: BTreeSet<Url> = contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let value = line.trim();
            let url: Url = value.parse().context(DiscoveryUrlSnafu {
                path: path.clone(),
                value: value.to_owned(),
            })?;
            if !is_supported_discovery_url(&url) {
                return Err(Error::DiscoveryUrlScheme {
                    path: path.clone(),
                    value: value.to_owned(),
                });
            }
            Ok(url)
        })
        .collect::<Result<_>>()?;
    if urls.len() > MAX_DISCOVERY_SERVICES {
        return Err(Error::TooManyDiscoveryUrls {
            path,
            count: urls.len(),
            limit: MAX_DISCOVERY_SERVICES,
        });
    }
    Ok(Some(urls))
}

pub fn store_discovery_urls(data_dir: &Path, urls: &BTreeSet<Url>) -> Result<()> {
    let path = data_dir.join(FILE_NAME);
    if urls.len() > MAX_DISCOVERY_SERVICES {
        return Err(Error::TooManyDiscoveryUrls {
            path,
            count: urls.len(),
            limit: MAX_DISCOVERY_SERVICES,
        });
    }
    if let Some(url) = urls.iter().find(|url| !is_supported_discovery_url(url)) {
        return Err(Error::DiscoveryUrlScheme {
            path,
            value: url.to_string(),
        });
    }
    let mut contents = String::new();
    for url in urls {
        contents.push_str(url.as_str());
        contents.push('\n');
    }
    durable::replace(&path, contents.as_bytes()).context(WriteSnafu { path })
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn stored_urls_survive_restart() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let urls = BTreeSet::from([
            "https://discovery.example/nodes".parse().unwrap(),
            "https://backup.example/nodes".parse().unwrap(),
        ]);

        store_discovery_urls(&directory, &urls).unwrap();

        assert_eq!(load_discovery_urls(&directory).unwrap(), Some(urls));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn explicit_urls_replace_stored_urls() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let old = BTreeSet::from(["https://old.example/nodes".parse().unwrap()]);
        let new = BTreeSet::from(["https://new.example/nodes".parse().unwrap()]);
        store_discovery_urls(&directory, &old).unwrap();

        store_discovery_urls(&directory, &new).unwrap();
        assert_eq!(load_discovery_urls(&directory).unwrap(), Some(new));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn explicit_empty_urls_clear_stored_urls() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let old = BTreeSet::from(["https://old.example/nodes".parse().unwrap()]);
        let empty = BTreeSet::new();
        store_discovery_urls(&directory, &old).unwrap();

        store_discovery_urls(&directory, &empty).unwrap();
        assert_eq!(load_discovery_urls(&directory).unwrap(), Some(empty),);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn stored_url_count_is_bounded() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let contents = (0..=MAX_DISCOVERY_SERVICES)
            .map(|index| format!("https://discovery-{index}.example/nodes"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(directory.join(FILE_NAME), contents).unwrap();

        assert!(matches!(
            load_discovery_urls(&directory),
            Err(Error::TooManyDiscoveryUrls { count, limit, .. })
                if count == MAX_DISCOVERY_SERVICES + 1 && limit == MAX_DISCOVERY_SERVICES
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn discovery_urls_reject_sensitive_components() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        for value in [
            "https://user:password@discovery.example/nodes",
            "https://discovery.example/nodes?token=secret",
            "https://discovery.example/nodes#secret",
        ] {
            let urls = BTreeSet::from([value.parse().unwrap()]);
            assert!(store_discovery_urls(&directory, &urls).is_err());
            assert!(!directory.join(FILE_NAME).exists());
        }
        fs::remove_dir_all(directory).unwrap();
    }
}
