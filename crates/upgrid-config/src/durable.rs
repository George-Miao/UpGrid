//! Durable, atomic configuration-file replacement.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Atomically replaces a file after flushing its new contents to stable
/// storage.
pub fn replace(path: &Path, contents: &[u8]) -> io::Result<()> {
    replace_with_mode(path, contents, false)
}

pub fn replace_private(path: &Path, contents: &[u8]) -> io::Result<()> {
    replace_with_mode(path, contents, true)
}

pub fn remove(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_parent(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn replace_with_mode(path: &Path, contents: &[u8], private: bool) -> io::Result<()> {
    replace_with_mode_and(path, contents, private, || Ok(()))
}

fn replace_with_mode_and(
    path: &Path,
    contents: &[u8],
    private: bool,
    before_rename: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let result = (|| {
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        if private {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        #[cfg(not(unix))]
        let _ = private;
        let mut file = options.open(&temporary)?;
        file.write_all(contents)?;
        file.sync_all()?;
        drop(file);
        before_rename()?;
        fs::rename(&temporary, path)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
fn replace_with_fault(
    path: &Path,
    contents: &[u8],
    fault: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    replace_with_mode_and(path, contents, false, fault)
}

fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use uuid::Uuid;

    use super::{replace, replace_with_fault};

    #[test]
    fn replacement_is_visible_and_leaves_no_temporary_file() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("state");

        replace(&path, b"first").unwrap();
        replace(&path, b"second").unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"second");
        assert!(!directory.join("state.tmp").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failure_before_rename_preserves_the_committed_file() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("state");
        replace(&path, b"committed").unwrap();

        let result = replace_with_fault(&path, b"uncommitted", || {
            Err(std::io::Error::other("injected failure"))
        });

        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"committed");
        assert!(!directory.join("state.tmp").exists());
        fs::remove_dir_all(directory).unwrap();
    }
}
