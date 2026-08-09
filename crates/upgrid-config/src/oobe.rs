use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{fs, io};

use serde::{Deserialize, Serialize};

use crate::{AppResult, durable};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OobePhase {
    Cluster,
    Channel,
    Target,
    Complete,
}

impl OobePhase {
    pub fn path(self) -> &'static str {
        match self {
            Self::Cluster => "/setup",
            Self::Channel => "/setup/channel",
            Self::Target => "/setup/target",
            Self::Complete => "/",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Cluster => Self::Channel,
            Self::Channel => Self::Target,
            Self::Target | Self::Complete => Self::Complete,
        }
    }

    fn parse(value: &str) -> io::Result<Self> {
        match value.trim() {
            "cluster" => Ok(Self::Cluster),
            "channel" => Ok(Self::Channel),
            "target" => Ok(Self::Target),
            "complete" => Ok(Self::Complete),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid OOBE phase",
            )),
        }
    }
}

impl std::fmt::Display for OobePhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Cluster => "cluster",
            Self::Channel => "channel",
            Self::Target => "target",
            Self::Complete => "complete",
        })
    }
}

#[derive(Clone)]
pub struct Oobe {
    path: PathBuf,
    phase: Arc<Mutex<OobePhase>>,
}

impl Oobe {
    pub fn open(data_dir: &Path) -> AppResult<Self> {
        let path = data_dir.join("oobe-state");
        let phase = match fs::read_to_string(&path) {
            Ok(value) => OobePhase::parse(&value)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => OobePhase::Cluster,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            phase: Arc::new(Mutex::new(phase)),
        })
    }

    pub fn phase(&self) -> OobePhase {
        *self.phase.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub fn set(&self, phase: OobePhase) -> AppResult<()> {
        durable::replace(&self.path, phase.to_string().as_bytes())?;
        *self.phase.lock().unwrap_or_else(|error| error.into_inner()) = phase;
        Ok(())
    }

    pub fn advance(&self) -> AppResult<OobePhase> {
        let next = self.phase().next();
        self.set(next)?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn phase_survives_reopen_and_advances_in_order() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let oobe = Oobe::open(&directory).unwrap();
        assert_eq!(oobe.phase(), OobePhase::Cluster);
        assert_eq!(oobe.advance().unwrap(), OobePhase::Channel);
        assert_eq!(Oobe::open(&directory).unwrap().phase(), OobePhase::Channel);
        assert_eq!(oobe.advance().unwrap(), OobePhase::Target);
        assert_eq!(oobe.advance().unwrap(), OobePhase::Complete);
        assert_eq!(oobe.advance().unwrap(), OobePhase::Complete);
        fs::remove_dir_all(directory).unwrap();
    }
}
