use snafu::{ResultExt, Snafu};

use crate::domain::ApplicationState;

mod v2026_08_12_initial;
mod v2026_08_19_connectivity_alerts;
mod v2026_08_19_connectivity_degradation;
mod v2026_08_19_reachability;

pub(super) const CURRENT_VERSION: &str = v2026_08_19_connectivity_degradation::VERSION;

#[cfg(test)]
pub(super) const INITIAL_VERSION: &str = v2026_08_12_initial::VERSION;
#[cfg(test)]
pub(super) const REACHABILITY_VERSION: &str = v2026_08_19_reachability::VERSION;
#[cfg(test)]
pub(super) const CONNECTIVITY_ALERTS_VERSION: &str = v2026_08_19_connectivity_alerts::VERSION;

pub(super) fn snapshot(version: &str, payload: &[u8]) -> Result<ApplicationState, Error> {
    match version {
        v2026_08_12_initial::VERSION => {
            v2026_08_12_initial::snapshot(payload).context(DecodeSnafu {
                version: v2026_08_12_initial::VERSION,
            })
        }
        v2026_08_19_reachability::VERSION => {
            v2026_08_19_reachability::snapshot(payload).context(DecodeSnafu {
                version: v2026_08_19_reachability::VERSION,
            })
        }
        v2026_08_19_connectivity_alerts::VERSION => {
            v2026_08_19_connectivity_alerts::snapshot(payload).context(DecodeSnafu {
                version: v2026_08_19_connectivity_alerts::VERSION,
            })
        }
        v2026_08_19_connectivity_degradation::VERSION => {
            v2026_08_19_connectivity_degradation::snapshot(payload).context(DecodeSnafu {
                version: v2026_08_19_connectivity_degradation::VERSION,
            })
        }
        _ => UnsupportedVersionSnafu {
            version: version.to_owned(),
        }
        .fail(),
    }
}

#[derive(Debug, Snafu)]
pub(super) enum Error {
    #[snafu(display("unsupported state-machine version `{version}`"))]
    UnsupportedVersion { version: String },

    #[snafu(display("failed to decode state-machine version `{version}`: {source}"))]
    Decode {
        version: &'static str,
        source: postcard::Error,
    },
}
