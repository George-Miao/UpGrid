use snafu::{ResultExt, Snafu};

use crate::domain::ApplicationState;

mod v2026_08_12_initial;

pub(super) const CURRENT_VERSION: &str = v2026_08_12_initial::VERSION;

pub(super) fn snapshot(version: &str, payload: &[u8]) -> Result<ApplicationState, Error> {
    match version {
        v2026_08_12_initial::VERSION => {
            v2026_08_12_initial::snapshot(payload).context(DecodeSnafu {
                version: CURRENT_VERSION,
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
