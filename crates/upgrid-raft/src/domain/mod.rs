//! Replicated UpGrid model and commands.

mod alert;
mod apply;
mod assertion;
mod auth;
mod command;
mod config_value;
mod connectivity;
mod evaluation;
mod history;
pub(crate) mod map_as_entries;
mod model;
mod secret;
mod state;
mod target;
mod trash;

pub use alert::*;
pub use assertion::*;
pub use auth::*;
pub use command::*;
pub use config_value::*;
pub use history::*;
pub use model::*;
pub use state::{ApplicationState, ReadmissionRollback};
pub(crate) use state::{
    ApplicationStateV20260812, ExpiredJoinReservation, decode_v2026_08_12_application_state,
    decode_v2026_08_19_application_state, decode_v2026_08_19_connectivity_alerts_application_state,
};
#[cfg(test)]
pub(crate) use state::{
    encode_v2026_08_12_application_state, encode_v2026_08_19_application_state,
    encode_v2026_08_19_connectivity_alerts_application_state,
};
pub use target::*;
pub use trash::*;

#[cfg(test)]
mod tests;
