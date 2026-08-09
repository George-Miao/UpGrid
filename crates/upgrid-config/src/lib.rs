//! UpGrid runtime configuration, admission links, and deployment secrets.

mod admission;
mod app;
mod cli;
pub mod durable;
mod node_name;
mod oobe;
mod secret;

pub use admission::{Error as JoinLinkError, JoinLink};
pub use app::{
    Action, AppResult, Config, JoinIntent, load_or_create_cipher, load_or_create_node_id, now_ms,
};
pub use node_name::{friendly_node_name, load_or_create_node_name, store_node_name};
pub use oobe::{Oobe, OobePhase};
pub use secret::{Cipher, Error as CipherError, generate_join_token};
