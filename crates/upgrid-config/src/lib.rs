//! UpGrid runtime configuration, admission links, and deployment secrets.

mod admission;
mod app;
mod cli;
pub mod durable;
mod secret;

pub use admission::{Error as JoinLinkError, JoinLink};
pub use app::{Action, AppResult, Config, load_or_create_cipher, load_or_create_node_id, now_ms};
pub use secret::{Cipher, Error as CipherError, generate_join_token};
