//! UpGrid runtime configuration, admission links, and deployment secrets.

mod admission;
mod app;
mod cli;
mod discovery;
pub mod durable;
mod error;
mod node_name;
mod oobe;
mod reachable_addresses;
mod secret;

pub use admission::{
    Error as JoinLinkError, JoinLink, PendingJoin, load_pending_join, remove_pending_join,
    store_pending_join,
};
pub use app::{
    Config, JoinIntent, LocalAddress, load_or_create_cipher, load_or_create_node_id,
    load_or_create_quic_ca_key, now_ms,
};
pub use cli::ConfigArgs;
pub use discovery::{
    MAX_DISCOVERY_SERVICES, is_supported_discovery_url, load_discovery_urls, store_discovery_urls,
};
pub use error::{Error, Result};
pub use node_name::{friendly_node_name, load_or_create_node_name, store_node_name};
pub use oobe::{Oobe, OobePhase};
pub use reachable_addresses::{
    ReachableAddress, ReachableAddressError, load as load_reachable_addresses,
    store as store_reachable_addresses,
};
pub use secret::{Cipher, Error as CipherError, QuicCaKey, generate_join_token};
