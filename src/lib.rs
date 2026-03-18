#![doc = include_str!("../README.md")]
//!

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

mod common;
mod dht;
mod rpc;

pub use common::{Id, MutableItem, Node, RoutingTable};

pub use dht::{Dht, DhtBuilder, Testnet, TestnetBuilder};
pub use rpc::{
    messages::{MessageType, PutRequestSpecific, RequestSpecific},
    server::{RequestFilter, ServerSettings, MAX_INFO_HASHES, MAX_PEERS, MAX_VALUES},
    ClosestNodes, DEFAULT_REQUEST_TIMEOUT,
};

pub use ed25519_dalek::SigningKey;

pub mod errors {
    //! Exported errors
    pub use super::common::ErrorSpecific;
    pub use super::dht::PutMutableError;
    pub use super::rpc::{ConcurrencyError, PutError, PutQueryError};

    pub use super::common::DecodeIdError;
    pub use super::common::MutableError;
}
