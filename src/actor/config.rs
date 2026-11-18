use std::{net::Ipv4Addr, time::Duration};

pub const DEFAULT_BOOTSTRAP_NODES: [&str; 4] = [
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
    "relay.pkarr.org:6881",
];

use crate::ServerSettings;

#[derive(Debug, Clone)]
/// Dht Configurations
pub struct Config {
    /// Bootstrap nodes
    ///
    /// Defaults to [DEFAULT_BOOTSTRAP_NODES]
    pub bootstrap: Vec<String>,
    /// Explicit port to listen on.
    ///
    /// Defaults to None
    pub port: Option<u16>,
    /// Server to respond to incoming Requests
    pub server_settings: ServerSettings,
    /// The mode to run in.
    ///
    /// Defaults to client mode, without automatic promotion to server mode.
    pub mode: crate::Mode,
    /// Fixed timeout for individual outgoing DHT requests.
    pub request_timeout: Duration,
    /// A known public IPv4 address for this node to generate
    /// a secure node Id from according to [BEP_0042](https://www.bittorrent.org/beps/bep_0042.html)
    ///
    /// Defaults to None, where we depend on suggestions from responding nodes.
    pub public_ip: Option<Ipv4Addr>,

    // Testing helpers
    //
    /// Used to simulate a DHT that doesn't support `announce_signed_peers`
    #[cfg(test)]
    pub(crate) disable_announce_signed_peers: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootstrap: DEFAULT_BOOTSTRAP_NODES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            port: None,
            server_settings: Default::default(),
            mode: crate::Mode::Client,
            request_timeout: Duration::from_secs(5),
            public_ip: None,
            #[cfg(test)]
            disable_announce_signed_peers: false,
        }
    }
}
