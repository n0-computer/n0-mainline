# n0-mainline

Simple, robust, BitTorrent's [Mainline DHT](https://en.wikipedia.org/wiki/Mainline_DHT) implementation.

This is an iroh-flavored fork of [nuhvi/dht](https://github.com/nuhvi/dht): it is a support crate for [iroh](https://github.com/n0-computer/iroh) and integrates with the iroh ecosystem (e.g. [`iroh-base`](https://crates.io/crates/iroh-base) keys, async UDP via [`noq-udp`](https://crates.io/crates/noq-udp)). It does **not** depend on the `iroh` crate itself, so it can be used standalone. It does however require `noq-udp` for its socket layer.

The main purpose for which iroh uses n0-mainline is endpoint address lookup via BEP_0044.

## Getting started

Check the [Examples](https://github.com/n0-computer/n0-mainline/tree/main/examples).

## Features

### Client

Running as a client, means you can store and query for values on the DHT, but not accept any incoming requests.

```rust,ignore
use n0_mainline::{Dht, Mode};

let dht = Dht::client().await.unwrap();
```

Supported BEPs:
- [x] [BEP_0005 DHT Protocol](https://www.bittorrent.org/beps/bep_0005.html)
- [x] [BEP_0042 DHT Security extension](https://www.bittorrent.org/beps/bep_0042.html)
- [x] [BEP_0043 Read-only DHT Nodes](https://www.bittorrent.org/beps/bep_0043.html)
- [x] [BEP_0044 Storing arbitrary data in the DHT](https://www.bittorrent.org/beps/bep_0044.html)

This implementation also includes [measures against Vertical Sybil Attacks](https://github.com/Nuhvi/mainline/blob/main/docs/censorship-resistance.md#vertical-sybil-attacks).

### Server

Running as a server is the same as a client, but you also respond to incoming requests and serve as a routing and storing node, supporting the general routing of the DHT, and contributing to the storage capacity of the DHT.

```rust,ignore
use n0_mainline::{Dht, Mode};

let dht = Dht::builder().mode(Mode::Server).build()?; // or Dht::server()?
```

Supported BEPs:
- [x] [BEP_0005 DHT Protocol](https://www.bittorrent.org/beps/bep_0005.html)
- [x] [BEP_0042 DHT Security extension](https://www.bittorrent.org/beps/bep_0042.html)
- [x] [BEP_0043 Read-only DHT Nodes](https://www.bittorrent.org/beps/bep_0043.html)
- [x] [BEP_0044 Storing arbitrary data in the DHT](https://www.bittorrent.org/beps/bep_0044.html)

#### Rate limiting

The server implementation has no rate-limiting, you can run your own [request filter](./examples/request_filter.rs) and apply your custom rate-limiting. 
However, that limit/block will only apply _after_ parsing incoming messages, and it won't affect handling incoming responses.

### Client and server modes

Nodes start in client mode and do not automatically switch to server mode.
If you know your node is publicly reachable and will be long running, enable server mode
with `Dht::builder().mode(Mode::Server).build()`.
You can also provide a known public IPv4 address with `.public_ip(ip)` to derive the node ID
from that address.

Reported public addresses are observations from other nodes, not proof of inbound reachability.

## Acknowledgment

This implementation was possible thanks to [Webtorrent's Bittorrent-dht](https://github.com/webtorrent/bittorrent-dht) as a reference, 
and [Rustydht-lib](https://github.com/raptorswing/rustydht-lib) that saved me a lot of time, especially at the serialization and deserialization of Bencode messages.
