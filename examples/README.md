# Examples

## Core API Examples
These examples demonstrate the main functionality of the Mainline DHT library:

### Setup
```sh
# Bootstrap a DHT node
cargo run --example bootstrap

# Implement a custom request filter
cargo run --example request_filter

# Cache and reuse bootstrap nodes
cargo run --example cache_bootstrap

# Advanced logging configuration
cargo run --example logging
```

### Monitor a public server

Run from a machine with a reachable UDP port. Replace `YOUR_PUBLIC_IPV4` with
its actual external IPv4 address and allow inbound UDP on port 6881 (or use
`--port` to choose another port):

```sh
cargo run --release --example server_monitor -- --public-ip YOUR_PUBLIC_IPV4 | tee server-monitor.log
```

The server runs for 24 hours, reporting every 60 seconds. Use `--duration-secs`
and `--interval-secs` to change these, or Ctrl-C to stop early. It bootstraps and
maintains its routing table automatically; the monitor adds no application lookups.

Each report includes total routing-table entries, non-stale entries, addresses
added/removed since the previous report, incoming parsed queries, distinct querying
IPs, and cumulative queries. Query counts and IP counts cover the reporting interval.
Distinct IP tracking is capped at 10,000 per interval; `query_ips_capped=true` means
the reported number is a lower bound. These are observed source addresses, not
verified identities. Address turnover comes from snapshots and can miss changes
between reports. The first report counts all visible non-stale entries as additions.

Watch for a populated, stable table and sustained incoming queries. Table size alone
does not establish inbound reachability. Bucket occupancy and exact node ages are
not exposed by the current API. Diagnostics go to stderr; set `RUST_LOG=info` for
more logging. Use `--bootstrap host:port,...` to override the default bootstrap hosts.

### Anounce/GET Peers (to announce Ip/port addresses)
```sh
# Announce as a peer
cargo run --example announce_peer <40 bytes hex info_hash>

# Find peers
cargo run --example get_peers <40 bytes hex info_hash>
```

### PUT/GET Arbitrary Immutable values.
```sh
# Store immutable data
cargo run --example put_immutable <string>

# Retrieve immutable data
cargo run --example get_immutable <40 bytes hex target from put_immutable>
```

### PUT/GET Arbitrary Mutable items.
```sh

# Store mutable data
cargo run --example put_mutable <64 bytes hex secret_key> <string>

# Retrieve mutable data
cargo run --example get_mutable <40 bytes hex target from put_mutable>
```

---

## Analysis & Research Tools
These examples are for DHT network analysis and research purposes:

> Note: These tools are not part of the main API and are provided for curiosity/research only.

```sh
# Analyze DHT node distribution
cargo run --example count_ips_close_to_key

# Estimate DHT size (Mark-Recapture method)
cargo run --example mark_recapture_dht

# Measure DHT network size
cargo run --example measure_dht
```
