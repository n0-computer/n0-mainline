//! Run a public DHT server and report routing-table health and incoming queries.
//!
//! cargo run --release --example server_monitor
//! Allow inbound UDP on the selected port (6881 by default).

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Parser;
use n0_mainline::{Dht, Mode, RequestFilter, RequestSpecific, ServerSettings};

// Bound monitoring memory even if many different source addresses send requests.
const MAX_QUERY_IPS: usize = 10_000;

#[derive(Debug, Parser)]
#[command(about = "Run an idle DHT server and periodically print health statistics")]
struct Args {
    /// Override public IPv4 discovery from DHT bootstrap replies.
    #[arg(long)]
    public_ip: Option<Ipv4Addr>,
    /// UDP port to listen on; allow this port through your firewall/NAT.
    #[arg(long, default_value_t = 6881, value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,
    /// Seconds between reports.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    interval_secs: u64,
    /// Run duration in seconds (default: 24 hours). Ctrl-C also stops the server.
    #[arg(long, default_value_t = 86400, value_parser = clap::value_parser!(u64).range(1..))]
    duration_secs: u64,
    /// Override the default bootstrap hosts (comma-separated host:port addresses).
    #[arg(long, value_delimiter = ',')]
    bootstrap: Vec<String>,
}

#[derive(Debug, Default)]
struct Incoming {
    queries: u64,
    ips: HashSet<Ipv4Addr>,
    ips_capped: bool,
}

#[derive(Debug, Default, Clone)]
struct Counter(Arc<Mutex<Incoming>>);

impl Counter {
    fn take(&self) -> Incoming {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

impl RequestFilter for Counter {
    fn allow_request(&self, _: &RequestSpecific, from: SocketAddrV4) -> bool {
        let mut incoming = self.0.lock().unwrap_or_else(|e| e.into_inner());
        incoming.queries += 1;
        if incoming.ips.len() < MAX_QUERY_IPS {
            incoming.ips.insert(*from.ip());
        } else if !incoming.ips.contains(from.ip()) {
            incoming.ips_capped = true;
        }
        true
    }
}

#[derive(Default)]
struct Reports {
    previous: HashSet<String>,
    total_queries: u64,
}

impl Reports {
    async fn print(&mut self, dht: &Dht, counter: &Counter, start: Instant) -> anyhow::Result<()> {
        let info = dht.info().await?;
        let current: HashSet<_> = dht.to_bootstrap().await?.into_iter().collect();
        let incoming = counter.take();
        self.total_queries += incoming.queries;
        println!(
            "uptime_s={} routing_nodes={} nonstale_nodes={} added={} removed={} queries={} query_ips={} query_ips_capped={} total_queries={} observed_address={:?}",
            start.elapsed().as_secs(),
            info.routing_table_size(),
            current.len(),
            current.difference(&self.previous).count(),
            self.previous.difference(&current).count(),
            incoming.queries,
            incoming.ips.len(),
            incoming.ips_capped,
            self.total_queries,
            info.public_address(),
        );
        self.previous = current;
        Ok(())
    }
}

/// Bootstrap a temporary client to learn the public IP before choosing a server node ID.
async fn discover_public_ip(bootstrap: &[String]) -> anyhow::Result<Ipv4Addr> {
    let mut builder = Dht::builder();
    builder.port(0);
    if !bootstrap.is_empty() {
        builder.bootstrap(bootstrap);
    }
    let client = builder.build()?;
    tokio::time::timeout(Duration::from_secs(20), async {
        client.bootstrapped().await?;
        client
            .info()
            .await?
            .public_address()
            .map(|address| *address.ip())
            .context("DHT replies supplied no public IPv4 address; use --public-ip")
    })
    .await
    .context("Public IPv4 discovery timed out; use --public-ip")?
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let stop = Arc::new(tokio::sync::Notify::new());
    let notify = stop.clone();
    ctrlc::set_handler(move || notify.notify_one())?;

    let public_ip = match args.public_ip {
        Some(ip) => ip,
        None => {
            eprintln!("Discovering public IPv4 address from DHT bootstrap replies...");
            tokio::select! {
                _ = stop.notified() => return Ok(()),
                result = discover_public_ip(&args.bootstrap) => result?,
            }
        }
    };
    let counter = Counter::default();
    let mut builder = Dht::builder();
    builder
        .mode(Mode::Server)
        .public_ip(public_ip)
        .port(args.port)
        .server_settings(ServerSettings {
            filter: Box::new(counter.clone()),
            ..Default::default()
        });
    if !args.bootstrap.is_empty() {
        builder.bootstrap(&args.bootstrap);
    }
    let dht = builder.build()?;
    let info = dht.info().await?;
    println!(
        "node_id={} public_ip={} local_address={} interval_secs={} duration_secs={}",
        info.id(),
        public_ip,
        info.local_addr(),
        args.interval_secs,
        args.duration_secs,
    );

    // The actor bootstraps and maintains its routing table automatically.
    // This monitor only reads state; it does not generate application lookups.
    let start = Instant::now();
    let mut reports = Reports::default();
    let mut interval = tokio::time::interval(Duration::from_secs(args.interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let duration = tokio::time::sleep(Duration::from_secs(args.duration_secs));
    tokio::pin!(duration);
    loop {
        tokio::select! {
            _ = stop.notified() => {
                eprintln!("Stopping after Ctrl-C");
                break;
            }
            _ = &mut duration => {
                eprintln!("Run duration reached");
                break;
            }
            _ = interval.tick() => reports.print(&dht, &counter, start).await?,
        }
    }
    reports.print(&dht, &counter, start).await?;
    Ok(())
}
