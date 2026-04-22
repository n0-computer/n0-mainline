use std::{str::FromStr, time::Instant};

use iroh_mainline::{Dht, Id};

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// info_hash to announce a peer on
    infohash: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let info_hash = Id::from_str(cli.infohash.as_str()).expect("invalid infohash");

    let dht = Dht::client().await.unwrap();

    println!("\nAnnouncing peer on an infohash: {} ...\n", cli.infohash);

    println!("\n=== COLD QUERY ===");
    announce(&dht, info_hash).await;

    println!("\n=== SUBSEQUENT QUERY ===");
    announce(&dht, info_hash).await;
}

async fn announce(dht: &Dht, info_hash: Id) {
    let start = Instant::now();

    dht.announce_peer(info_hash, Some(6991))
        .await
        .expect("announce_peer failed");

    println!(
        "Announced peer in {:?} seconds",
        start.elapsed().as_secs_f32()
    );
}
