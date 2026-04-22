use std::{str::FromStr, time::Instant};

use dht::{Dht, Id, SecretKey};

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// info_hash to announce a peer on
    infohash: String,
    /// Mutable data public key.
    secret_key: String,
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

    // Usually you want to create the info hash from hashing the concatenation
    // of the topic you are interested in and a namespacing based on the overlay
    // network you are using, and any other diffrentiators to filter out peers
    // you can't or don't want to connect to by accident.
    let info_hash = Id::from_str(cli.infohash.as_str()).expect("invalid infohash");

    let dht = Dht::client().await.unwrap();

    let signer: SecretKey = cli.secret_key.parse().expect("Invalid secret key");

    println!(
        "\nAnnouncing signed peer {} on an infohash: {} ...\n",
        to_hex(signer.public().as_bytes()),
        cli.infohash,
    );

    println!("\n=== COLD QUERY ===");
    announce(&dht, info_hash, &signer).await;

    println!("\n=== SUBSEQUENT QUERY ===");
    announce(&dht, info_hash, &signer).await;
}

async fn announce(dht: &Dht, info_hash: Id, signer: &SecretKey) {
    let start = Instant::now();

    dht.announce_signed_peer(info_hash, signer)
        .await
        .expect("announce_peer failed");

    println!(
        "Announced peer in {:?} seconds",
        start.elapsed().as_secs_f32()
    );
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{:02x}", byte)).collect()
}
