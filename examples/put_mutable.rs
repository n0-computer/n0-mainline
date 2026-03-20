use std::{convert::TryFrom, time::Instant};

use dht::{Dht, MutableItem, SecretKey};

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Mutable data public key.
    secret_key: String,
    /// Value to store on the DHT
    value: String,
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

    let dht = Dht::client().await.unwrap();

    let signer = from_hex(cli.secret_key);

    println!(
        "\nStoring mutable data: \"{}\" for public_key: {}  ...",
        cli.value,
        to_hex(signer.public().as_bytes())
    );

    println!("\n=== COLD QUERY ===");
    put(&dht, &signer, cli.value.as_bytes(), None).await;

    println!("\n=== SUBSEQUENT QUERY ===");
    put(&dht, &signer, cli.value.as_bytes(), None).await;
}

async fn put(dht: &Dht, signer: &SecretKey, value: &[u8], salt: Option<&[u8]>) {
    let start = Instant::now();

    let (item, cas) = if let Some(most_recent) =
        dht.get_mutable_most_recent(signer.public().as_bytes(), salt).await
    {
        let mut new_value = most_recent.value().to_vec();

        println!(
            "Found older value {:?}, appending new value to the old...",
            new_value
        );

        new_value.extend_from_slice(value);

        let most_recent_seq = most_recent.seq();
        let new_seq = most_recent_seq + 1;

        println!("Found older seq {most_recent_seq} incremnting sequence to {new_seq}...",);

        (
            MutableItem::new(signer, &new_value, new_seq, salt),
            Some(most_recent_seq),
        )
    } else {
        (MutableItem::new(signer, value, 1, salt), None)
    };

    dht.put_mutable(item, cas).await.unwrap();

    println!(
        "Stored mutable data as {:?} in {:?} milliseconds",
        to_hex(signer.public().as_bytes()),
        start.elapsed().as_millis()
    );
}

fn from_hex(s: String) -> SecretKey {
    if s.len() % 2 != 0 {
        panic!("Number of Hex characters should be even");
    }

    let mut bytes = Vec::with_capacity(s.len() / 2);

    for i in 0..s.len() / 2 {
        let byte_str = &s[i * 2..(i * 2) + 2];
        let byte = u8::from_str_radix(byte_str, 16).expect("Invalid hex character");
        bytes.push(byte);
    }

    SecretKey::try_from(bytes.as_slice()).expect("Invalid signing key")
}

fn to_hex(bytes: &[u8]) -> String {
    let hex_chars: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();

    hex_chars
}
