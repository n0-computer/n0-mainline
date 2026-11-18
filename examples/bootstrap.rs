use std::net::{Ipv4Addr, SocketAddrV4};

use n0_mainline::Dht;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let client = Dht::server()?;

    client.bootstrapped().await?;

    let info = client.info().await?;

    println!("{info:?}");

    let local = SocketAddrV4::new(Ipv4Addr::LOCALHOST, info.local_addr().port());
    let client = Dht::builder().bootstrap(&[local]).build()?;

    client.bootstrapped().await?;

    let info = client.info().await?;

    println!("Bootstrapped using local node. {info:?}");

    Ok(())
}
