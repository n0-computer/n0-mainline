use dht::Dht;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let client = Dht::server().await.unwrap();

    client.bootstrapped().await;

    let info = client.info().await;

    println!("{info:?}");

    let client = Dht::builder()
        .bootstrap(&[info.local_addr()])
        .build()
        .await
        .unwrap();

    client.bootstrapped().await;

    let info = client.info().await;

    println!("Bootstrapped using local node. {info:?}");
}
