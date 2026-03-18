use mainline::Dht;

use tracing::Level;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let client = Dht::client().unwrap();

    client.bootstrapped().await;

    let info = client.info().await;

    println!("{:?}", info);
}
