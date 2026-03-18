use std::time::Instant;

#[tokio::main]
async fn main() {
    let start = Instant::now();
    mainline::Testnet::new(100).await.unwrap();
    println!("{:?}", start.elapsed());
}
