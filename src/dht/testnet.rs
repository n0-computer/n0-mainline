use super::Dht;

/// Create a testnet of Dht nodes to run tests against instead of the real mainline network.
#[derive(Debug)]
pub struct Testnet {
    /// bootstrapping nodes for this testnet.
    pub bootstrap: Vec<String>,
    /// all nodes in this testnet
    pub nodes: Vec<Dht>,
}

#[allow(dead_code)]
impl Testnet {
    /// Create a new testnet with a certain size.
    ///
    /// Note: this network will be shutdown as soon as this struct
    /// gets dropped, if you want the network to be `'static`, then
    /// you should call [Self::leak].
    ///
    /// This will await until all nodes are [bootstrapped][Dht::bootstrapped].
    pub async fn new(count: usize) -> Result<Testnet, std::io::Error> {
        let testnet = Testnet::new_inner(count, None).await?;

        for node in &testnet.nodes {
            node.bootstrapped().await?;
        }

        Ok(testnet)
    }

    async fn new_inner(
        count: usize,
        bootstrap: Option<Vec<String>>,
    ) -> Result<Testnet, std::io::Error> {
        let mut nodes: Vec<Dht> = vec![];
        let mut bootstrap = bootstrap.unwrap_or_default();

        for i in 0..count {
            let node = Dht::builder()
                .mode(crate::Mode::Server)
                .bootstrap(&bootstrap)
                .build()?;

            if i == 0 {
                let info = node.info().await?;
                let addr = info.local_addr();

                bootstrap.push(format!("127.0.0.1:{}", addr.port()));
            }

            nodes.push(node);
        }

        let testnet = Self { bootstrap, nodes };

        Ok(testnet)
    }

    /// By default as soon as this testnet gets dropped,
    /// all the nodes get dropped and the entire network is shutdown.
    ///
    /// This method uses [Box::leak] to keep nodes running, which is
    /// useful if you need to keep running the testnet in the process
    /// even if this struct gets dropped.
    pub fn leak(&self) {
        for node in self.nodes.clone() {
            Box::leak(Box::new(node));
        }
    }
}
