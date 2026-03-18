//! Dht node.

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::{
    common::{
        hash_immutable, AnnouncePeerRequestArguments, FindNodeRequestArguments,
        GetPeersRequestArguments, GetValueRequestArguments, Id, MutableItem,
        PutImmutableRequestArguments, PutMutableRequestArguments, PutRequestSpecific,
    },
    rpc::{
        to_socket_address, ConcurrencyError, GetRequestSpecific, Info, PutError, PutQueryError,
        Response, Rpc,
    },
    Node, ServerSettings,
};

use crate::rpc::config::Config;

#[derive(Debug, Clone)]
/// Mainline Dht node.
pub struct Dht(mpsc::UnboundedSender<ActorMessage>);

#[derive(Debug, Default, Clone)]
/// A builder for the [Dht] node.
pub struct DhtBuilder(Config);

impl DhtBuilder {
    /// Set this node's server_mode.
    pub fn server_mode(&mut self) -> &mut Self {
        self.0.server_mode = true;

        self
    }

    /// Set a custom settings for the node to use at server mode.
    ///
    /// Defaults to [ServerSettings::default]
    pub fn server_settings(&mut self, server_settings: ServerSettings) -> &mut Self {
        self.0.server_settings = server_settings;

        self
    }

    /// Set bootstrapping nodes.
    pub fn bootstrap<T: ToSocketAddrs>(&mut self, bootstrap: &[T]) -> &mut Self {
        self.0.bootstrap = Some(to_socket_address(bootstrap));

        self
    }

    /// Add more bootstrap nodes to default bootstrapping nodes.
    ///
    /// Useful when you want to augment the default bootstrapping nodes with
    /// dynamic list of nodes you have seen in previous sessions.
    pub fn extra_bootstrap<T: ToSocketAddrs>(&mut self, extra_bootstrap: &[T]) -> &mut Self {
        let mut bootstrap = self.0.bootstrap.clone().unwrap_or_default();
        for address in to_socket_address(extra_bootstrap) {
            bootstrap.push(address);
        }
        self.0.bootstrap = Some(bootstrap);

        self
    }

    /// Remove the existing bootstrapping nodes, usually to create the first node in a new network.
    pub fn no_bootstrap(&mut self) -> &mut Self {
        self.0.bootstrap = Some(vec![]);

        self
    }

    /// Set an explicit port to listen on.
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.0.port = Some(port);

        self
    }

    /// A known public IPv4 address for this node to generate
    /// a secure node Id from according to [BEP_0042](https://www.bittorrent.org/beps/bep_0042.html)
    ///
    /// Defaults to depending on suggestions from responding nodes.
    pub fn public_ip(&mut self, public_ip: Ipv4Addr) -> &mut Self {
        self.0.public_ip = Some(public_ip);

        self
    }

    /// UDP socket request timeout duration.
    ///
    /// The longer this duration is, the longer queries take until they are deemeed "done".
    /// The shortet this duration is, the more responses from busy nodes we miss out on,
    /// which affects the accuracy of queries trying to find closest nodes to a target.
    ///
    /// Defaults to [crate::DEFAULT_REQUEST_TIMEOUT]
    pub fn request_timeout(&mut self, request_timeout: Duration) -> &mut Self {
        self.0.request_timeout = request_timeout;

        self
    }

    /// Set the address to bind to.
    ///
    /// Defaults to 0.0.0.0 (all interfaces).
    pub fn bind_address(&mut self, bind_address: Ipv4Addr) -> &mut Self {
        self.0.bind_address = Some(bind_address);

        self
    }

    /// Create a Dht node.
    pub fn build(&self) -> Result<Dht, std::io::Error> {
        Dht::new(self.0.clone())
    }
}

impl Dht {
    /// Create a new Dht node.
    ///
    /// Could return an error if it failed to bind to the specified
    /// port or other io errors while binding the udp socket.
    pub fn new(config: Config) -> Result<Self, std::io::Error> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let rpc = Rpc::new(config)?;
        let address = rpc.local_addr();
        info!(?address, "Mainline DHT listening");
        tokio::spawn(run(rpc, receiver));

        Ok(Dht(sender))
    }

    /// Returns a builder to edit settings before creating a Dht node.
    pub fn builder() -> DhtBuilder {
        DhtBuilder::default()
    }

    /// Create a new DHT client with default bootstrap nodes.
    pub fn client() -> Result<Self, std::io::Error> {
        Dht::builder().build()
    }

    /// Create a new DHT node that is running in [Server mode][DhtBuilder::server_mode] as
    /// soon as possible.
    ///
    /// You shouldn't use this option unless you are sure your
    /// DHT node is publicly accessible (not firewalled) _AND_ will be long running,
    /// and/or you are running your own local network for testing.
    ///
    /// If you are not sure, use [Self::client] and it will switch
    /// to server mode when/if these two conditions are met.
    pub fn server() -> Result<Self, std::io::Error> {
        Dht::builder().server_mode().build()
    }

    // === Getters ===

    /// Information and statistics about this [Dht] node.
    pub async fn info(&self) -> Info {
        let (tx, rx) = oneshot::channel();
        self.send(ActorMessage::Info(tx));

        rx.await.expect("actor task unexpectedly shutdown")
    }

    /// Turn this node's routing table to a list of bootstrapping nodes.
    pub async fn to_bootstrap(&self) -> Vec<String> {
        let (tx, rx) = oneshot::channel();
        self.send(ActorMessage::ToBootstrap(tx));

        rx.await.expect("actor task unexpectedly shutdown")
    }

    // === Public Methods ===

    /// Await until the bootstrapping query is done.
    ///
    /// Returns true if the bootstrapping was successful.
    pub async fn bootstrapped(&self) -> bool {
        let info = self.info().await;
        let nodes = self.find_node(*info.id()).await;

        !nodes.is_empty()
    }

    // === Find nodes ===

    /// Returns the closest 20 [secure](Node::is_secure) nodes to a target [Id].
    ///
    /// Mostly useful to crawl the DHT.
    ///
    /// The returned nodes are claims by other nodes, they may be lies, or may have churned
    /// since they were last seen, but haven't been pinged yet.
    ///
    /// You might need to ping them to confirm they exist, and responsive, or if you want to
    /// learn more about them like the client they are using, or if they support a given BEP.
    ///
    /// If you are trying to find the closest nodes to a target with intent to [Self::put],
    /// a request directly to these nodes (using `extra_nodes` parameter), then you should
    /// use [Self::get_closest_nodes] instead.
    pub async fn find_node(&self, target: Id) -> Box<[Node]> {
        let (tx, rx) = oneshot::channel();
        self.send(ActorMessage::Get(
            GetRequestSpecific::FindNode(FindNodeRequestArguments { target }),
            ResponseSender::ClosestNodes(tx),
        ));

        rx.await
            .expect("Query was dropped before sending a response, please open an issue.")
    }

    // === Peers ===

    /// Get peers for a given infohash.
    ///
    /// Note: each node of the network will only return a _random_ subset (usually 20)
    /// of the total peers it has for a given infohash, so if you are getting responses
    /// from 20 nodes, you can expect up to 400 peers in total, but if there are more
    /// announced peers on that infohash, you are likely to miss some, the logic here
    /// for Bittorrent is that any peer will introduce you to more peers through "peer exchange"
    /// so if you are implementing something different from Bittorrent, you might want
    /// to implement your own logic for gossipping more peers after you discover the first ones.
    pub fn get_peers(&self, info_hash: Id) -> GetStream<Vec<SocketAddrV4>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.send(ActorMessage::Get(
            GetRequestSpecific::GetPeers(GetPeersRequestArguments { info_hash }),
            ResponseSender::Peers(tx),
        ));

        GetStream(rx)
    }

    /// Announce a peer for a given infohash.
    ///
    /// The peer will be announced on this process IP.
    /// If explicit port is passed, it will be used, otherwise the port will be implicitly
    /// assumed by remote nodes to be the same ase port they received the request from.
    pub async fn announce_peer(
        &self,
        info_hash: Id,
        port: Option<u16>,
    ) -> Result<Id, PutQueryError> {
        let (port, implied_port) = match port {
            Some(port) => (port, None),
            None => (0, Some(true)),
        };

        self.put(
            PutRequestSpecific::AnnouncePeer(AnnouncePeerRequestArguments {
                info_hash,
                port,
                implied_port,
            }),
            None,
        )
        .await
        .map_err(|error| match error {
            PutError::Query(error) => error,
            PutError::Concurrency(_) => {
                unreachable!("should not receive a concurrency error from announce peer query")
            }
        })
    }

    // === Immutable data ===

    /// Get an Immutable data by its sha1 hash.
    pub async fn get_immutable(&self, target: Id) -> Option<Box<[u8]>> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.send(ActorMessage::Get(
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: None,
                salt: None,
            }),
            ResponseSender::Immutable(tx),
        ));

        rx.recv().await
    }

    /// Put an immutable data to the DHT.
    pub async fn put_immutable(&self, value: &[u8]) -> Result<Id, PutQueryError> {
        let target: Id = hash_immutable(value).into();

        self.put(
            PutRequestSpecific::PutImmutable(PutImmutableRequestArguments {
                target,
                v: value.into(),
            }),
            None,
        )
        .await
        .map_err(|error| match error {
            PutError::Query(error) => error,
            PutError::Concurrency(_) => {
                unreachable!("should not receive a concurrency error from put immutable query")
            }
        })
    }

    // === Mutable data ===

    /// Get a mutable data by its `public_key` and optional `salt`.
    ///
    /// You can ask for items `more_recent_than` than a certain `seq`,
    /// usually one that you already have seen before, similar to `If-Modified-Since` header in HTTP.
    ///
    /// # Order
    ///
    /// The order of [MutableItem]s returned by this stream is not guaranteed to
    /// reflect their `seq` value. You should not assume that the later items are
    /// more recent than earlier ones.
    ///
    /// Consider using [Self::get_mutable_most_recent] if that is what you need.
    pub fn get_mutable(
        &self,
        public_key: &[u8; 32],
        salt: Option<&[u8]>,
        more_recent_than: Option<i64>,
    ) -> GetStream<MutableItem> {
        let salt = salt.map(|s| s.into());
        let target = MutableItem::target_from_key(public_key, salt.as_deref());
        let (tx, rx) = mpsc::unbounded_channel();
        self.send(ActorMessage::Get(
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: more_recent_than,
                salt,
            }),
            ResponseSender::Mutable(tx),
        ));

        GetStream(rx)
    }

    /// Get the most recent [MutableItem] from the network.
    pub async fn get_mutable_most_recent(
        &self,
        public_key: &[u8; 32],
        salt: Option<&[u8]>,
    ) -> Option<MutableItem> {
        let mut most_recent: Option<MutableItem> = None;
        let mut stream = self.get_mutable(public_key, salt, None);

        while let Some(item) = stream.next().await {
            if let Some(mr) = &most_recent {
                if item.seq() == mr.seq && item.value() > &mr.value {
                    most_recent = Some(item)
                }
            } else {
                most_recent = Some(item);
            }
        }

        most_recent
    }

    /// Put a mutable data to the DHT.
    ///
    /// # Lost Update Problem
    ///
    /// As mainline DHT is a distributed system, it is vulnerable to [Write–write conflict](https://en.wikipedia.org/wiki/Write-write_conflict).
    ///
    /// ## Read first
    ///
    /// To mitigate the risk of lost updates, you should call the [Self::get_mutable_most_recent] method
    /// then start authoring the new [MutableItem] based on the most recent as in the following example:
    ///
    ///```ignore
    /// use mainline::{Dht, MutableItem, SigningKey, Testnet};
    /// use std::net::Ipv4Addr;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let testnet = Testnet::builder(3).build().await.unwrap();
    ///     let dht = Dht::builder()
    ///         .bootstrap(&testnet.bootstrap)
    ///         .bind_address(Ipv4Addr::LOCALHOST)
    ///         .build()
    ///         .unwrap();
    ///
    ///     let signing_key = SigningKey::from_bytes(&[0; 32]);
    ///     let key = signing_key.verifying_key().to_bytes();
    ///     let salt = Some(b"salt".as_ref());
    ///
    ///     let (item, cas) = if let Some(most_recent) = dht.get_mutable_most_recent(&key, salt).await {
    ///         // 1. Optionally Create a new value to take the most recent's value in consideration.
    ///         let mut new_value = most_recent.value().to_vec();
    ///         new_value.extend_from_slice(b" more data");
    ///
    ///         // 2. Increment the sequence number to be higher than the most recent's.
    ///         let most_recent_seq = most_recent.seq();
    ///         let new_seq = most_recent_seq + 1;
    ///
    ///         (
    ///             MutableItem::new(signing_key, &new_value, new_seq, salt),
    ///             // 3. Use the most recent [MutableItem::seq] as a `CAS`.
    ///             Some(most_recent_seq)
    ///         )
    ///     } else {
    ///         (MutableItem::new(signing_key, b"first value", 1, salt), None)
    ///     };
    ///
    ///     dht.put_mutable(item, cas).await.unwrap();
    /// }
    /// ```
    ///
    /// ## Errors
    ///
    /// In addition to the [PutQueryError] common with all PUT queries, PUT mutable item
    /// query has other [Concurrency errors][crate::rpc::ConcurrencyError], that try to detect write conflict
    /// risks or obvious conflicts.
    ///
    /// If you are lucky to get one of these errors (which is not guaranteed), then you should
    /// read the most recent item again, and repeat the steps in the previous example.
    pub async fn put_mutable(
        &self,
        item: MutableItem,
        cas: Option<i64>,
    ) -> Result<Id, PutMutableError> {
        let request = PutRequestSpecific::PutMutable(PutMutableRequestArguments::from(item, cas));

        self.put(request, None).await.map_err(|error| match error {
            PutError::Query(err) => PutMutableError::Query(err),
            PutError::Concurrency(err) => PutMutableError::Concurrency(err),
        })
    }

    // === Raw ===

    /// Get closet nodes to a specific target, that support [BEP_0044](https://www.bittorrent.org/beps/bep_0044.html).
    ///
    /// Useful to [Self::put] a request to nodes further from the 20 closest nodes to the
    /// [PutRequestSpecific::target]. Which itself is useful to circumvent [extreme vertical sybil attacks](https://github.com/pubky/mainline/blob/main/docs/censorship-resistance.md#extreme-vertical-sybil-attacks).
    pub async fn get_closest_nodes(&self, target: Id) -> Box<[Node]> {
        let (tx, rx) = oneshot::channel();
        self.send(ActorMessage::Get(
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                salt: None,
                seq: None,
            }),
            ResponseSender::ClosestNodes(tx),
        ));

        rx.await
            .expect("Query was dropped before sending a response, please open an issue.")
    }

    /// Send a PUT request to the closest nodes, and optionally some extra nodes.
    ///
    /// This is useful to put data to regions of the DHT other than the closest nodes
    /// to this request's [target][PutRequestSpecific::target].
    ///
    /// You can find nodes close to other regions of the network by calling
    /// [Self::get_closest_nodes] with the target that you want to find the closest nodes to.
    ///
    /// Note: extra nodes need to have [Node::valid_token].
    pub async fn put(
        &self,
        request: PutRequestSpecific,
        extra_nodes: Option<Box<[Node]>>,
    ) -> Result<Id, PutError> {
        let (tx, rx) = oneshot::channel();
        self.send(ActorMessage::Put(request, tx, extra_nodes));

        rx.await
            .expect("Query was dropped before sending a response, please open an issue.")
    }

    // === Private Methods ===

    pub(crate) fn send(&self, message: ActorMessage) {
        self.0
            .send(message)
            .expect("actor task unexpectedly shutdown");
    }
}

/// A [futures_core::Stream] of incoming peers, immutable or mutable values.
pub struct GetStream<T>(mpsc::UnboundedReceiver<T>);

impl<T> GetStream<T> {
    /// Get the next item from the stream.
    pub async fn next(&mut self) -> Option<T> {
        self.0.recv().await
    }
}

impl<T> futures_core::Stream for GetStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

// === Actor ===

/// Tick interval for the actor loop when no messages or socket data arrive.
const TICK_INTERVAL: Duration = Duration::from_millis(1);

async fn run(mut rpc: Rpc, mut receiver: mpsc::UnboundedReceiver<ActorMessage>) {
    let mut put_senders: HashMap<Id, Vec<oneshot::Sender<Result<Id, PutError>>>> = HashMap::new();
    let mut get_senders: HashMap<Id, Vec<ResponseSender>> = HashMap::new();
    let mut tick_interval = tokio::time::interval(TICK_INTERVAL);

    loop {
        // Wait for either a message or a tick interval
        tokio::select! {
            biased;

            msg = receiver.recv() => {
                match msg {
                    Some(msg) => handle_actor_message(&mut rpc, msg, &mut put_senders, &mut get_senders),
                    None => {
                        tracing::debug!("mainline::Dht actor task shutdown after Drop.");
                        break;
                    }
                }
            }

            _ = tick_interval.tick() => {
                // Periodic tick for socket I/O and maintenance
            }
        }

        // Drain any additional pending messages
        loop {
            match receiver.try_recv() {
                Ok(msg) => {
                    handle_actor_message(&mut rpc, msg, &mut put_senders, &mut get_senders)
                }
                Err(_) => break,
            }
        }

        // Tick the RPC engine
        let report = rpc.tick();

        // Response for an ongoing GET query
        if let Some((target, response)) = report.new_query_response {
            if let Some(senders) = get_senders.get(&target) {
                for sender in senders {
                    send_streaming(sender, response.clone());
                }
            }
        }

        // Cleanup done GET queries
        for (id, closest_nodes) in report.done_get_queries {
            if let Some(senders) = get_senders.remove(&id) {
                for sender in senders {
                    if let ResponseSender::ClosestNodes(sender) = sender {
                        let _ = sender.send(closest_nodes.clone());
                    }
                }
            }
        }

        // Cleanup done PUT queries and send results
        for (id, error) in report.done_put_queries {
            if let Some(senders) = put_senders.remove(&id) {
                for sender in senders {
                    let result = match &error {
                        Some(err) => Err(err.clone()),
                        None => Ok(id),
                    };
                    let _ = sender.send(result);
                }
            }
        }
    }
}

fn handle_actor_message(
    rpc: &mut Rpc,
    msg: ActorMessage,
    put_senders: &mut HashMap<Id, Vec<oneshot::Sender<Result<Id, PutError>>>>,
    get_senders: &mut HashMap<Id, Vec<ResponseSender>>,
) {
    match msg {
        ActorMessage::Info(sender) => {
            let _ = sender.send(rpc.info());
        }
        ActorMessage::Put(request, sender, extra_nodes) => {
            let target = *request.target();

            match rpc.put(request, extra_nodes) {
                Ok(()) => {
                    put_senders.entry(target).or_default().push(sender);
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                }
            };
        }
        ActorMessage::Get(request, sender) => {
            let target = *request.target();

            if let Some(responses) = rpc.get(request, None) {
                for response in responses {
                    send_streaming(&sender, response);
                }
            };

            get_senders.entry(target).or_default().push(sender);
        }
        ActorMessage::ToBootstrap(sender) => {
            let _ = sender.send(rpc.routing_table().to_bootstrap());
        }
        ActorMessage::SeedRouting(nodes, sender) => {
            for node in nodes {
                rpc.routing_table_mut().add(node);
            }
            let _ = sender.send(());
        }
    }
}

fn send_streaming(sender: &ResponseSender, response: Response) {
    match (sender, response) {
        (ResponseSender::Peers(s), Response::Peers(r)) => {
            let _ = s.send(r);
        }
        (ResponseSender::Mutable(s), Response::Mutable(r)) => {
            let _ = s.send(r);
        }
        (ResponseSender::Immutable(s), Response::Immutable(r)) => {
            let _ = s.send(r);
        }
        _ => {}
    }
}

#[derive(Debug)]
pub(crate) enum ActorMessage {
    Info(oneshot::Sender<Info>),
    Put(
        PutRequestSpecific,
        oneshot::Sender<Result<Id, PutError>>,
        Option<Box<[Node]>>,
    ),
    Get(GetRequestSpecific, ResponseSender),
    ToBootstrap(oneshot::Sender<Vec<String>>),
    SeedRouting(Vec<Node>, oneshot::Sender<()>),
}

#[derive(Debug)]
pub(crate) enum ResponseSender {
    ClosestNodes(oneshot::Sender<Box<[Node]>>),
    Peers(mpsc::UnboundedSender<Vec<SocketAddrV4>>),
    Mutable(mpsc::UnboundedSender<MutableItem>),
    Immutable(mpsc::UnboundedSender<Box<[u8]>>),
}

/// Builder for creating a [Testnet] with custom configuration.
///
/// # Defaults
///
/// - `bind_address`: `127.0.0.1` (localhost)
/// - `seeded`: `true` - nodes start with fully populated routing tables
///
/// # Example
///
/// ```ignore
/// use std::net::Ipv4Addr;
/// use mainline::Testnet;
///
/// // Use localhost (default)
/// let testnet = Testnet::builder(3).build().await.unwrap();
///
/// // Use all interfaces (0.0.0.0)
/// let testnet = Testnet::builder(3)
///     .bind_address(Ipv4Addr::UNSPECIFIED)
///     .build()
///     .await
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct TestnetBuilder {
    count: usize,
    bind_address: Ipv4Addr,
    seeded: bool,
}

impl TestnetBuilder {
    /// Create a new builder with the specified number of nodes.
    ///
    /// # Defaults
    ///
    /// - `bind_address`: `127.0.0.1` (localhost)
    /// - `seeded`: `true`
    pub fn new(count: usize) -> Self {
        Self {
            count,
            bind_address: Ipv4Addr::LOCALHOST,
            seeded: true,
        }
    }

    /// Set the address to bind all nodes to.
    ///
    /// Defaults to `127.0.0.1` (localhost).
    /// Use `Ipv4Addr::UNSPECIFIED` (`0.0.0.0`) to bind to all interfaces.
    pub fn bind_address(&mut self, bind_address: Ipv4Addr) -> &mut Self {
        self.bind_address = bind_address;
        self
    }

    /// Whether to pre-seed routing tables with all nodes.
    ///
    /// Defaults to `true`.
    ///
    /// When `true`, all nodes start with fully populated routing tables.
    /// When `false`, nodes bootstrap from each other which is faster at startup
    /// but may not have immediate full connectivity.
    pub fn seeded(&mut self, seeded: bool) -> &mut Self {
        self.seeded = seeded;
        self
    }

    /// Build the testnet.
    ///
    /// Nodes will be bound to the configured `bind_address` (default: `127.0.0.1`).
    ///
    /// This will block until all nodes are created (and seeded if `seeded` is true).
    pub async fn build(&self) -> Result<Testnet, std::io::Error> {
        if self.seeded {
            Testnet::build_seeded(self.count, self.bind_address).await
        } else {
            Testnet::build_unseeded(self.count, self.bind_address).await
        }
    }
}

/// Create a testnet of Dht nodes to run tests against instead of the real mainline network.
#[derive(Debug)]
pub struct Testnet {
    /// bootstrapping nodes for this testnet.
    pub bootstrap: Vec<String>,
    /// all nodes in this testnet
    pub nodes: Vec<Dht>,
}

impl Testnet {
    /// Returns a builder to configure and create a [Testnet].
    ///
    /// The builder defaults to binding to `127.0.0.1` (localhost).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let testnet = Testnet::builder(3).build().await.unwrap();
    /// ```
    pub fn builder(count: usize) -> TestnetBuilder {
        TestnetBuilder::new(count)
    }

    /// Create a new testnet with a certain size.
    ///
    /// This will block until all nodes are seeded with local peers.
    pub async fn new(count: usize) -> Result<Testnet, std::io::Error> {
        Testnet::build_seeded(count, Ipv4Addr::UNSPECIFIED).await
    }

    /// Create a new testnet without pre-seeding routing tables.
    ///
    /// This is faster at startup, but nodes will not start with fully populated routing tables.
    pub async fn new_unseeded(count: usize) -> Result<Testnet, std::io::Error> {
        Testnet::build_unseeded(count, Ipv4Addr::UNSPECIFIED).await
    }

    async fn build_seeded(
        count: usize,
        bind_address: Ipv4Addr,
    ) -> Result<Testnet, std::io::Error> {
        let mut nodes = Vec::with_capacity(count);

        for _ in 0..count {
            let node = Dht::builder()
                .server_mode()
                .no_bootstrap()
                .bind_address(bind_address)
                .build()?;
            nodes.push(node);
        }

        let mut infos = Vec::with_capacity(count);
        for node in &nodes {
            infos.push(node.info().await);
        }

        let bootstrap = infos
            .iter()
            .map(|info| info.local_addr().to_string())
            .collect::<Vec<_>>();
        let seeded_nodes: Vec<_> = infos
            .iter()
            .map(|info| Node::new(*info.id(), info.local_addr()))
            .collect();

        for (node, info) in nodes.iter().zip(infos.iter()) {
            let peers = seeded_nodes
                .iter()
                .filter(|peer| peer.id() != info.id())
                .cloned()
                .collect::<Vec<_>>();
            let (tx, rx) = oneshot::channel();
            node.send(ActorMessage::SeedRouting(peers, tx));
            let _ = rx.await;
        }

        Ok(Self { bootstrap, nodes })
    }

    async fn build_unseeded(
        count: usize,
        bind_address: Ipv4Addr,
    ) -> Result<Testnet, std::io::Error> {
        let mut nodes = Vec::with_capacity(count);
        let mut bootstrap = Vec::new();

        for i in 0..count {
            if i == 0 {
                let node = Dht::builder()
                    .server_mode()
                    .no_bootstrap()
                    .bind_address(bind_address)
                    .build()?;

                let info = node.info().await;

                bootstrap.push(info.local_addr().to_string());

                nodes.push(node);
            } else {
                let node = Dht::builder()
                    .server_mode()
                    .bootstrap(&bootstrap)
                    .bind_address(bind_address)
                    .build()?;
                nodes.push(node);
            }
        }

        Ok(Self { bootstrap, nodes })
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

#[derive(Debug, Clone)]
/// Put MutableItem errors.
pub enum PutMutableError {
    /// Common PutQuery errors
    Query(PutQueryError),

    /// PutQuery for [crate::MutableItem] errors
    Concurrency(ConcurrencyError),
}

impl std::fmt::Display for PutMutableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PutMutableError::Query(e) => e.fmt(f),
            PutMutableError::Concurrency(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for PutMutableError {}

impl From<PutQueryError> for PutMutableError {
    fn from(e: PutQueryError) -> Self {
        PutMutableError::Query(e)
    }
}

impl From<ConcurrencyError> for PutMutableError {
    fn from(e: ConcurrencyError) -> Self {
        PutMutableError::Concurrency(e)
    }
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;
    use std::str::FromStr;

    use ed25519_dalek::SigningKey;
    use crate::rpc::ConcurrencyError;

    use super::*;

    #[tokio::test]
    async fn bind_twice() {
        let a = Dht::client().unwrap();
        let result = Dht::builder()
            .port(a.info().await.local_addr().port())
            .server_mode()
            .build();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn announce_get_peer() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let a = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();
        let b = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let info_hash = Id::random();

        a.announce_peer(info_hash, Some(45555))
            .await
            .expect("failed to announce");

        let peers = b.get_peers(info_hash).next().await.expect("No peers");

        assert_eq!(peers.first().unwrap().port(), 45555);
    }

    #[tokio::test]
    async fn put_get_immutable() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let a = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();
        let b = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let value = b"Hello World!";
        let expected_target = Id::from_str("e5f96f6f38320f0f33959cb4d3d656452117aadb").unwrap();

        let target = a.put_immutable(value).await.unwrap();
        assert_eq!(target, expected_target);

        let response = b.get_immutable(target).await;
        assert_eq!(response, Some(value.to_vec().into_boxed_slice()));
    }

    #[tokio::test]
    async fn find_node_no_values() {
        let client = Dht::builder().no_bootstrap().build().unwrap();

        client.find_node(Id::random()).await;
    }

    #[tokio::test]
    async fn put_get_immutable_no_values() {
        let client = Dht::builder().no_bootstrap().build().unwrap();

        assert_eq!(client.get_immutable(Id::random()).await, None);
    }

    #[tokio::test]
    async fn put_get_mutable() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let a = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();
        let b = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let signer = SigningKey::from_bytes(&[
            56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115, 7,
            228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
        ]);

        let seq = 1000;
        let value = b"Hello World!";

        let item = MutableItem::new(signer.clone(), value, seq, None);

        a.put_mutable(item.clone(), None).await.unwrap();

        let response = b
            .get_mutable(signer.verifying_key().as_bytes(), None, None)
            .next()
            .await
            .expect("No mutable values");

        assert_eq!(&response, &item);
    }

    #[tokio::test]
    async fn put_get_mutable_no_more_recent_value() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let a = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();
        let b = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let signer = SigningKey::from_bytes(&[
            56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115, 7,
            228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
        ]);

        let seq = 1000;
        let value = b"Hello World!";

        let item = MutableItem::new(signer.clone(), value, seq, None);

        a.put_mutable(item.clone(), None).await.unwrap();

        let response = b
            .get_mutable(signer.verifying_key().as_bytes(), None, Some(seq))
            .next()
            .await;

        assert!(&response.is_none());
    }

    #[tokio::test]
    async fn repeated_put_query() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let a = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let first = a.put_immutable(&[1, 2, 3]).await;
        let second = a.put_immutable(&[1, 2, 3]).await;

        assert_eq!(first.unwrap(), second.unwrap());
    }

    #[tokio::test]
    async fn concurrent_get_mutable() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let a = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();
        let b = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let signer = SigningKey::from_bytes(&[
            56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115, 7,
            228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
        ]);

        let seq = 1000;
        let value = b"Hello World!";

        let item = MutableItem::new(signer.clone(), value, seq, None);

        a.put_mutable(item.clone(), None).await.unwrap();

        let _response_first = b
            .get_mutable(signer.verifying_key().as_bytes(), None, None)
            .next()
            .await
            .expect("No mutable values");

        let response_second = b
            .get_mutable(signer.verifying_key().as_bytes(), None, None)
            .next()
            .await
            .expect("No mutable values");

        assert_eq!(&response_second, &item);
    }

    #[tokio::test]
    async fn concurrent_put_mutable_same() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let dht = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let signer = SigningKey::from_bytes(&[
            56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115, 7,
            228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
        ]);

        let seq = 1000;
        let value = b"Hello World!";

        let item = MutableItem::new(signer.clone(), value, seq, None);

        let mut handles = vec![];

        for _ in 0..2 {
            let dht = dht.clone();
            let item = item.clone();

            let handle = tokio::spawn(async move {
                dht.put_mutable(item, None).await.unwrap();
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn concurrent_put_mutable_different() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let dht = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let mut handles = vec![];

        for i in 0..2u8 {
            let dht = dht.clone();

            let signer = SigningKey::from_bytes(&[
                56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115,
                7, 228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
            ]);

            let seq = 1000;

            let mut value = b"Hello World!".to_vec();
            value.push(i);

            let item = MutableItem::new(signer.clone(), &value, seq, None);

            let handle = tokio::spawn(async move {
                let result = dht.put_mutable(item, None).await;
                if i == 0 {
                    assert!(result.is_ok())
                } else {
                    assert!(matches!(
                        result,
                        Err(PutMutableError::Concurrency(ConcurrencyError::ConflictRisk))
                    ))
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn concurrent_put_mutable_different_with_cas() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let dht = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let signer = SigningKey::from_bytes(&[
            56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115, 7,
            228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
        ]);
        let value = b"Hello World!".to_vec();

        // First - fire and forget a put
        {
            let item = MutableItem::new(signer.clone(), &value, 1000, None);
            let request =
                PutRequestSpecific::PutMutable(PutMutableRequestArguments::from(item, None));
            let (tx, _rx) = oneshot::channel();
            dht.send(ActorMessage::Put(request, tx, None));
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second - read most recent then put
        {
            let item = MutableItem::new(signer, &value, 1001, None);

            let most_recent = dht.get_mutable_most_recent(item.key(), None).await;

            if let Some(cas) = most_recent.map(|item| item.seq()) {
                dht.put_mutable(item, Some(cas)).await.unwrap();
            } else {
                dht.put_mutable(item, None).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn conflict_302_seq_less_than_current() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let dht = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let signer = SigningKey::from_bytes(&[
            56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115, 7,
            228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
        ]);

        dht.put_mutable(MutableItem::new(signer.clone(), &[], 1001, None), None)
            .await
            .unwrap();

        assert!(matches!(
            dht.put_mutable(MutableItem::new(signer, &[], 1000, None), None)
                .await,
            Err(PutMutableError::Concurrency(
                ConcurrencyError::NotMostRecent
            ))
        ));
    }

    #[tokio::test]
    async fn conflict_301_cas() {
        let testnet = Testnet::builder(10).build().await.unwrap();

        let dht = Dht::builder()
            .bootstrap(&testnet.bootstrap)
            .bind_address(Ipv4Addr::LOCALHOST)
            .build()
            .unwrap();

        let signer = SigningKey::from_bytes(&[
            56, 171, 62, 85, 105, 58, 155, 209, 189, 8, 59, 109, 137, 84, 84, 201, 221, 115, 7,
            228, 127, 70, 4, 204, 182, 64, 77, 98, 92, 215, 27, 103,
        ]);

        dht.put_mutable(MutableItem::new(signer.clone(), &[], 1001, None), None)
            .await
            .unwrap();

        assert!(matches!(
            dht.put_mutable(MutableItem::new(signer, &[], 1002, None), Some(1000))
                .await,
            Err(PutMutableError::Concurrency(ConcurrencyError::CasFailed))
        ));
    }

    #[tokio::test]
    async fn populate_bootstrapping_node_routing_table() {
        let size = 3;

        let testnet = Testnet::builder(size).build().await.unwrap();

        for node in &testnet.nodes {
            assert_eq!(node.to_bootstrap().await.len(), size - 1);
        }
    }
}
