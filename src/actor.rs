pub(crate) mod config;
mod info;
pub(crate) mod socket;

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::MutableItem;
use crate::common::{
    FindNodeRequestArguments, Id, Message, MessageType, Node, PutRequestSpecific, RequestSpecific,
    RequestTypeSpecific,
};
use crate::core::{
    Core, PutError, QueryId, Response, iterative_query::GetRequestSpecific, put_query::PutQuery,
};

use socket::{DatagramHook, KrpcSocket};

pub use info::Info;

#[derive(Debug)]
/// Internal Rpc called in the Dht thread loop, useful to create your own actor setup.
pub(crate) struct Actor {
    pub(crate) socket: KrpcSocket,
    core: Core,

    put_senders: HashMap<QueryId, oneshot::Sender<Result<Id, PutError>>>,
    get_senders: HashMap<QueryId, Vec<ResponseSender>>,

    /// Configured bootstrap hosts, kept so they can be resolved again.
    bootstrap_hosts: Box<[String]>,
    /// When the bootstrap hosts were last resolved.
    last_bootstrap_resolve: Instant,
    /// Resolution of the bootstrap hosts running in the background.
    bootstrap_resolve: Option<JoinHandle<Vec<SocketAddrV4>>>,
}

/// Minimum time between resolving the bootstrap hosts again while the routing table is empty.
const BOOTSTRAP_RESOLVE_INTERVAL: Duration = Duration::from_secs(30);

impl Actor {
    /// Create a new actor
    pub(crate) fn new(config: config::Config) -> io::Result<Self> {
        let id = if let Some(ip) = config.public_ip {
            Id::from_ip(ip.into())
        } else {
            Id::random()
        };

        let socket = KrpcSocket::new(&config)?;
        let bootstrap = resolve_bootstrap_hosts(&config.bootstrap);

        let address = socket.local_addr();
        info!(?address, "Mainline DHT started");

        Ok(Actor {
            socket,
            core: Core::new(id, bootstrap, config.mode, config.server_settings),

            put_senders: HashMap::new(),
            get_senders: HashMap::new(),

            bootstrap_hosts: config.bootstrap.into(),
            last_bootstrap_resolve: Instant::now(),
            bootstrap_resolve: None,
        })
    }

    // === Getters ===

    /// Returns the node's Id
    fn id(&self) -> &Id {
        self.core.routing_table.id()
    }

    /// Create a list of unique bootstrapping nodes from all our
    /// routing table to use as `extra_bootsrtap` in next sessions.
    fn to_bootstrap(&self) -> Vec<String> {
        self.core.routing_table.to_bootstrap()
    }

    /// Returns a thread safe and lightweight summary of this node's
    /// information and statistics.
    fn info(&self) -> Info {
        Info::from(self)
    }

    // === Public Methods ===

    /// Advance the inflight queries, receive incoming requests,
    /// maintain the routing table, and everything else that needs
    /// to happen at every tick.
    fn tick(&mut self) {
        self.periodic_node_maintaenance();

        let mut done_put_queries = self.check_done_put_queries();

        for query in self.core.iterative_queries.values_mut() {
            query.visit_closest(&mut self.socket);
        }

        let done_iterative_queries = self.check_done_iterative_queries();

        self.start_put_queries(&done_iterative_queries, &mut done_put_queries);

        self.core
            .cleanup_done_queries(&done_iterative_queries, &done_put_queries);

        // Cleanup done iterative queries
        for (id, closest_nodes) in done_iterative_queries {
            if let Some(senders) = self.get_senders.remove(&id) {
                for sender in senders {
                    // return closest_nodes to whoever was asking
                    if let ResponseSender::ClosestNodes(sender) = sender {
                        let _ = sender.send(closest_nodes.clone());
                    }
                }
            }
        }

        // Cleanup done PUT query and send a resulting error if any.
        for (id, result) in done_put_queries {
            if let Some(sender) = self.put_senders.remove(&id) {
                let _ = sender.send(result);
            }
        }
    }

    /// Process an incoming message from the network.
    fn process_message(&mut self, message: Message, from: SocketAddrV4) {
        let new_query_response = self.handle_incoming_message(message, from);

        // Response for an ongoing GET query
        if let Some((id, response)) = new_query_response
            && let Some(senders) = self.get_senders.get_mut(&id)
        {
            for sender in senders.iter_mut() {
                send(sender, response.clone());
            }
        }
    }

    /// Store a value in the closest nodes, optionally trigger a lookup query if
    /// the cached closest_nodes aren't fresh enough.
    fn put(
        &mut self,
        request: PutRequestSpecific,
        extra_nodes: Option<Box<[Node]>>,
    ) -> Result<QueryId, PutError> {
        let mut query = PutQuery::new(request.clone(), extra_nodes);

        let target = request.target();
        if let Some(closest_nodes) = self.core.get_cached_closest_nodes(target) {
            query.start(&mut self.socket, &closest_nodes)?
        } else {
            query.lookup_id = Some(self.get(GetRequestSpecific::from(&request), None).0);
        };

        let id = self.core.allocate_query_id();
        self.core.put_queries.insert(id, query);

        Ok(id)
    }

    /// Send a message to closer and closer nodes until we can't find any more nodes.
    ///
    /// Queries take few seconds to fully traverse the network, once it is done, it will be removed from
    /// self.iterative_queries. Until then, identical requests share the lookup and receive
    /// the responses seen so far. Different requests use separate internal lookup IDs.
    ///
    /// Effectively, we are caching responses and backing off the network for the duration it takes
    /// to traverse it.
    ///
    /// - `request` [RequestTypeSpecific], except [RequestTypeSpecific::Ping] and
    ///   [RequestTypeSpecific::Put] which will be ignored.
    /// - `extra_nodes` option allows the query to visit specific nodes, that won't necessesarily be visited
    ///   through the query otherwise.
    fn get(
        &mut self,
        request: GetRequestSpecific,
        extra_nodes: Option<&[SocketAddrV4]>,
    ) -> (QueryId, Vec<Response>) {
        let target = request.target();

        let mut responses = vec![];

        if let Some(response_from_outgoing_request) = self.core.check_outgoing_put_request(&request)
        {
            responses.push(response_from_outgoing_request);
        }

        if let Some((id, responses_from_active_query)) = self.core.find_active_query(&request) {
            responses.extend_from_slice(responses_from_active_query);
            return (id, responses);
        }

        let (id, mut query, to_visit) = self.core.create_iterative_query(request, extra_nodes);
        let node_id = self.id();
        if target == *node_id {
            debug!(?node_id, "Bootstrapping the routing table");
        }

        for address in to_visit {
            query.visit(&mut self.socket, address);
        }
        self.core.iterative_queries.insert(id, query);

        (id, responses)
    }

    // === Private Methods ===

    fn periodic_node_maintaenance(&mut self) {
        // Bootstrap if necessary
        if self.core.routing_table.is_empty() {
            self.maybe_resolve_bootstrap();
            self.populate();
        }

        // Every 15 minutes refresh the routing table.
        if self.core.should_refresh_table() {
            self.core.update_last_table_refresh();
            self.populate();
        }

        if self.core.should_ping_table() {
            self.core.update_last_table_ping();
            let to_ping = self.core.check_nodes_to_ping_and_remove_stale_nodes();
            for address in to_ping {
                self.ping(address);
            }
        }
    }

    /// Resolves the bootstrap hosts again in the background.
    ///
    /// Runs at most every [`BOOTSTRAP_RESOLVE_INTERVAL`]. The hosts are first
    /// resolved when the actor is created. If DNS was not available then, or
    /// the addresses have changed since, the node could otherwise never
    /// bootstrap.
    fn maybe_resolve_bootstrap(&mut self) {
        if self.bootstrap_hosts.is_empty()
            || self.bootstrap_resolve.is_some()
            || self.last_bootstrap_resolve.elapsed() < BOOTSTRAP_RESOLVE_INTERVAL
        {
            return;
        }
        self.last_bootstrap_resolve = Instant::now();
        let hosts = self.bootstrap_hosts.clone();
        debug!("Resolving bootstrap hosts again");
        self.bootstrap_resolve = Some(tokio::task::spawn_blocking(move || {
            resolve_bootstrap_hosts(&hosts)
        }));
    }

    /// Replaces the bootstrap addresses with a finished resolution.
    fn set_bootstrap(&mut self, bootstrap: Vec<SocketAddrV4>) {
        self.bootstrap_resolve = None;
        if bootstrap.is_empty() {
            // Keep the previous addresses, they are better than none.
            return;
        }
        debug!(?bootstrap, "Resolved bootstrap hosts");
        self.core.bootstrap = bootstrap.into();
    }

    fn handle_incoming_message(
        &mut self,
        message: Message,
        from: SocketAddrV4,
    ) -> Option<(QueryId, Response)> {
        match message.message_type {
            MessageType::Request(request_specific) => {
                let response = self
                    .core
                    .handle_request(from, message.read_only, request_specific);

                match response {
                    Some(MessageType::Error(error)) => {
                        self.socket.error(from, message.transaction_id, error)
                    }
                    Some(MessageType::Response(response)) => {
                        self.socket.response(from, message.transaction_id, response)
                    }
                    _ => {}
                }

                for address in self.core.take_pending_pings() {
                    self.ping(address);
                }

                None
            }
            _ => self.core.handle_response(from, message),
        }
    }

    fn check_done_put_queries(&self) -> Vec<(QueryId, Result<Id, PutError>)> {
        self.core
            .put_queries
            .iter()
            .filter_map(|(id, query)| match query.check(&self.socket) {
                Ok(done) => {
                    if done {
                        Some((*id, Ok(query.target)))
                    } else {
                        None
                    }
                }
                Err(error) => Some((*id, Err(error))),
            })
            .collect()
    }

    fn check_done_iterative_queries(&self) -> Vec<(QueryId, Box<[Node]>)> {
        self.core
            .iterative_queries
            .iter()
            .filter_map(|(id, query)| {
                let is_done = query.is_done(&self.socket);
                if is_done {
                    Some((
                        *id,
                        self.core.closest_nodes_from_done_iterative_query(query),
                    ))
                } else {
                    None
                }
            })
            .collect()
    }

    fn start_put_queries(
        &mut self,
        done_iterative_queries: &[(QueryId, Box<[Node]>)],
        done_put_queries: &mut Vec<(QueryId, Result<Id, PutError>)>,
    ) {
        for (lookup_id, closest_nodes) in done_iterative_queries {
            for (id, put_query) in &mut self.core.put_queries {
                if put_query.lookup_id == Some(*lookup_id)
                    && let Err(error) = put_query.start(&mut self.socket, closest_nodes)
                {
                    done_put_queries.push((*id, Err(error)));
                }
            }
        }
    }

    /// Ping bootstrap nodes, add them to the routing table with closest query.
    fn populate(&mut self) {
        if self.core.bootstrap.is_empty() {
            return;
        }

        self.get(
            GetRequestSpecific::FindNode(FindNodeRequestArguments { target: *self.id() }),
            None,
        );
    }

    fn ping(&mut self, address: SocketAddrV4) {
        self.socket.request(
            address,
            RequestSpecific {
                requester_id: *self.id(),
                request_type: RequestTypeSpecific::Ping,
            },
        );
    }
}

/// Async event loop driving an already-built [`Actor`].
///
/// [`crate::Dht::new`] builds the actor synchronously (so bind errors surface immediately
/// on the caller) and hands it off here to run in a spawned task.
pub(crate) async fn run(mut actor: Actor, mut receiver: mpsc::Receiver<ActorMessage>) {
    // Maintenance interval
    let mut maintenance = tokio::time::interval(Duration::from_secs(1));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        // Flush any queued outgoing packets
        actor.socket.flush().await;

        let bootstrap_resolve = actor.bootstrap_resolve.as_mut();
        tokio::select! {
            msg = receiver.recv() => {
                match msg {
                    Some(actor_message) => match actor_message {
                        ActorMessage::Info(sender) => {
                            let _ = sender.send(actor.info());
                        }
                        ActorMessage::Put(request, sender, extra_nodes) => {
                            match actor.put(request, extra_nodes) {
                                Ok(id) => {
                                    actor.put_senders.insert(id, sender);
                                }
                                Err(error) => {
                                    let _ = sender.send(Err(error));
                                }
                            };
                        }
                        ActorMessage::Get(request, mut sender) => {
                            let (id, responses) = actor.get(request, None);
                            for response in responses {
                                send(&mut sender, response);
                            }

                            let senders = actor.get_senders.entry(id).or_default();
                            senders.push(sender);
                        }
                        ActorMessage::ToBootstrap(sender) => {
                            let _ = sender.send(actor.to_bootstrap());
                        }
                        ActorMessage::SetDatagramHook(hook) => {
                            actor.socket.set_datagram_hook(hook);
                        }
                        ActorMessage::SendDatagram(bytes, to) => {
                            actor.socket.send_datagram(bytes, to);
                        }
                    },
                    None => {
                        // All senders dropped, shutdown.
                        debug!("dht::Dht's actor task was shutdown after Drop.");
                        break;
                    }
                }
            }
            messages = actor.socket.recv_batch() => {
                for (message, from) in messages {
                    actor.process_message(message, from);
                }
            }
            resolved = async {
                match bootstrap_resolve {
                    Some(task) => task.await,
                    None => std::future::pending().await,
                }
            } => {
                actor.set_bootstrap(resolved.unwrap_or_default());
            }
            _ = maintenance.tick() => {
                // Wake the loop so actor.tick() runs even when idle,
                // ensuring routing table refresh and node pings happen on schedule.
            }
        }

        actor.tick();
    }
}

/// Resolves bootstrap hosts to IPv4 addresses, logging resolution errors.
fn resolve_bootstrap_hosts(hosts: &[String]) -> Vec<SocketAddrV4> {
    let mut resolved = Vec::new();
    for host in hosts {
        match host.to_socket_addrs() {
            Ok(addrs) => resolved.extend(addrs.filter_map(|addr| match addr {
                SocketAddr::V4(addr_v4) => Some(addr_v4),
                _ => None,
            })),
            Err(error) => {
                warn!(host, %error, "Failed to resolve bootstrap host");
            }
        }
    }
    resolved
}

fn send(sender: &mut ResponseSender, response: Response) {
    match (sender, response) {
        (ResponseSender::Peers(s), Response::Peers(r)) => {
            let _ = s.send(r);
        }
        (ResponseSender::Mutable(s), Response::Mutable(r)) => {
            let _ = s.send(r);
        }
        (ResponseSender::Immutable(s), Response::Immutable(r)) => {
            if let Some(tx) = s.take() {
                let _ = tx.send(r);
            }
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
    SetDatagramHook(Option<DatagramHook>),
    SendDatagram(Vec<u8>, SocketAddrV4),
}

/// Sender side for streaming GET query results back to the caller.
#[derive(Debug)]
pub(crate) enum ResponseSender {
    ClosestNodes(oneshot::Sender<Box<[Node]>>),
    Peers(mpsc::UnboundedSender<Vec<SocketAddrV4>>),
    Mutable(mpsc::UnboundedSender<MutableItem>),
    Immutable(Option<oneshot::Sender<Box<[u8]>>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Dht, Testnet};

    /// A node whose bootstrap hosts failed to resolve recovers once they resolve.
    #[tokio::test]
    async fn resolves_bootstrap_hosts_again() {
        let testnet = Testnet::new(3).await.unwrap();

        let config = config::Config {
            bootstrap: vec!["bootstrap.invalid:6881".to_string()],
            ..Default::default()
        };
        let mut actor = Actor::new(config).unwrap();
        assert!(actor.core.bootstrap.is_empty());

        // DNS works again: the hosts now resolve, and the interval has passed.
        actor.bootstrap_hosts = testnet.bootstrap.clone().into();
        actor.last_bootstrap_resolve = Instant::now() - BOOTSTRAP_RESOLVE_INTERVAL;

        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(run(actor, receiver));
        let dht = Dht(sender);

        tokio::time::timeout(Duration::from_secs(10), async {
            while !dht.bootstrapped().await.unwrap() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("node did not bootstrap after its hosts resolved");
    }

    #[tokio::test]
    async fn ping_to_reported_address_does_not_change_mode_or_id() {
        let config = config::Config {
            bootstrap: vec![],
            ..Default::default()
        };
        let mut actor = Actor::new(config).unwrap();
        let public_address = SocketAddrV4::new([127, 0, 0, 1].into(), 6881);
        actor.core.public_address = Some(public_address);

        let ping = Message {
            transaction_id: 7_u32.to_be_bytes().into(),
            message_type: MessageType::Request(RequestSpecific {
                requester_id: Id::random(),
                request_type: RequestTypeSpecific::Ping,
            }),
            version: None,
            read_only: false,
            requester_ip: None,
        };
        let original_id = *actor.id();
        actor.handle_incoming_message(ping, public_address);
        actor.core.last_table_refresh =
            std::time::Instant::now() - crate::core::REFRESH_TABLE_INTERVAL;
        actor.periodic_node_maintaenance();

        assert_eq!(actor.info().mode(), crate::Mode::Client);
        assert_eq!(actor.socket.mode, crate::Mode::Client);
        assert_eq!(*actor.id(), original_id);
    }

    #[tokio::test]
    async fn lookups_share_ids_only_for_identical_requests() {
        use crate::common::{GetPeersRequestArguments, GetValueRequestArguments};
        let mut actor = Actor::new(config::Config {
            bootstrap: vec![],
            port: Some(0),
            ..Default::default()
        })
        .unwrap();
        let target = Id::from([9; 20]);
        let requests = [
            GetRequestSpecific::FindNode(FindNodeRequestArguments { target }),
            GetRequestSpecific::GetPeers(GetPeersRequestArguments { info_hash: target }),
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: None,
                salt: None,
            }),
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: Some(1),
                salt: None,
            }),
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: None,
                salt: Some(Box::from(&b"salt"[..])),
            }),
        ];
        let mut ids = std::collections::HashSet::new();
        for request in requests {
            let (id, _) = actor.get(request.clone(), None);
            assert!(
                ids.insert(id),
                "different lookup requests need different IDs"
            );
            assert_eq!(
                actor.get(request, None).0,
                id,
                "identical lookups still share work"
            );
        }
        assert_eq!(actor.core.iterative_queries.len(), ids.len());
    }

    #[tokio::test]
    async fn put_waits_for_its_own_lookup() {
        use crate::common::AnnouncePeerRequestArguments;
        let mut actor = Actor::new(config::Config {
            bootstrap: vec![],
            port: Some(0),
            ..Default::default()
        })
        .unwrap();
        let target = Id::from([9; 20]);
        let (find_id, _) = actor.get(
            GetRequestSpecific::FindNode(FindNodeRequestArguments { target }),
            None,
        );
        let put_id = actor
            .put(
                PutRequestSpecific::AnnouncePeer(AnnouncePeerRequestArguments {
                    info_hash: target,
                    port: 1234,
                    implied_port: None,
                }),
                None,
            )
            .unwrap();
        let lookup_id = actor.core.put_queries[&put_id].lookup_id.unwrap();
        assert_ne!(lookup_id, find_id);
        let node = Node::new(Id::random(), "127.0.0.1:12345".parse().unwrap());
        let done_find = [(find_id, vec![node].into_boxed_slice())];
        let mut done_puts = vec![];
        actor.start_put_queries(&done_find, &mut done_puts);
        actor.core.cleanup_done_queries(&done_find, &done_puts);
        assert!(done_puts.is_empty());
        assert!(!actor.core.put_queries[&put_id].started());
        assert!(actor.core.iterative_queries.contains_key(&lookup_id));
        let node = Node::new_with_token(
            Id::random(),
            "127.0.0.1:12345".parse().unwrap(),
            Box::new([1]),
        );
        actor.start_put_queries(
            &[(lookup_id, vec![node].into_boxed_slice())],
            &mut done_puts,
        );
        assert!(done_puts.is_empty());
        assert!(actor.core.put_queries[&put_id].started());
        assert_eq!(actor.core.put_queries[&put_id].lookup_id, None);
    }

    #[tokio::test]
    async fn read_results_are_routed_to_their_lookup_id() {
        use crate::{
            SigningKey,
            common::{GetMutableResponseArguments, GetValueRequestArguments, ResponseSpecific},
        };
        let mut actor = Actor::new(config::Config {
            bootstrap: vec![],
            port: Some(0),
            ..Default::default()
        })
        .unwrap();
        let item = MutableItem::new(&SigningKey::from_bytes(&[7; 32]), b"value", 1, None);
        let request = |seq| {
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target: *item.target(),
                seq,
                salt: None,
            })
        };
        let (all_id, _) = actor.get(request(None), None);
        let (newer_id, _) = actor.get(request(Some(1)), None);
        let (all_tx, mut all_rx) = mpsc::unbounded_channel();
        let (newer_tx, mut newer_rx) = mpsc::unbounded_channel();
        actor
            .get_senders
            .insert(all_id, vec![ResponseSender::Mutable(all_tx)]);
        actor
            .get_senders
            .insert(newer_id, vec![ResponseSender::Mutable(newer_tx)]);
        let from = "127.0.0.1:12345".parse().unwrap();
        let tid = actor
            .core
            .iterative_queries
            .get_mut(&all_id)
            .unwrap()
            .visit(&mut actor.socket, from);
        actor.process_message(
            Message {
                transaction_id: tid.to_be_bytes().into(),
                version: None,
                requester_ip: None,
                read_only: false,
                message_type: MessageType::Response(ResponseSpecific::GetMutable(
                    GetMutableResponseArguments {
                        responder_id: Id::random(),
                        token: Box::new([1]),
                        nodes: None,
                        v: item.value().into(),
                        k: *item.key(),
                        seq: item.seq(),
                        sig: *item.signature(),
                    },
                )),
            },
            from,
        );
        assert_eq!(all_rx.try_recv().unwrap(), item);
        assert!(matches!(
            newer_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(actor.get(request(None), None).1.len(), 1);
        assert!(actor.get(request(Some(1)), None).1.is_empty());
    }

    #[tokio::test]
    async fn pending_put_respects_read_sequence_filter() {
        use crate::{
            SigningKey,
            common::{GetValueRequestArguments, PutMutableRequestArguments},
        };
        let mut actor = Actor::new(config::Config {
            bootstrap: vec![],
            port: Some(0),
            ..Default::default()
        })
        .unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let newest = MutableItem::new(&key, b"newest", 2, None);
        // Insert in reverse sequence order: the result must not depend on map order.
        for item in [newest.clone(), MutableItem::new(&key, b"older", 1, None)] {
            actor
                .put(
                    PutRequestSpecific::PutMutable(PutMutableRequestArguments::from(item, None)),
                    None,
                )
                .unwrap();
        }
        let target = *newest.target();
        for (seq, expected) in [
            (None, true),
            (Some(1), true),
            (Some(2), false),
            (Some(3), false),
        ] {
            let (_, responses) = actor.get(
                GetRequestSpecific::GetValue(GetValueRequestArguments {
                    target,
                    seq,
                    salt: None,
                }),
                None,
            );
            if expected {
                assert!(
                    matches!(responses.as_slice(), [Response::Mutable(item)] if item == &newest)
                );
            } else {
                assert!(
                    responses.is_empty(),
                    "read filter {seq:?} returned {responses:?}"
                );
            }
        }
        let (_, responses) = actor.get(
            GetRequestSpecific::FindNode(FindNodeRequestArguments { target }),
            None,
        );
        assert!(responses.is_empty());
        let (_, responses) = actor.get(
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: None,
                salt: Some(Box::new([1])),
            }),
            None,
        );
        assert!(responses.is_empty());
    }
}
