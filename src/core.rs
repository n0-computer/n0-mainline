use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use lru::LruCache;
use tracing::debug;
use tracing::error;

use iterative_query::IterativeQuery;
use put_query::PutQuery;

use crate::common::{
    Id, MAX_BUCKET_SIZE_K, Node, PutMutableRequestArguments, RequestTypeSpecific, RoutingTable,
};
use crate::core::iterative_query::GetRequestSpecific;
use crate::{MutableItem, PutRequestSpecific};

pub mod handle_request;
pub mod handle_response;
pub mod iterative_query;
pub mod put_query;
pub mod server;

use server::Server;
use server::ServerSettings;

pub use put_query::{ConcurrencyError, PutError, PutQueryError};

pub const REFRESH_TABLE_INTERVAL: Duration = Duration::from_secs(15 * 60);
pub const PING_TABLE_INTERVAL: Duration = Duration::from_secs(5 * 60);

pub const MAX_CACHED_ITERATIVE_QUERIES: usize = 1000;
/// How many requesters we remember having pinged.
const MAX_RECENTLY_PINGED: usize = 1000;
/// Minimum time before we ping the same requester again.
const REQUESTER_PING_INTERVAL: Duration = Duration::from_secs(10);

/// Client version sent in every message (`v` in BEP 5).
///
/// Not "RS": upstream `mainline` nodes take "RS" 06 or later as support for the
/// signed peers extension, which this crate does not have.
pub const VERSION: [u8; 4] = [b'N', b'0', 0, 7]; // "N0" version 0.7

#[derive(Debug)]
/// Side effect free Core, containing all the state and methods necessary
/// for comprehensive and deterministic testing.
pub struct Core {
    // Options
    pub bootstrap: Box<[SocketAddrV4]>,

    // Routing
    /// Closest nodes to this node
    pub routing_table: RoutingTable,

    /// Closest responding nodes to specific target
    pub cached_iterative_queries: LruCache<Id, CachedIterativeQuery>,
    /// Active IterativeQueries
    pub iterative_queries: HashMap<Id, IterativeQuery>,
    /// Put queries are special, since they have to wait for a corresponding
    /// get query to finish, update the closest_nodes, then `query_all` these.
    pub put_queries: HashMap<Id, PutQuery>,

    // Time
    /// Last time we refreshed the routing table with a find_node query.
    pub last_table_refresh: Instant,
    /// Last time we pinged nodes in the routing table.
    pub last_table_ping: Instant,

    pub server: Server,
    pub public_address: Option<SocketAddrV4>,
    pub mode: crate::Mode,

    /// Requesters to ping, so that their responses can add them to the routing tables.
    pending_pings: Vec<SocketAddrV4>,
    /// When we last pinged a requester, to not ping it for every request.
    recently_pinged: LruCache<SocketAddrV4, Instant>,
}

impl Core {
    pub fn new(
        id: Id,
        bootstrap: Vec<SocketAddrV4>,
        mode: crate::Mode,
        server_settings: ServerSettings,
    ) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            routing_table: RoutingTable::new(id),
            iterative_queries: HashMap::new(),
            put_queries: HashMap::new(),
            cached_iterative_queries: LruCache::new(
                NonZeroUsize::new(MAX_CACHED_ITERATIVE_QUERIES)
                    .expect("MAX_CACHED_BUCKETS is NonZeroUsize"),
            ),
            last_table_refresh: Instant::now(),
            last_table_ping: Instant::now(),
            server: Server::new(server_settings),
            public_address: None,
            mode,
            pending_pings: Vec::new(),
            recently_pinged: LruCache::new(
                NonZeroUsize::new(MAX_RECENTLY_PINGED)
                    .expect("MAX_RECENTLY_PINGED is NonZeroUsize"),
            ),
        }
    }

    fn id(&self) -> &Id {
        self.routing_table.id()
    }

    /// Takes the addresses that should be pinged now.
    pub fn take_pending_pings(&mut self) -> Vec<SocketAddrV4> {
        std::mem::take(&mut self.pending_pings)
    }

    pub fn should_ping_table(&self) -> bool {
        self.last_table_ping.elapsed() > PING_TABLE_INTERVAL
    }

    pub fn update_last_table_ping(&mut self) {
        self.last_table_ping = Instant::now();
    }

    pub fn should_refresh_table(&self) -> bool {
        self.last_table_refresh.elapsed() > REFRESH_TABLE_INTERVAL
    }

    pub fn update_last_table_refresh(&mut self) {
        self.last_table_refresh = Instant::now();
    }

    pub fn get_cached_closest_nodes(&mut self, target: &Id) -> Option<Box<[Node]>> {
        self.cached_iterative_queries
            .get(target)
            .map(|cached| cached.closest_responding_nodes.clone())
            .filter(|closest_nodes| {
                !closest_nodes.is_empty() && closest_nodes.iter().any(|n| n.valid_token())
            })
    }

    /// Check if the query is either successfully done, in which case return the closest nodes.
    pub fn closest_nodes_from_done_iterative_query(&self, query: &IterativeQuery) -> Box<[Node]> {
        let self_id = self.id();
        let table_size = self.routing_table.size();

        if let RequestTypeSpecific::FindNode(_) = query.request.request_type {
            if query.target() == *self_id {
                if !self.bootstrap.is_empty() && table_size == 0 {
                    error!("Could not bootstrap the routing table");
                } else {
                    debug!(?self_id, table_size, "Populated the routing table");
                }
            };

            query
                .closest()
                .nodes()
                .iter()
                .take(MAX_BUCKET_SIZE_K)
                .cloned()
                .collect::<Box<[_]>>()
        } else {
            query
                .responders()
                .take_until_secure(
                    self.routing_table.responders_based_dht_size_estimate(),
                    self.routing_table.average_subnets(),
                )
                .to_vec()
                .into_boxed_slice()
        }
    }

    /// Remove completed queries, caching their results and reported public address.
    pub fn cleanup_done_queries(
        &mut self,
        done_get_queries: &[(Id, Box<[Node]>)],
        done_put_queries: &[(Id, Option<PutError>)],
    ) {
        for (id, closest_nodes) in done_get_queries {
            if let Some(query) = self.iterative_queries.remove(id) {
                self.cache_iterative_query(&query, closest_nodes);

                self.update_address_votes_from_iterative_query(&query);
            };
        }

        for (id, _) in done_put_queries {
            self.put_queries.remove(id);
        }
    }

    pub fn check_nodes_to_ping_and_remove_stale_nodes(&mut self) -> Vec<SocketAddrV4> {
        let mut to_ping = vec![];

        let mut to_remove = Vec::with_capacity(self.routing_table.size());

        for node in self.routing_table.nodes() {
            if node.is_stale() {
                to_remove.push(*node.id())
            } else if node.should_ping() {
                to_ping.push(node.address())
            }
        }

        for id in to_remove {
            self.routing_table.remove(&id);
        }

        to_ping
    }

    pub fn check_concurrency_errors(
        &mut self,
        request: &PutRequestSpecific,
    ) -> Result<(), ConcurrencyError> {
        if let PutRequestSpecific::PutMutable(PutMutableRequestArguments {
            sig,
            cas,
            seq,
            target,
            ..
        }) = request
            && let Some(PutRequestSpecific::PutMutable(inflight_request)) = self
                .put_queries
                .get(target)
                .map(|existing| &existing.request)
        {
            debug!(?inflight_request, ?request, "Possible conflict risk");

            if *sig == inflight_request.sig {
                // Noop, the inflight query is sufficient.
                return Ok(());
            } else if *seq <= inflight_request.seq {
                // this is a bit unclear in the spec. https://www.bittorrent.org/beps/bep_0044.html
                //
                // "a node hosting the list node MUST not downgrade a list head from a higher sequence number to a lower one, only upgrade"
                // Must not downgrade is clear, but it seems to say that equal is also not allowed.
                //
                // I checked in libtorrent. What it does is that if it gets the same seq, it just refreshes the old value. This would lead
                // to the weird situation where you think you have put a new value, but just refreshed the old one. So in this case we don't
                // allow this at all.
                //
                // Note that publishing the same value with the same signature is still allowed.
                return Err(ConcurrencyError::NotMostRecent);
            } else if cas.is_none() {
                // For unconditional puts (`cas: None`), accept the new request as long as
                // it is not older than the in-flight request.
                self.put_queries.remove(target);
            } else if let Some(cas) = cas {
                if *cas == inflight_request.seq {
                    // The user is aware of the inflight query and whiches to overrides it.
                    //
                    // Remove the inflight request, and create a new one.
                    self.put_queries.remove(target);
                } else {
                    return Err(ConcurrencyError::CasFailed);
                }
            };
        };

        Ok(())
    }

    /// If we are already announcing a mutable item, might as well
    /// return it to the request asking for the same target.
    pub fn check_outgoing_put_request(&self, target: &Id) -> Option<Response> {
        self.put_queries
            .get(target)
            .and_then(|existing| match existing.request.clone() {
                PutRequestSpecific::PutMutable(request) => Some(Response::Mutable(request.into())),
                _ => None,
            })
    }

    /// If query is still active, no need to create a new one.
    pub fn check_responses_from_active_query(&self, target: &Id) -> Option<&[Response]> {
        self.iterative_queries
            .get(target)
            .map(|query| query.responses())
    }

    /// Create an [IterativeQuery] and populate it with candidate nodes, but not insert it to
    /// [Core::iterative_queries] yet.
    ///
    /// Return the query, and a list of nodes (from our bootstrapping node and/or routing tables)
    /// to visit to start the query.
    ///
    /// Returns `None` if there is an already active query.
    pub fn create_iterative_query(
        &mut self,
        request: GetRequestSpecific,
        extra_nodes: Option<&[SocketAddrV4]>,
    ) -> Option<(IterativeQuery, Vec<SocketAddrV4>)> {
        let target = request.target();

        if self.iterative_queries.contains_key(&target) {
            return None;
        }

        // We have multiple routing table now, so we should first figure out which one
        // is the appropriate for this query.
        let routing_table_closest = self.get_closest_nodes_from_routing_tables(&request);

        let mut query = IterativeQuery::new(*self.id(), target, request);

        // Seed this query with the closest nodes we know about.
        for node in routing_table_closest {
            query.add_candidate(node)
        }

        // If we have cached iterative query with the same hash, use its nodes as well..
        if let Some(closest_responding_nodes) = self.get_cached_closest_nodes(&target) {
            for node in closest_responding_nodes {
                query.add_candidate(node.clone())
            }
        }

        // Nothing is visited yet, so no request needs to be looked up.
        let mut to_visit = query.closest_candidates(|_| true);

        // Seed the query either with the closest nodes from the routing table, or the
        // bootstrapping nodes if the closest nodes are not enough.
        let candidates = query.closest();
        if candidates.is_empty() || candidates.len() < self.bootstrap.len() {
            for bootstrapping_node in self.bootstrap.clone() {
                to_visit.push(bootstrapping_node)
            }
        }

        if let Some(extra_nodes) = extra_nodes {
            to_visit.extend_from_slice(extra_nodes);
        }

        Some((query, to_visit))
    }

    // === Private Methods ===

    fn get_closest_nodes_from_routing_tables(&self, request: &GetRequestSpecific) -> Vec<Node> {
        let target = request.target();

        self.routing_table.closest_secure(target)
    }

    fn update_address_votes_from_iterative_query(&mut self, query: &IterativeQuery) {
        if let Some(address) = query.best_address() {
            self.public_address = Some(address);
        }
    }

    fn cache_iterative_query(&mut self, query: &IterativeQuery, closest_responding_nodes: &[Node]) {
        if self.cached_iterative_queries.len() >= MAX_CACHED_ITERATIVE_QUERIES {
            let q = self.cached_iterative_queries.pop_lru();
            self.decrement_cached_iterative_query_stats(q.map(|q| q.1));
        }

        let closest = query.closest();
        let responders = query.responders();

        if closest.nodes().is_empty() {
            // We are clearly offline.
            return;
        }

        let dht_size_estimate = closest.dht_size_estimate();
        let responders_dht_size_estimate = responders.dht_size_estimate();
        let subnets_count = responders.subnets_count();

        let previous = self.cached_iterative_queries.put(
            query.target(),
            CachedIterativeQuery {
                closest_responding_nodes: closest_responding_nodes.into(),
                dht_size_estimate,
                responders_dht_size_estimate,
                subnets: subnets_count,

                request_type: query.request.request_type.clone(),
            },
        );

        self.decrement_cached_iterative_query_stats(previous);

        self.routing_table.increment_responders_stats(
            dht_size_estimate,
            responders_dht_size_estimate,
            subnets_count,
        );

        // Only for get queries, not find node.
        if !matches!(query.request.request_type, RequestTypeSpecific::FindNode(_)) {
            debug!(
                target = ?query.target(),
                responders_size_estimate = ?self.routing_table.responders_based_dht_size_estimate(),
                responders_subnets_count = ?self.routing_table.average_subnets(),
                "Storing nodes stats..",
            );
        }
    }

    /// Decrement stats after an iterative query is popped
    fn decrement_cached_iterative_query_stats(&mut self, query: Option<CachedIterativeQuery>) {
        if let Some(CachedIterativeQuery {
            dht_size_estimate,
            responders_dht_size_estimate,
            subnets,
            request_type,
            ..
        }) = query
        {
            match request_type {
                RequestTypeSpecific::FindNode(..) => {
                    self.routing_table
                        .decrement_dht_size_estimate(dht_size_estimate);
                }
                _ => {
                    self.routing_table.decrement_responders_stats(
                        dht_size_estimate,
                        responders_dht_size_estimate,
                        subnets,
                    );
                }
            }
        };
    }
}

pub(crate) struct CachedIterativeQuery {
    closest_responding_nodes: Box<[Node]>,
    dht_size_estimate: f64,
    responders_dht_size_estimate: f64,
    subnets: u8,

    request_type: RequestTypeSpecific,
}

#[derive(Debug, Clone)]
pub enum Response {
    Peers(Vec<SocketAddrV4>),
    Immutable(Box<[u8]>),
    Mutable(MutableItem),
}
