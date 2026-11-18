//! Manage iterative queries and their corresponding request/response.

use std::collections::HashMap;
use std::net::SocketAddrV4;

use tracing::{debug, trace};

use crate::ClosestNodes;
use crate::PutRequestSpecific;
use crate::actor::socket::KrpcSocket;
use crate::common::{FindNodeRequestArguments, GetPeersRequestArguments, GetValueRequestArguments};
use crate::common::{Id, MAX_BUCKET_SIZE_K, Node, RequestSpecific, RequestTypeSpecific};
use crate::core::Response;

/// An iterative process of concurrently sending a request to the closest known nodes to
/// the target, updating the routing table with closer nodes discovered in the responses, and
/// repeating this process until no closer nodes (that aren't already queried) are found.
#[derive(Debug)]
pub(crate) struct IterativeQuery {
    pub request: RequestSpecific,
    closest: ClosestNodes,
    responders: ClosestNodes,
    inflight_requests: Vec<u32>,
    /// Nodes we sent a request to, with its transaction id until they respond.
    visited: HashMap<SocketAddrV4, Option<u32>>,
    responses: Vec<Response>,
    public_address_votes: HashMap<SocketAddrV4, u32>,
}

#[derive(Debug)]
pub enum GetRequestSpecific {
    FindNode(FindNodeRequestArguments),
    GetPeers(GetPeersRequestArguments),
    GetValue(GetValueRequestArguments),
}

impl GetRequestSpecific {
    pub fn target(&self) -> Id {
        match self {
            GetRequestSpecific::FindNode(args) => args.target,
            GetRequestSpecific::GetPeers(args) => args.info_hash,
            GetRequestSpecific::GetValue(args) => args.target,
        }
    }
}

impl From<&PutRequestSpecific> for GetRequestSpecific {
    fn from(request: &PutRequestSpecific) -> Self {
        match request {
            PutRequestSpecific::PutImmutable(args) => {
                GetRequestSpecific::GetValue(GetValueRequestArguments {
                    target: args.target,
                    seq: None,
                    salt: None,
                })
            }
            PutRequestSpecific::PutMutable(args) => {
                GetRequestSpecific::GetValue(GetValueRequestArguments {
                    target: args.target,
                    seq: None,
                    salt: args.salt.clone(),
                })
            }
            PutRequestSpecific::AnnouncePeer(args) => {
                GetRequestSpecific::GetPeers(GetPeersRequestArguments {
                    info_hash: args.info_hash,
                })
            }
        }
    }
}

impl IterativeQuery {
    pub fn new(requester_id: Id, target: Id, request: GetRequestSpecific) -> Self {
        let request_type = match request {
            GetRequestSpecific::FindNode(s) => RequestTypeSpecific::FindNode(s),
            GetRequestSpecific::GetPeers(s) => RequestTypeSpecific::GetPeers(s),
            GetRequestSpecific::GetValue(s) => RequestTypeSpecific::GetValue(s),
        };

        trace!(?target, ?request_type, "New Query");

        Self {
            request: RequestSpecific {
                requester_id,
                request_type,
            },

            closest: ClosestNodes::new(target),
            responders: ClosestNodes::new(target),

            inflight_requests: Vec::new(),
            visited: HashMap::new(),

            responses: Vec::new(),

            public_address_votes: HashMap::new(),
        }
    }

    // === Getters ===

    pub fn target(&self) -> Id {
        self.responders.target()
    }

    /// Closest nodes according to other nodes.
    pub fn closest(&self) -> &ClosestNodes {
        &self.closest
    }

    /// Return the closest responding nodes after the query is done.
    pub fn responders(&self) -> &ClosestNodes {
        &self.responders
    }

    pub fn responses(&self) -> &[Response] {
        &self.responses
    }

    pub fn best_address(&self) -> Option<SocketAddrV4> {
        let mut max = 0_u32;
        let mut best_addr = None;

        for (addr, count) in self.public_address_votes.iter() {
            if *count > max {
                max = *count;
                best_addr = Some(*addr);
            };
        }

        best_addr
    }

    // === Public Methods ===

    /// Add a candidate node to query on next tick if it is among the closest nodes.
    pub fn add_candidate(&mut self, node: Node) {
        // ready for a ipv6 routing table?
        self.closest.add(node);
    }

    /// Add a vote for this node's address.
    pub fn add_address_vote(&mut self, address: SocketAddrV4) {
        self.public_address_votes
            .entry(address)
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
    }

    /// Visit explicitly given addresses, add them to the visited set, and return the transaction id.
    /// only used from the Rpc when calling bootstrapping nodes.
    pub fn visit(&mut self, socket: &mut KrpcSocket, address: SocketAddrV4) -> u32 {
        let tid = socket.request(address, self.request.clone());
        self.inflight_requests.push(tid);
        self.visited.insert(address, Some(tid));
        tid
    }

    /// Return true if a response (by transaction_id) is expected by this query.
    pub fn inflight(&self, tid: u32) -> bool {
        self.inflight_requests.contains(&tid)
    }

    /// Record that the node at `address` responded to this query.
    pub fn responded(&mut self, address: SocketAddrV4) {
        if let Some(tid) = self.visited.get_mut(&address) {
            *tid = None;
        }
    }

    /// Add a node that responded with a token as a probable storage node.
    pub fn add_responding_node(&mut self, node: Node) {
        self.responders.add(node)
    }

    /// Store received response.
    pub fn response(&mut self, from: SocketAddrV4, response: Response) {
        let target = self.target();

        trace!(?target, ?response, ?from, "Query got response");

        self.responses.push(response.to_owned());
    }

    pub fn visit_closest(&mut self, socket: &mut KrpcSocket) {
        let to_visit = self.closest_candidates(|tid| socket.inflight(&tid));

        for address in to_visit {
            self.visit(socket, address);
        }
    }

    /// Returns the nodes to visit next: the closest nodes that we did not visit yet.
    ///
    /// Nodes that never responded and whose request is no longer in flight (`inflight`
    /// returns false for it) are skipped before taking the closest, so that they cannot hold
    /// on to a place among the closest nodes and end the query early.
    pub fn closest_candidates(&self, inflight: impl Fn(u32) -> bool) -> Vec<SocketAddrV4> {
        let unresponsive = |address: &SocketAddrV4| matches!(self.visited.get(address), Some(Some(tid)) if !inflight(*tid));
        self.closest
            .nodes()
            .iter()
            .filter(|node| !unresponsive(&node.address()))
            .take(MAX_BUCKET_SIZE_K)
            .filter(|node| !self.visited.contains_key(&node.address()))
            .map(|node| node.address())
            .collect()
    }

    /// Returns true if it is done.
    pub fn is_done(&self, socket: &KrpcSocket) -> bool {
        // If no more inflight_requests are inflight in the socket (not timed out),
        // then the query is done.
        let done = !self
            .inflight_requests
            .iter()
            .any(|&tid| socket.inflight(&tid));

        if done {
            debug!(id=?self.target(), closest = ?self.closest.len(), visited = ?self.visited.len(), responders = ?self.responders.len(), "Done query");
        };

        done
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn unresponsive_nodes_do_not_crowd_out_further_nodes() {
        let mut socket = KrpcSocket::client().unwrap();
        let target = Id::random();
        let mut query = IterativeQuery::new(
            Id::random(),
            target,
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: None,
                salt: None,
            }),
        );
        for i in 1..=MAX_BUCKET_SIZE_K as u8 {
            let mut id = *target.as_bytes();
            id[19] ^= i;
            let address = SocketAddrV4::new([127, 66, 0, i].into(), 1);
            query.add_candidate(Node::new(id.into(), address));
        }
        let mut id = *target.as_bytes();
        id[0] ^= 1;
        let further = SocketAddrV4::new([127, 0, 0, 1].into(), 1);
        query.add_candidate(Node::new(id.into(), further));

        query.visit_closest(&mut socket);
        while !query.is_done(&socket) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        query.visit_closest(&mut socket);

        assert!(query.visited.contains_key(&further));
    }

    #[test]
    fn only_nodes_that_timed_out_without_responding_lose_their_place() {
        let target = Id::random();
        let mut query = IterativeQuery::new(
            Id::random(),
            target,
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: None,
                salt: None,
            }),
        );
        let mut close = vec![];
        for i in 1..=MAX_BUCKET_SIZE_K as u8 {
            let mut id = *target.as_bytes();
            id[19] ^= i;
            let address = SocketAddrV4::new([127, 66, 0, i].into(), 1);
            query.add_candidate(Node::new(id.into(), address));
            query.visited.insert(address, Some(i.into()));
            close.push(address);
        }
        let mut id = *target.as_bytes();
        id[0] ^= 1;
        let further = SocketAddrV4::new([127, 0, 0, 1].into(), 1);
        query.add_candidate(Node::new(id.into(), further));

        // Requests still in flight: the close nodes keep their places.
        assert!(query.closest_candidates(|_| true).is_empty());
        // Timed out without a response: the further node is next.
        assert_eq!(query.closest_candidates(|_| false), vec![further]);
        // Responded, even late: they get their places back, and the query is done.
        for address in close {
            query.responded(address);
        }
        assert!(query.closest_candidates(|_| false).is_empty());
    }
}
