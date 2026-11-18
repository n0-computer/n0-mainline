use tracing::{debug, trace};

use crate::{
    Node,
    common::{
        ErrorSpecific, Id, PutRequest, PutRequestSpecific, RequestSpecific, RequestTypeSpecific,
    },
};

use crate::actor::socket::KrpcSocket;

#[derive(Debug)]
/// Once an [super::IterativeQuery] is done, or if a previous cached one was a available,
/// we can store data at the closest nodes using this PutQuery, that keeps track of
/// acknowledging nodes, and or errors.
pub struct PutQuery {
    pub target: Id,
    /// The token lookup this put is waiting for, if it has not started yet.
    pub lookup_id: Option<super::QueryId>,
    /// Nodes that confirmed success
    stored_at: usize,
    inflight_requests: Vec<u32>,
    pub request: PutRequestSpecific,
    errors: Vec<(usize, ErrorSpecific)>,
    extra_nodes: Box<[Node]>,
}

impl PutQuery {
    pub fn new(request: PutRequestSpecific, extra_nodes: Option<Box<[Node]>>) -> Self {
        Self {
            target: *request.target(),
            lookup_id: None,
            stored_at: 0,
            inflight_requests: Vec::new(),
            request,
            errors: Vec::new(),
            extra_nodes: extra_nodes.unwrap_or(Box::new([])),
        }
    }

    pub fn start(
        &mut self,
        socket: &mut KrpcSocket,
        closest_nodes: &[Node],
    ) -> Result<(), PutError> {
        if self.started() {
            return Ok(());
        };

        self.lookup_id = None;
        let target = self.target;
        debug!(?target, closest_secure_nodes = ?closest_nodes.len(), "PutQuery start");

        for node in closest_nodes.iter().chain(self.extra_nodes.iter()) {
            // Set correct values to the request placeholders
            if let Some(token) = node.token() {
                let tid = socket.request(
                    node.address(),
                    RequestSpecific {
                        requester_id: Id::random(),
                        request_type: RequestTypeSpecific::Put(PutRequest {
                            token,
                            put_request_type: self.request.clone(),
                        }),
                    },
                );

                self.inflight_requests.push(tid);
            }
        }

        if !self.started() {
            return Err(PutQueryError::NoClosestNodes.into());
        }
        Ok(())
    }

    pub fn started(&self) -> bool {
        !self.inflight_requests.is_empty()
    }

    pub fn inflight(&self, tid: u32) -> bool {
        self.inflight_requests.contains(&tid)
    }

    pub fn success(&mut self) {
        trace!(target = ?self.target, "PutQuery got success response");
        self.stored_at += 1
    }

    pub fn error(&mut self, error: ErrorSpecific) {
        debug!(target = ?self.target, ?error, "PutQuery got error");

        if let Some(pos) = self
            .errors
            .iter()
            .position(|(_, err)| error.code == err.code)
        {
            // Increment the count of the existing error
            self.errors[pos].0 += 1;

            // Move the updated element to maintain the order (highest count first)
            let mut i = pos;
            while i > 0 && self.errors[i].0 > self.errors[i - 1].0 {
                self.errors.swap(i, i - 1);
                i -= 1;
            }
        } else {
            // Add the new error with a count of 1
            self.errors.push((1, error));
        }
    }

    /// Check if the query is either successfully done, or terminated with an error.
    pub fn check(&self, socket: &KrpcSocket) -> Result<bool, PutError> {
        // And all queries got responses or timedout
        if self.is_done(socket) {
            let target = self.target;

            if self.stored_at == 0 {
                let most_common_error = self.most_common_error();

                debug!(
                    ?target,
                    ?most_common_error,
                    nodes_count = self.inflight_requests.len(),
                    "Put Query: failed"
                );

                return Err(most_common_error
                    .map(|(_, error)| error)
                    .unwrap_or(PutQueryError::Timeout.into()));
            }

            debug!(?target, stored_at = ?self.stored_at, "PutQuery Done successfully");

            return Ok(true);
        } else if let Some(most_common_error) = self.majority_nodes_rejected_put_mutable() {
            let target = self.target;

            debug!(
                ?target,
                ?most_common_error,
                nodes_count = self.inflight_requests.len(),
                "PutQuery for MutableItem was rejected by most nodes with 3xx code."
            );

            Err(most_common_error)?;
        }

        Ok(false)
    }

    /// Query started and finished
    fn is_done(&self, socket: &KrpcSocket) -> bool {
        // Didn't start yet.
        if self.inflight_requests.is_empty() {
            return false;
        }

        !self
            .inflight_requests
            .iter()
            .any(|tid| socket.inflight(tid))
    }

    /// Return most common error if any
    fn majority_nodes_rejected_put_mutable(&self) -> Option<ConcurrencyError> {
        let half = (self.inflight_requests.len() / 2) + 1;

        if matches!(self.request, PutRequestSpecific::PutMutable(_)) {
            return self.most_common_error().and_then(|(count, error)| {
                if count >= half {
                    if let PutError::Concurrency(err) = error {
                        Some(err)
                    } else {
                        None
                    }
                } else {
                    None
                }
            });
        };

        None
    }

    fn most_common_error(&self) -> Option<(usize, PutError)> {
        self.errors.first().map(|(count, error)| {
            let error = match (&self.request, error.code) {
                (PutRequestSpecific::PutMutable(_), 301) => ConcurrencyError::CasFailed.into(),
                (PutRequestSpecific::PutMutable(_), 302) => ConcurrencyError::NotMostRecent.into(),
                _ => PutQueryError::ErrorResponse(error.clone()).into(),
            };
            (*count, error)
        })
    }
}

#[n0_error::stack_error(derive, from_sources, std_sources)]
#[derive(Clone)]
/// PutQuery errors
pub enum PutError {
    /// Common PutQuery errors
    #[error(transparent)]
    Query(PutQueryError),

    #[error(transparent)]
    /// PutQuery for [crate::MutableItem] errors
    Concurrency(ConcurrencyError),
}

#[n0_error::stack_error(derive, std_sources)]
#[derive(Clone)]
/// Common PutQuery errors
pub enum PutQueryError {
    /// Failed to find any nodes close, usually means dht node failed to bootstrap,
    /// so the routing table is empty. Check the machine's access to UDP socket,
    /// or find better bootstrapping nodes.
    #[error("Failed to find any nodes close to store value at")]
    NoClosestNodes,

    /// Either Put Query failed to store at any nodes, and most nodes responded
    /// with a non `301` nor `302` errors.
    ///
    /// Either way; contains the most common error response.
    #[error("Query Error Response")]
    ErrorResponse(ErrorSpecific),

    /// PutQuery timed out with no responses neither success or errors
    #[error("PutQuery timed out with no responses neither success or errors")]
    Timeout,

    /// The DHT actor task has shut down.
    #[error("DHT actor task has shut down")]
    Shutdown,
}

#[n0_error::stack_error(derive, std_sources)]
#[derive(Clone)]
/// PutQuery for [crate::MutableItem] errors
pub enum ConcurrencyError {
    /// The [crate::MutableItem::seq] is less than or equal the sequence from another signed item.
    ///
    /// Try reading most recent mutable item before writing again.
    #[error(
        "MutableItem::seq is not the most recent, try reading most recent item before writing again."
    )]
    NotMostRecent,

    /// The `CAS` condition does not match the `seq` of the most recent known signed item.
    #[error("CAS check failed, try reading most recent item before writing again.")]
    CasFailed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MutableItem, SigningKey, actor::config::Config, common::PutMutableRequestArguments,
    };

    #[tokio::test]
    async fn large_puts_count_successes_errors_and_majorities() {
        let mut socket = KrpcSocket::new(&Config {
            bootstrap: vec![],
            port: Some(0),
            ..Default::default()
        })
        .unwrap();
        let node = Node::new_with_token(
            Id::random(),
            "127.0.0.1:12345".parse().unwrap(),
            Box::new([1]),
        );
        let item = MutableItem::new(&SigningKey::from_bytes(&[7; 32]), b"value", 1, None);
        let request = PutRequestSpecific::PutMutable(PutMutableRequestArguments::from(item, None));
        let mut success = PutQuery::new(request.clone(), Some(vec![node.clone(); 511].into()));
        success
            .start(&mut socket, std::slice::from_ref(&node))
            .unwrap();
        assert_eq!(success.inflight_requests.len(), 512);
        for _ in 0..512 {
            success.success();
        }
        assert_eq!(success.stored_at, 512);

        let mut rejected = PutQuery::new(request, Some(vec![node.clone(); 511].into()));
        rejected.start(&mut socket, &[node]).unwrap();
        for count in 1..=257 {
            rejected.error(ErrorSpecific {
                code: 301,
                description: "CAS mismatch".into(),
            });
            assert_eq!(rejected.most_common_error().unwrap().0, count);
            assert_eq!(
                rejected.majority_nodes_rejected_put_mutable().is_some(),
                count > 256
            );
        }
    }

    #[tokio::test]
    async fn no_usable_tokens_returns_an_error() {
        use crate::common::PutImmutableRequestArguments;
        let mut socket = KrpcSocket::new(&Config {
            bootstrap: vec![],
            port: Some(0),
            ..Default::default()
        })
        .unwrap();
        let node = Node::new(Id::random(), "127.0.0.1:12345".parse().unwrap());
        let request = PutRequestSpecific::PutImmutable(PutImmutableRequestArguments {
            target: Id::random(),
            v: Box::new([1]),
        });
        let mut put = PutQuery::new(request.clone(), None);
        assert!(matches!(
            put.start(&mut socket, &[node]),
            Err(PutError::Query(PutQueryError::NoClosestNodes))
        ));
        assert!(!put.started());
        // Extra nodes can supply the usable token even if the lookup found none.
        let extra = Node::new_with_token(
            Id::random(),
            "127.0.0.1:12345".parse().unwrap(),
            Box::new([1]),
        );
        let mut put = PutQuery::new(request, Some(vec![extra].into()));
        put.start(&mut socket, &[]).unwrap();
        assert!(put.started());
    }
}
