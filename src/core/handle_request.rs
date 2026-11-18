use std::net::SocketAddrV4;

use crate::RequestSpecific;
use crate::common::messages::MessageType;
use crate::core::Core;
use crate::core::supports_signed_peers;

use crate::common::{Node, RequestTypeSpecific};

impl Core {
    pub fn handle_request(
        &mut self,
        from: SocketAddrV4,
        request_from_read_only_node: bool,
        version: Option<[u8; 4]>,
        request_specific: RequestSpecific,
    ) -> Option<MessageType> {
        if self.mode != crate::Mode::Server || !self.server.allow_request(&request_specific, from) {
            return None;
        }

        self.maybe_add_node_from_request(
            from,
            version,
            request_from_read_only_node,
            &request_specific,
        );

        self.server.handle_allowed_request(
            &self.routing_table,
            &self.signed_peers_routing_table,
            from,
            request_specific,
        )
    }

    /// In client mode, we never add to our table any node contacting us
    /// without querying it first, to avoid eclipse attacks.
    ///
    /// While bootstrapping though, we can't be that picky, we have two
    /// reasons to except that rule:
    ///
    /// 1. first node creating the DHT;
    ///    without this exception, the bootstrapping node's routing table will never be populated.
    /// 2. Bootstrapping signed_peers_routing_table requires that we latch to any node
    ///    that claims to support it.
    ///    In `periodic_node_maintaenance` fake unresponsive nodes will be removed.
    ///    And either way we prioritize secure nodes, so making up nodes from same
    //    machine won't have much effect.
    fn maybe_add_node_from_request(
        &mut self,
        from: SocketAddrV4,
        version: Option<[u8; 4]>,
        request_from_read_only_node: bool,
        request_specific: &RequestSpecific,
    ) {
        if self.mode == crate::Mode::Server
            && !request_from_read_only_node
            && let RequestTypeSpecific::FindNode(_) = request_specific.request_type
        {
            let node = Node::new(request_specific.requester_id, from);
            let supports_signed_peers = supports_signed_peers(version);

            if self.bootstrap.is_empty() {
                self.routing_table.add(node.clone());

                if supports_signed_peers {
                    self.signed_peers_routing_table.add(node);
                }
            } else if supports_signed_peers {
                self.signed_peers_routing_table.add(node);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::FindNodeRequestArguments;
    use crate::{Id, Mode, RequestFilter, ServerSettings};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug, Clone)]
    struct CountingFilter {
        allow: bool,
        calls: Arc<AtomicUsize>,
    }
    impl RequestFilter for CountingFilter {
        fn allow_request(&self, _: &RequestSpecific, _: SocketAddrV4) -> bool {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.allow
        }
    }

    fn check(allow: bool, bootstrapped: bool, mode: Mode, read_only: bool) {
        let calls = Arc::new(AtomicUsize::new(0));
        let from = SocketAddrV4::new([203, 0, 113, 10].into(), 6881);
        let requester_id = Id::from_ipv4(*from.ip());
        let target = Id::random();
        assert_ne!(requester_id, target);
        let bootstrap = if bootstrapped { vec![from] } else { vec![] };
        let mut core = Core::new(
            Id::random(),
            bootstrap,
            mode,
            ServerSettings {
                filter: Box::new(CountingFilter {
                    allow,
                    calls: calls.clone(),
                }),
                ..Default::default()
            },
        );
        let response = core.handle_request(
            from,
            read_only,
            Some(crate::core::VERSION),
            RequestSpecific {
                requester_id,
                request_type: RequestTypeSpecific::FindNode(FindNodeRequestArguments { target }),
            },
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            usize::from(mode == Mode::Server)
        );
        assert_eq!(response.is_some(), allow && mode == Mode::Server);
        for (table, should_learn) in [
            (&core.routing_table, !bootstrapped),
            (&core.signed_peers_routing_table, true),
        ] {
            let nodes: Vec<_> = table.nodes().collect();
            if allow && mode == Mode::Server && !read_only && should_learn {
                assert_eq!(nodes.len(), 1);
                assert_eq!(*nodes[0].id(), requester_id);
                assert_eq!(nodes[0].address(), from);
            } else {
                assert!(nodes.is_empty());
            }
        }
    }

    #[test]
    fn rejected_requests_leave_both_routing_tables_unchanged() {
        for bootstrapped in [false, true] {
            check(false, bootstrapped, Mode::Server, false);
        }
    }

    #[test]
    fn accepted_requests_learn_requester_id_and_filter_only_once() {
        for bootstrapped in [false, true] {
            check(true, bootstrapped, Mode::Server, false);
        }
    }

    #[test]
    fn client_mode_and_read_only_requesters_do_not_populate_tables() {
        check(true, false, Mode::Client, false);
        check(true, false, Mode::Server, true);
    }
}
