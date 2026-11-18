use std::net::SocketAddrV4;
use std::time::Instant;

use crate::RequestSpecific;
use crate::common::messages::MessageType;
use crate::core::{Core, REQUESTER_PING_INTERVAL, supports_signed_peers};

use crate::common::{RequestTypeSpecific, RoutingTable};

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

        self.maybe_ping_requester(
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

    /// Pings a requester that we might want in our routing tables.
    ///
    /// A request proves nothing: its sender address can be forged, and the
    /// requester picks its own id. So we never add a requester directly. We
    /// ping it instead, and its response, which the socket matches against
    /// our random transaction id and the address we pinged, adds it in
    /// [`Core::handle_response`].
    ///
    /// We only do this for `find_node` requests, and only in two cases:
    ///
    /// 1. While we have no bootstrap nodes, e.g. as the first node of a DHT;
    ///    otherwise our routing table would never be populated.
    /// 2. For requesters that claim to support signed peers, so that the
    ///    `signed_peers_routing_table` can be bootstrapped.
    fn maybe_ping_requester(
        &mut self,
        from: SocketAddrV4,
        version: Option<[u8; 4]>,
        request_from_read_only_node: bool,
        request_specific: &RequestSpecific,
    ) {
        let RequestTypeSpecific::FindNode(_) = request_specific.request_type else {
            return;
        };
        let supports_signed_peers = supports_signed_peers(version);
        if request_from_read_only_node || !(self.bootstrap.is_empty() || supports_signed_peers) {
            return;
        }

        let known = |table: &RoutingTable| table.nodes().any(|node| node.address() == from);
        if known(&self.routing_table)
            && (!supports_signed_peers || known(&self.signed_peers_routing_table))
        {
            return;
        }
        if self
            .recently_pinged
            .get(&from)
            .is_some_and(|pinged| pinged.elapsed() < REQUESTER_PING_INTERVAL)
        {
            return;
        }

        self.recently_pinged.put(from, Instant::now());
        self.pending_pings.push(from);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{
        FindNodeRequestArguments, Message, PingResponseArguments, ResponseSpecific,
    };
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

    fn core(allow: bool, bootstrapped: bool, mode: Mode, calls: Arc<AtomicUsize>) -> Core {
        let bootstrap = if bootstrapped {
            vec![SocketAddrV4::new([198, 51, 100, 1].into(), 6881)]
        } else {
            vec![]
        };
        Core::new(
            Id::random(),
            bootstrap,
            mode,
            ServerSettings {
                filter: Box::new(CountingFilter { allow, calls }),
                ..Default::default()
            },
        )
    }

    fn find_node(core: &mut Core, from: SocketAddrV4, read_only: bool) -> Option<MessageType> {
        core.handle_request(
            from,
            read_only,
            Some(crate::core::VERSION),
            RequestSpecific {
                requester_id: Id::from_ipv4(*from.ip()),
                request_type: RequestTypeSpecific::FindNode(FindNodeRequestArguments {
                    target: Id::random(),
                }),
            },
        )
    }

    fn ping_response(core: &mut Core, from: SocketAddrV4, responder_id: Id) {
        core.handle_response(
            from,
            Message {
                transaction_id: 0,
                version: Some(crate::core::VERSION),
                requester_ip: None,
                read_only: false,
                message_type: MessageType::Response(ResponseSpecific::Ping(
                    PingResponseArguments { responder_id },
                )),
            },
        );
    }

    fn check(allow: bool, bootstrapped: bool, mode: Mode, read_only: bool) {
        let calls = Arc::new(AtomicUsize::new(0));
        let from = SocketAddrV4::new([203, 0, 113, 10].into(), 6881);
        let mut core = core(allow, bootstrapped, mode, calls.clone());
        let response = find_node(&mut core, from, read_only);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            usize::from(mode == Mode::Server)
        );
        assert_eq!(response.is_some(), allow && mode == Mode::Server);
        // A request alone never adds the requester.
        assert!(core.routing_table.is_empty());
        assert!(core.signed_peers_routing_table.is_empty());
        let should_ping = allow && mode == Mode::Server && !read_only;
        let pings = core.take_pending_pings();
        assert_eq!(pings, if should_ping { vec![from] } else { vec![] });
    }

    #[test]
    fn rejected_requests_do_not_ping() {
        for bootstrapped in [false, true] {
            check(false, bootstrapped, Mode::Server, false);
        }
    }

    #[test]
    fn accepted_requests_ping_requester_and_filter_only_once() {
        for bootstrapped in [false, true] {
            check(true, bootstrapped, Mode::Server, false);
        }
    }

    #[test]
    fn client_mode_and_read_only_requesters_are_not_pinged() {
        check(true, false, Mode::Client, false);
        check(true, false, Mode::Server, true);
    }

    #[test]
    fn requester_is_pinged_once_and_added_by_its_response() {
        let from = SocketAddrV4::new([203, 0, 113, 10].into(), 6881);
        let mut core = core(true, false, Mode::Server, Default::default());

        find_node(&mut core, from, false);
        assert_eq!(core.take_pending_pings(), vec![from]);
        // Repeated requests do not trigger another ping.
        find_node(&mut core, from, false);
        assert!(core.take_pending_pings().is_empty());

        let id = Id::from_ipv4(*from.ip());
        ping_response(&mut core, from, id);
        for table in [&core.routing_table, &core.signed_peers_routing_table] {
            let nodes: Vec<_> = table.nodes().collect();
            assert_eq!(nodes.len(), 1);
            assert_eq!(*nodes[0].id(), id);
            assert_eq!(nodes[0].address(), from);
        }
    }

    #[test]
    fn known_requesters_are_not_pinged() {
        let from = SocketAddrV4::new([203, 0, 113, 10].into(), 6881);
        let mut core = core(true, false, Mode::Server, Default::default());
        ping_response(&mut core, from, Id::from_ipv4(*from.ip()));

        find_node(&mut core, from, false);
        assert!(core.take_pending_pings().is_empty());
    }
}
