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
        self.maybe_add_node_from_request(
            from,
            version,
            request_from_read_only_node,
            &request_specific,
        );

        if self.mode == crate::Mode::Server {
            let server = &mut self.server;
            let response = server.handle_request(
                &self.routing_table,
                &self.signed_peers_routing_table,
                from,
                request_specific,
            );

            match response {
                Some(MessageType::Error(_)) | Some(MessageType::Response(_)) => response,
                _ => None,
            }
        } else {
            None
        }
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
            && let RequestTypeSpecific::FindNode(ref param) = request_specific.request_type
        {
            let node = Node::new(param.target, from);
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
