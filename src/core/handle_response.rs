use std::net::{Ipv4Addr, SocketAddrV4};

use tracing::{debug, trace};

use crate::{
    MutableItem, Node,
    common::{
        GetImmutableResponseArguments, GetMutableResponseArguments, GetPeersResponseArguments,
        Message, NoMoreRecentValueResponseArguments, NoValuesResponseArguments,
        RequestTypeSpecific, ResponseSpecific, messages::MessageType, validate_immutable,
    },
    core::Response,
};

use super::{Core, QueryId};

impl Core {
    pub(crate) fn handle_response(
        &mut self,
        from: SocketAddrV4,
        message: Message,
    ) -> Option<(QueryId, Response)> {
        // If someone claims to be readonly, then let's not store anything even if they respond.
        if message.read_only {
            return None;
        };

        // The socket only hands us responses to our own requests, from the
        // address we sent them to, so the responder exists at that address.
        let transaction_id = u32::from_be_bytes(message.transaction_id.as_ref().try_into().ok()?);
        let mut should_add_node = true;
        let mut result = None;
        let author_id = message.get_author_id();
        let from_version = message.version.to_owned();

        // If the response looks like a Ping response, check StoreQueries for the transaction_id.
        if let Some(query) = self
            .put_queries
            .values_mut()
            .find(|query| query.inflight(transaction_id))
        {
            match message.message_type {
                MessageType::Response(ResponseSpecific::Ping(_)) => {
                    // Mark storage at that node as a success.
                    query.success();
                }
                MessageType::Error(error) => query.error(error),
                _ => {}
            };
        } else if let Some((query_id, query)) = self
            .iterative_queries
            .iter_mut()
            .find(|(_, query)| query.inflight(transaction_id))
        {
            query.responded(from);
            if let Some(id) = author_id {
                query.add_answered(Node::new(id, from));
            }

            if let Some(nodes) = message.get_closer_nodes() {
                // A node on the Internet must not be able to point our requests at our local
                // network. Nodes in a local network may name each other, so that test networks
                // and DHTs inside a LAN keep working.
                let from_local_network = is_local_network(*from.ip());
                query.add_named_candidates(
                    nodes
                        .iter()
                        .filter(|node| {
                            from_local_network || !is_local_network(*node.address().ip())
                        })
                        .cloned(),
                );
            }

            if let Some((responder_id, token)) = message.get_token() {
                query.add_responding_node(Node::new_with_token(responder_id, from, token.into()));
            }

            if let Some(proposed_ip) = message.requester_ip {
                query.add_address_vote(proposed_ip);
            }

            let target = query.target();

            match message.message_type {
                MessageType::Response(ResponseSpecific::GetPeers(GetPeersResponseArguments {
                    values,
                    ..
                })) => {
                    let response = Response::Peers(values);
                    query.response(from, response.clone());

                    result = Some((*query_id, response));
                }
                MessageType::Response(ResponseSpecific::GetImmutable(
                    GetImmutableResponseArguments {
                        v, responder_id, ..
                    },
                )) => {
                    if validate_immutable(&v, query.target()) {
                        let response = Response::Immutable(v);
                        query.response(from, response.clone());

                        result = Some((*query_id, response));
                    } else {
                        debug!(
                            ?v,
                            ?target,
                            ?responder_id,
                            ?from,
                            ?from_version,
                            "Invalid immutable value"
                        );

                        should_add_node = false;
                    }
                }
                MessageType::Response(ResponseSpecific::GetMutable(
                    GetMutableResponseArguments {
                        v,
                        seq,
                        sig,
                        k,
                        responder_id,
                        ..
                    },
                )) => {
                    let salt = match query.request.request_type.clone() {
                        RequestTypeSpecific::GetValue(args) => args.salt,
                        _ => None,
                    };
                    match MutableItem::from_dht_message(query.target(), &k, v, seq, &sig, salt) {
                        Ok(item) => {
                            let min_seq = match &query.request.request_type {
                                RequestTypeSpecific::GetValue(args) => args.seq,
                                _ => None,
                            };
                            if min_seq.is_none_or(|seq| item.seq() > seq) {
                                let response = Response::Mutable(item);
                                query.response(from, response.clone());

                                result = Some((*query_id, response));
                            }
                        }
                        Err(error) => {
                            debug!(
                                ?error,
                                ?from,
                                ?responder_id,
                                ?from_version,
                                "Invalid mutable record"
                            );

                            should_add_node = false;
                        }
                    }
                }
                MessageType::Response(ResponseSpecific::NoMoreRecentValue(
                    NoMoreRecentValueResponseArguments {
                        seq, responder_id, ..
                    },
                )) => {
                    trace!(
                        target= ?query.target(),
                        salt= ?match query.request.request_type.clone() {
                            RequestTypeSpecific::GetValue(args) => args.salt,
                            _ => None,
                        },
                        ?seq,
                        ?from,
                        ?responder_id,
                        ?from_version,
                        "No more recent value"
                    );
                }
                MessageType::Response(ResponseSpecific::NoValues(NoValuesResponseArguments {
                    responder_id,
                    ..
                })) => {
                    trace!(
                        target= ?query.target(),
                        salt= ?match query.request.request_type.clone() {
                            RequestTypeSpecific::GetValue(args) => args.salt,
                            _ => None,
                        },
                        ?from,
                        ?responder_id,
                        ?from_version ,
                        "No values"
                    );
                }
                MessageType::Error(error) => {
                    debug!(?error, ?from_version, "Get query got error response");
                }
                // Ping response is already handled in add_node()
                // FindNode response is already handled in query.add_named_candidates()
                // Requests are handled elsewhere
                MessageType::Response(ResponseSpecific::Ping(_))
                | MessageType::Response(ResponseSpecific::FindNode(_))
                | MessageType::Request(_) => {}
            };
        };

        if should_add_node && let Some(id) = author_id {
            self.routing_table.add(Node::new(id, from));
        }

        result
    }
}

/// Returns whether `ip` is not a public Internet address.
fn is_local_network(ip: Ipv4Addr) -> bool {
    ip.is_private()
        || matches!(ip.octets(), [100, 64..=127, ..]) // shared address space, RFC 6598
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_broadcast()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Id,
        actor::socket::KrpcSocket,
        common::{
            FindNodeRequestArguments, FindNodeResponseArguments, GetValueRequestArguments,
            hash_immutable,
        },
        core::GetRequestSpecific,
    };

    #[tokio::test]
    async fn invalid_values_do_not_add_responder() {
        let mut core = Core::new(
            Id::random(),
            vec![],
            crate::Mode::Client,
            Default::default(),
        );
        let mut socket = KrpcSocket::client().unwrap();
        let v: Box<[u8]> = Box::from(&b"value"[..]);
        let target = Id::from_bytes(hash_immutable(&v)).unwrap();
        let (id, mut query, _) = core.create_iterative_query(
            GetRequestSpecific::GetValue(GetValueRequestArguments {
                target,
                seq: None,
                salt: None,
            }),
            None,
        );
        let liar = SocketAddrV4::new([203, 0, 113, 1].into(), 6881);
        let honest = SocketAddrV4::new([203, 0, 113, 2].into(), 6881);
        let liar_tid = query.visit(&mut socket, liar);
        let honest_tid = query.visit(&mut socket, honest);
        core.iterative_queries.insert(id, query);

        let response = |transaction_id: u32, v| Message {
            transaction_id: transaction_id.to_be_bytes().into(),
            version: None,
            requester_ip: None,
            read_only: false,
            message_type: MessageType::Response(ResponseSpecific::GetImmutable(
                GetImmutableResponseArguments {
                    responder_id: Id::random(),
                    token: Box::new([1]),
                    nodes: None,
                    v,
                },
            )),
        };
        core.handle_response(liar, response(liar_tid, Box::from(&b"other"[..])));
        core.handle_response(honest, response(honest_tid, v));

        let addresses: Vec<_> = core.routing_table.nodes().map(|n| n.address()).collect();
        assert_eq!(addresses, [honest]);
    }

    #[tokio::test]
    async fn internet_node_cannot_aim_requests_at_local_network() {
        let mut core = Core::new(
            Id::random(),
            vec![],
            crate::Mode::Client,
            Default::default(),
        );
        let mut socket = KrpcSocket::client().unwrap();
        let target = Id::random();
        let (id, mut query, _) = core.create_iterative_query(
            GetRequestSpecific::FindNode(FindNodeRequestArguments { target }),
            None,
        );
        let internet = SocketAddrV4::new([203, 0, 113, 1].into(), 6881);
        let tid = query.visit(&mut socket, internet);
        core.iterative_queries.insert(id, query);

        let public = Node::new(
            Id::random(),
            SocketAddrV4::new([198, 51, 100, 1].into(), 6881),
        );
        let local = Node::new(
            Id::random(),
            SocketAddrV4::new([192, 168, 1, 2].into(), 6881),
        );
        let response = Message {
            transaction_id: tid.to_be_bytes().into(),
            version: None,
            requester_ip: None,
            read_only: false,
            message_type: MessageType::Response(ResponseSpecific::FindNode(
                FindNodeResponseArguments {
                    responder_id: Id::random(),
                    nodes: [public.clone(), local.clone()].into(),
                },
            )),
        };
        core.handle_response(internet, response);

        let closest = core.iterative_queries[&id].closest().nodes();
        assert!(closest.contains(&public));
        assert!(!closest.contains(&local));
    }

    #[tokio::test]
    async fn made_up_nodes_do_not_hide_real_nodes() {
        let mut core = Core::new(
            Id::random(),
            vec![],
            crate::Mode::Client,
            Default::default(),
        );
        let mut socket = KrpcSocket::client().unwrap();
        let target = Id::random();
        let (id, mut query, _) = core.create_iterative_query(
            GetRequestSpecific::FindNode(FindNodeRequestArguments { target }),
            None,
        );
        let attacker = SocketAddrV4::new([203, 0, 113, 1].into(), 6881);
        let honest = SocketAddrV4::new([203, 0, 113, 2].into(), 6881);
        let attacker_tid = query.visit(&mut socket, attacker);
        let honest_tid = query.visit(&mut socket, honest);
        core.iterative_queries.insert(id, query);

        let storage_nodes = [
            Node::new(
                Id::random(),
                SocketAddrV4::new([198, 51, 100, 1].into(), 6881),
            ),
            Node::new(
                Id::random(),
                SocketAddrV4::new([198, 51, 100, 2].into(), 6881),
            ),
        ];
        let wrong_port_node = Node::new(
            *storage_nodes[0].id(),
            SocketAddrV4::new([198, 51, 100, 1].into(), 7777),
        );
        let other_ip_node = Node::new(
            *storage_nodes[1].id(),
            SocketAddrV4::new([198, 51, 100, 3].into(), 6881),
        );
        let response = |transaction_id: u32, nodes: &[Node]| Message {
            transaction_id: transaction_id.to_be_bytes().into(),
            version: None,
            requester_ip: None,
            read_only: false,
            message_type: MessageType::Response(ResponseSpecific::FindNode(
                FindNodeResponseArguments {
                    responder_id: Id::random(),
                    nodes: nodes.into(),
                },
            )),
        };
        core.handle_response(
            attacker,
            response(attacker_tid, &[wrong_port_node, other_ip_node]),
        );
        core.handle_response(honest, response(honest_tid, &storage_nodes));

        let closest = core.iterative_queries[&id].closest().nodes();
        for node in &storage_nodes {
            assert!(closest.contains(node), "{node:?} is not a candidate");
        }
    }

    #[test]
    fn local_network_addresses() {
        for ip in [
            [10, 0, 0, 1],
            [172, 16, 0, 1],
            [192, 168, 1, 2],
            [100, 64, 0, 1],
            [100, 127, 255, 255],
            [127, 0, 0, 1],
            [169, 254, 0, 1],
            [0, 0, 0, 0],
            [224, 0, 0, 1],
            [255, 255, 255, 255],
        ] {
            assert!(is_local_network(ip.into()), "{ip:?}");
        }
        for ip in [
            [1, 1, 1, 1],
            [100, 63, 0, 1],
            [100, 128, 0, 1],
            [198, 51, 100, 1],
        ] {
            assert!(!is_local_network(ip.into()), "{ip:?}");
        }
    }
}
