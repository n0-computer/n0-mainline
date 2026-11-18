//! Mapping between [`Message`] and its KRPC bencode encoding.

use std::borrow::Cow;

use super::bencode::{self, BencodeError, Dict, Value};
use super::*;

fn bytes<'a>(bytes: impl Into<Cow<'a, [u8]>>) -> Value<'a> {
    Value::Bytes(bytes.into())
}

fn id(id: &Id) -> Value<'_> {
    bytes(&id.as_bytes()[..])
}

fn nodes_value(nodes: &[Node]) -> Value<'static> {
    bytes(nodes4_to_bytes(nodes).into_vec())
}

fn get_id(dict: &Dict<'_>, key: &'static str) -> Result<Id, BencodeError> {
    dict.array(key).map(Id::from)
}

impl Message {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut msg = Dict::default();
        msg.insert(b"t", bytes(&self.transaction_id[..]));
        msg.insert_opt(b"v", self.version.as_ref().map(|v| bytes(&v[..])));
        msg.insert_opt(
            b"ip",
            self.requester_ip
                .map(|ip| bytes(sockaddr_to_bytes(&ip).to_vec())),
        );
        msg.insert(b"ro", Value::Int(self.read_only.into()));
        match &self.message_type {
            MessageType::Request(request) => {
                let (name, arguments) = encode_request(request);
                msg.insert(b"y", bytes(&b"q"[..]));
                msg.insert(b"q", bytes(name));
                msg.insert(b"a", Value::Dict(arguments));
            }
            MessageType::Response(response) => {
                msg.insert(b"y", bytes(&b"r"[..]));
                msg.insert(b"r", Value::Dict(encode_response(response)));
            }
            MessageType::Error(error) => {
                msg.insert(b"y", bytes(&b"e"[..]));
                let info = vec![
                    Value::Int(error.code.into()),
                    bytes(error.description.as_bytes()),
                ];
                msg.insert(b"e", Value::List(info));
            }
        }
        bencode::encode_to_vec(&Value::Dict(msg))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Message, DecodeMessageError> {
        if bytes.len() < 15 {
            return Err(DecodeMessageError::TooShort);
        }
        let Value::Dict(msg) = bencode::decode(bytes)? else {
            return Err(DecodeMessageError::NotBencodeDictionary);
        };
        let transaction_id = Box::from(msg.bytes("t")?);
        let version = msg.opt_array("v")?;
        let requester_ip = msg.opt_array::<6>("ip")?;
        let requester_ip = requester_ip.map(bytes_to_sockaddr).transpose()?;
        let read_only = msg.opt_int::<i32>("ro")?.is_some_and(|ro| ro > 0);
        let message_type = match msg.bytes("y")? {
            b"q" => MessageType::Request(decode_request(&msg)?),
            b"r" => MessageType::Response(decode_response(msg.dict("r")?)?),
            b"e" => MessageType::Error(decode_error(&msg)?),
            _ => return Err(BencodeError::UnknownMessageType.into()),
        };
        Ok(Message {
            transaction_id,
            version,
            requester_ip,
            read_only,
            message_type,
        })
    }
}

fn encode_request(request: &RequestSpecific) -> (&'static [u8], Dict<'_>) {
    let mut a = Dict::default();
    a.insert(b"id", id(&request.requester_id));
    let name: &'static [u8] = match &request.request_type {
        RequestTypeSpecific::Ping => b"ping",
        RequestTypeSpecific::FindNode(args) => {
            a.insert(b"target", id(&args.target));
            b"find_node"
        }
        RequestTypeSpecific::GetPeers(args) => {
            a.insert(b"info_hash", id(&args.info_hash));
            b"get_peers"
        }
        RequestTypeSpecific::GetValue(args) => {
            // `salt` is only carried locally, it is not part of the request.
            a.insert(b"target", id(&args.target));
            a.insert_opt(b"seq", args.seq.map(Value::Int));
            b"get"
        }
        RequestTypeSpecific::Put(PutRequest {
            token,
            put_request_type,
        }) => {
            a.insert(b"token", bytes(&token[..]));
            match put_request_type {
                PutRequestSpecific::AnnouncePeer(args) => {
                    a.insert(b"info_hash", id(&args.info_hash));
                    a.insert(b"port", Value::Int(args.port.into()));
                    // Sent as 1 whenever it is set, even to `false`, as before.
                    let implied_port = args.implied_port.is_some();
                    a.insert(b"implied_port", Value::Int(implied_port.into()));
                    b"announce_peer"
                }
                PutRequestSpecific::PutImmutable(args) => {
                    // Older n0 servers require this redundant field.
                    a.insert(b"target", id(&args.target));
                    a.insert(b"v", bytes(&args.v[..]));
                    b"put"
                }
                PutRequestSpecific::PutMutable(args) => {
                    // Older n0 servers require this redundant field.
                    a.insert(b"target", id(&args.target));
                    a.insert(b"v", bytes(&args.v[..]));
                    a.insert(b"k", bytes(&args.k[..]));
                    a.insert(b"seq", Value::Int(args.seq));
                    a.insert(b"sig", bytes(&args.sig[..]));
                    a.insert_opt(b"salt", args.salt.as_deref().map(bytes));
                    a.insert_opt(b"cas", args.cas.map(Value::Int));
                    b"put"
                }
            }
        }
    };
    (name, a)
}

fn decode_request(msg: &Dict<'_>) -> Result<RequestSpecific, DecodeMessageError> {
    let a = msg.dict("a")?;
    let name = msg.bytes("q")?;
    let requester_id = get_id(a, "id")?;
    let put = |put_request_type| {
        Ok::<_, DecodeMessageError>(RequestTypeSpecific::Put(PutRequest {
            token: a.bytes("token")?.into(),
            put_request_type,
        }))
    };
    let request_type = match name {
        b"ping" => RequestTypeSpecific::Ping,
        b"find_node" => RequestTypeSpecific::FindNode(FindNodeRequestArguments {
            target: get_id(a, "target")?,
        }),
        b"get_peers" => RequestTypeSpecific::GetPeers(GetPeersRequestArguments {
            info_hash: get_id(a, "info_hash")?,
        }),
        b"get" => RequestTypeSpecific::GetValue(GetValueRequestArguments {
            target: get_id(a, "target")?,
            seq: a.opt_int("seq")?,
            salt: None,
        }),
        b"announce_peer" => put(PutRequestSpecific::AnnouncePeer(
            AnnouncePeerRequestArguments {
                info_hash: get_id(a, "info_hash")?,
                port: a.int("port")?,
                implied_port: a.opt_int::<u8>("implied_port")?.map(|p| p != 0),
            },
        ))?,
        b"put" => {
            let v = Box::from(a.bytes("v")?);
            // All optional keys are checked, even those an immutable put ignores.
            let k = a.opt_array("k")?;
            let sig = a.opt_array("sig")?;
            let seq = a.opt_int("seq")?;
            let cas = a.opt_int("cas")?;
            let salt = a.opt_bytes("salt")?.map(Box::from);
            put(match k {
                None => {
                    let target = crate::common::hash_immutable(&v).into();
                    PutRequestSpecific::PutImmutable(PutImmutableRequestArguments { target, v })
                }
                Some(k) => {
                    let (Some(seq), Some(sig)) = (seq, sig) else {
                        return Err(DecodeMessageError::MissingMutablePutSequenceOrSignature);
                    };
                    PutRequestSpecific::PutMutable(PutMutableRequestArguments {
                        target: crate::MutableItem::target_from_key(&k, salt.as_deref()),
                        v,
                        k,
                        seq,
                        sig,
                        salt,
                        cas,
                    })
                }
            })?
        }
        _ => return Err(BencodeError::UnknownMessageType.into()),
    };
    Ok(RequestSpecific {
        requester_id,
        request_type,
    })
}

fn encode_response(response: &ResponseSpecific) -> Dict<'_> {
    let mut r = Dict::default();
    match response {
        ResponseSpecific::Ping(args) => r.insert(b"id", id(&args.responder_id)),
        ResponseSpecific::FindNode(args) => {
            r.insert(b"id", id(&args.responder_id));
            r.insert(b"nodes", nodes_value(&args.nodes));
        }
        ResponseSpecific::GetPeers(args) => {
            insert_token_fields(&mut r, &args.responder_id, &args.token, &args.nodes);
            let values = args.values.iter();
            let values = values.map(|p| bytes(sockaddr_to_bytes(p).to_vec()));
            r.insert(b"values", Value::List(values.collect()));
        }
        ResponseSpecific::NoValues(args) => {
            insert_token_fields(&mut r, &args.responder_id, &args.token, &args.nodes);
        }
        ResponseSpecific::GetImmutable(args) => {
            insert_token_fields(&mut r, &args.responder_id, &args.token, &args.nodes);
            r.insert(b"v", bytes(&args.v[..]));
        }
        ResponseSpecific::GetMutable(args) => {
            insert_token_fields(&mut r, &args.responder_id, &args.token, &args.nodes);
            r.insert(b"v", bytes(&args.v[..]));
            r.insert(b"k", bytes(&args.k[..]));
            r.insert(b"sig", bytes(&args.sig[..]));
            r.insert(b"seq", Value::Int(args.seq));
        }
        ResponseSpecific::NoMoreRecentValue(args) => {
            insert_token_fields(&mut r, &args.responder_id, &args.token, &args.nodes);
            r.insert(b"seq", Value::Int(args.seq));
        }
    }
    r
}

/// Inserts the keys shared by the responses to `get_peers` and `get`.
fn insert_token_fields<'a>(
    r: &mut Dict<'a>,
    responder_id: &'a Id,
    token: &'a [u8],
    nodes: &Option<Box<[Node]>>,
) {
    r.insert(b"id", id(responder_id));
    r.insert(b"token", bytes(token));
    r.insert_opt(b"nodes", nodes.as_deref().map(nodes_value));
}

/// Decodes a response, which carries no type tag, by the keys it has.
///
/// The first shape whose keys are all present and well-formed wins, from most to least
/// detailed. A shape that matches but holds invalid nodes or peers fails the whole message.
fn decode_response(r: &Dict<'_>) -> Result<ResponseSpecific, DecodeMessageError> {
    let responder_id = get_id(r, "id")?;
    if let (Ok(token), Ok(nodes)) = (r.bytes("token"), r.opt_bytes("nodes")) {
        let token = Box::from(token);
        let nodes = nodes.map(bytes_to_nodes4).transpose()?;
        let v = r.bytes("v");
        let seq = r.int::<i64>("seq");
        if let (Ok(v), Ok(k), Ok(sig), Ok(seq)) = (&v, r.array("k"), r.array("sig"), &seq) {
            return Ok(ResponseSpecific::GetMutable(GetMutableResponseArguments {
                responder_id,
                token,
                nodes,
                v: Box::from(*v),
                k,
                seq: *seq,
                sig,
            }));
        }
        if let Ok(seq) = seq {
            return Ok(ResponseSpecific::NoMoreRecentValue(
                NoMoreRecentValueResponseArguments {
                    responder_id,
                    token,
                    nodes,
                    seq,
                },
            ));
        }
        if let Ok(v) = v {
            return Ok(ResponseSpecific::GetImmutable(
                GetImmutableResponseArguments {
                    responder_id,
                    token,
                    nodes,
                    v: v.into(),
                },
            ));
        }
        if let Ok(values) = r.bytes_list("values") {
            return Ok(ResponseSpecific::GetPeers(GetPeersResponseArguments {
                responder_id,
                token,
                values: values
                    .into_iter()
                    .map(bytes_to_sockaddr)
                    .collect::<Result<_, _>>()?,
                nodes,
            }));
        }
        return Ok(ResponseSpecific::NoValues(NoValuesResponseArguments {
            responder_id,
            token,
            nodes,
        }));
    }
    if let Ok(nodes) = r.bytes("nodes") {
        return Ok(ResponseSpecific::FindNode(FindNodeResponseArguments {
            responder_id,
            nodes: bytes_to_nodes4(nodes)?,
        }));
    }
    Ok(ResponseSpecific::Ping(PingResponseArguments {
        responder_id,
    }))
}

fn decode_error(msg: &Dict<'_>) -> Result<ErrorSpecific, BencodeError> {
    let invalid = || BencodeError::InvalidValue("e");
    let [Value::Int(code), Value::Bytes(description)] = msg.list("e")? else {
        return Err(invalid());
    };
    Ok(ErrorSpecific {
        code: i32::try_from(*code).map_err(|_| invalid())?,
        description: String::from_utf8(description.to_vec()).map_err(|_| invalid())?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID_A: &[u8; 20] = b"abcdefghij0123456789";
    const ID_M: &[u8; 20] = b"mnopqrstuvwxyz123456";
    /// Transaction id `aa` from the BEP 5 examples.
    const TID: &[u8] = b"aa";

    fn request(bytes: &[u8]) -> RequestSpecific {
        let msg = Message::from_bytes(bytes).unwrap();
        assert_eq!(&*msg.transaction_id, TID);
        match msg.message_type {
            MessageType::Request(request) => Some(request),
            _ => None,
        }
        .expect("not a request")
    }

    fn response(bytes: &[u8]) -> ResponseSpecific {
        let msg = Message::from_bytes(bytes).unwrap();
        assert_eq!(&*msg.transaction_id, TID);
        match msg.message_type {
            MessageType::Response(response) => Some(response),
            _ => None,
        }
        .expect("not a response")
    }

    // The examples from BEP 5, verbatim.

    #[test]
    fn bep5_ping() {
        let req = request(b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe");
        assert_eq!(req.requester_id, Id::from(*ID_A));
        assert_eq!(req.request_type, RequestTypeSpecific::Ping);

        let res = response(b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re");
        assert_eq!(
            res,
            ResponseSpecific::Ping(PingResponseArguments {
                responder_id: Id::from(*ID_M)
            })
        );
    }

    #[test]
    fn bep5_find_node() {
        let req = request(
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe",
        );
        assert_eq!(
            req.request_type,
            RequestTypeSpecific::FindNode(FindNodeRequestArguments {
                target: Id::from(*ID_M)
            })
        );
    }

    #[test]
    fn bep5_get_peers() {
        let req = request(
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
        );
        assert_eq!(
            req.request_type,
            RequestTypeSpecific::GetPeers(GetPeersRequestArguments {
                info_hash: Id::from(*ID_M)
            })
        );

        let res = response(
            b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re",
        );
        assert_eq!(
            res,
            ResponseSpecific::GetPeers(GetPeersResponseArguments {
                responder_id: Id::from(*ID_A),
                token: b"aoeusnth".to_vec().into(),
                values: vec![
                    bytes_to_sockaddr(b"axje.u").unwrap(),
                    bytes_to_sockaddr(b"idhtnm").unwrap(),
                ],
                nodes: None,
            })
        );
    }

    #[test]
    fn bep5_announce_peer() {
        let req = request(
            b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
        );
        assert_eq!(
            req.request_type,
            RequestTypeSpecific::Put(PutRequest {
                token: b"aoeusnth".to_vec().into(),
                put_request_type: PutRequestSpecific::AnnouncePeer(AnnouncePeerRequestArguments {
                    info_hash: Id::from(*ID_M),
                    port: 6881,
                    implied_port: Some(true),
                }),
            })
        );
    }

    #[test]
    fn bep5_error() {
        let msg =
            Message::from_bytes(b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee").unwrap();
        assert_eq!(
            msg.message_type,
            MessageType::Error(ErrorSpecific {
                code: 201,
                description: "A Generic Error Ocurred".into(),
            })
        );
    }

    #[test]
    fn encode_ping() {
        // Same as the BEP 5 example, plus the `ro` flag we always send.
        let msg = Message {
            transaction_id: TID.into(),
            version: None,
            requester_ip: None,
            read_only: false,
            message_type: MessageType::Request(RequestSpecific {
                requester_id: Id::from(*ID_A),
                request_type: RequestTypeSpecific::Ping,
            }),
        };
        assert_eq!(
            msg.to_bytes(),
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping2:roi0e1:t2:aa1:y1:qe"
        );
    }

    #[test]
    fn unknown_keys_and_unsorted_input() {
        // Keys in reverse order, with unknown keys at both levels.
        let req = request(b"d1:y1:q1:t2:aa1:q4:ping1:zli1ee1:ad1:x0:2:id20:abcdefghij0123456789ee");
        assert_eq!(req.request_type, RequestTypeSpecific::Ping);
    }

    #[test]
    fn rejects_invalid_messages() {
        for bytes in [
            // missing transaction id
            &b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:y1:qe"[..],
            // id of 19 bytes
            b"d1:ad2:id19:abcdefghij012345678e1:q4:ping1:t2:aa1:y1:qe",
            // unknown query
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:pong1:t2:aa1:y1:qe",
            // unknown message type
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:xe",
            // port out of range
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti65536e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
            // peer address of 5 bytes
            b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl5:axje.ee1:t2:aa1:y1:re",
            // error without a message
            b"d1:eli201ee1:t2:aa1:y1:ee",
            // not a dictionary
            b"l1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
        ] {
            assert!(
                Message::from_bytes(bytes).is_err(),
                "{}",
                String::from_utf8_lossy(bytes)
            );
        }
    }

    #[test]
    fn bep44_puts_derive_targets() {
        let immutable =
            b"d1:ad2:id20:abcdefghij01234567895:token1:x1:v5:helloe1:q3:put1:t2:aa1:y1:qe";
        let mutable = b"d1:ad2:id20:abcdefghij01234567891:k32:abcdefghijklmnopqrstuvwxyz1234563:seqi1e3:sig64:abcdefghijklmnopqrstuvwxyz123456abcdefghijklmnopqrstuvwxyz1234565:token1:x1:v5:helloe1:q3:put1:t2:aa1:y1:qe";
        let salted = b"d1:ad2:id20:abcdefghij01234567891:k32:abcdefghijklmnopqrstuvwxyz1234564:salt3:abc3:seqi1e3:sig64:abcdefghijklmnopqrstuvwxyz123456abcdefghijklmnopqrstuvwxyz1234565:token1:x1:v5:helloe1:q3:put1:t2:aa1:y1:qe";
        let key = b"abcdefghijklmnopqrstuvwxyz123456";
        for (wire, expected) in [
            (
                &immutable[..],
                crate::common::hash_immutable(b"hello").into(),
            ),
            (&mutable[..], crate::MutableItem::target_from_key(key, None)),
            (
                &salted[..],
                crate::MutableItem::target_from_key(key, Some(b"abc")),
            ),
        ] {
            let RequestTypeSpecific::Put(put) = request(wire).request_type else {
                unreachable!();
            };
            assert_eq!(*put.put_request_type.target(), expected);
            // An optional target supplied by an older sender must not override the derived key.
            let mut legacy = wire.to_vec();
            let end_of_args = legacy.windows(5).position(|w| w == b"e1:q3").unwrap();
            legacy.splice(
                end_of_args..end_of_args,
                b"6:target20:00000000000000000000".iter().copied(),
            );
            assert_eq!(request(&legacy).request_type, RequestTypeSpecific::Put(put));
        }
    }
}
