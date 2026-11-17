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
        msg.insert(b"t", bytes(self.transaction_id.to_be_bytes().to_vec()));
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
        let transaction_id = match *msg.bytes("t")? {
            [a, b] => u16::from_be_bytes([a, b]).into(),
            [a, b, c, d] => u32::from_be_bytes([a, b, c, d]),
            _ => return Err(DecodeMessageError::InvalidTransactionIdSize),
        };
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
        RequestTypeSpecific::GetSignedPeers(args) => {
            a.insert(b"info_hash", id(&args.info_hash));
            b"get_signed_peers"
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
                PutRequestSpecific::AnnounceSignedPeer(args) => {
                    a.insert(b"info_hash", id(&args.info_hash));
                    a.insert(b"t", Value::Int(args.t as i64));
                    a.insert(b"k", bytes(&args.k[..]));
                    a.insert(b"sig", bytes(&args.sig[..]));
                    b"announce_signed_peer"
                }
                PutRequestSpecific::PutImmutable(args) => {
                    a.insert(b"target", id(&args.target));
                    a.insert(b"v", bytes(&args.v[..]));
                    b"put"
                }
                PutRequestSpecific::PutMutable(args) => {
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
        b"get_signed_peers" => RequestTypeSpecific::GetSignedPeers(GetPeersRequestArguments {
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
        b"announce_signed_peer" => put(PutRequestSpecific::AnnounceSignedPeer(
            AnnounceSignedPeerRequestArguments {
                info_hash: get_id(a, "info_hash")?,
                t: a.int::<i64>("t")? as u64,
                k: a.array("k")?,
                sig: a.array("sig")?,
            },
        ))?,
        b"put" => {
            let target = get_id(a, "target")?;
            let v = Box::from(a.bytes("v")?);
            // All optional keys are checked, even those an immutable put ignores.
            let k = a.opt_array("k")?;
            let sig = a.opt_array("sig")?;
            let seq = a.opt_int("seq")?;
            let cas = a.opt_int("cas")?;
            let salt = a.opt_bytes("salt")?.map(Box::from);
            put(match k {
                None => {
                    PutRequestSpecific::PutImmutable(PutImmutableRequestArguments { target, v })
                }
                Some(k) => {
                    let (Some(seq), Some(sig)) = (seq, sig) else {
                        return Err(DecodeMessageError::MissingMutablePutSequenceOrSignature);
                    };
                    PutRequestSpecific::PutMutable(PutMutableRequestArguments {
                        target,
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
        ResponseSpecific::GetSignedPeers(args) => {
            insert_token_fields(&mut r, &args.responder_id, &args.token, &args.nodes);
            let peers = args.peers.iter();
            let peers = peers.map(|p| bytes(signed_peer_to_bytes(p).to_vec()));
            r.insert(b"peers", Value::List(peers.collect()));
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

/// Inserts the keys shared by the responses to `get_peers`, `get_signed_peers` and `get`.
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
        if let Ok(peers) = r.bytes_list("peers") {
            return Ok(ResponseSpecific::GetSignedPeers(
                GetSignedPeersResponseArguments {
                    responder_id,
                    token,
                    peers: peers
                        .into_iter()
                        .map(bytes_to_signed_peer)
                        .collect::<Result<_, _>>()?,
                    nodes,
                },
            ));
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
