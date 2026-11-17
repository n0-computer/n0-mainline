//! KRPC message structure, and its bencode encoding.

use super::bencode::{self, BencodeError, Dict, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct DHTMessage {
    pub transaction_id: Vec<u8>,
    pub version: Option<[u8; 4]>,
    pub variant: DHTMessageVariant,
    // Ipv6 is not supported anyways.
    pub ip: Option<[u8; 6]>,
    pub read_only: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DHTMessageVariant {
    Request(DHTRequestSpecific),
    Response(DHTResponseSpecific),
    Error(DHTErrorSpecific),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DHTRequestSpecific {
    Ping {
        arguments: DHTPingRequestArguments,
    },
    FindNode {
        arguments: DHTFindNodeRequestArguments,
    },
    GetPeers {
        arguments: DHTGetPeersRequestArguments,
    },
    GetSignedPeers {
        arguments: DHTGetPeersRequestArguments,
    },
    AnnouncePeer {
        arguments: DHTAnnouncePeerRequestArguments,
    },
    AnnounceSignedPeer {
        arguments: DHTAnnounceSignedPeerRequestArguments,
    },
    GetValue {
        arguments: DHTGetValueRequestArguments,
    },
    PutValue {
        arguments: DHTPutValueRequestArguments,
    },
}

#[derive(Debug, Clone, PartialEq)]
/// A KRPC response, identified by the keys it carries.
///
/// Responses have no type tag: [`DHTResponseSpecific::from_dict`] tries the variants in
/// declaration order, so they go from most to least detailed.
pub enum DHTResponseSpecific {
    GetMutable {
        arguments: DHTGetMutableResponseArguments,
    },

    NoMoreRecentValue {
        arguments: DHTNoMoreRecentValueResponseArguments,
    },

    GetImmutable {
        arguments: DHTGetImmutableResponseArguments,
    },

    GetPeers {
        arguments: DHTGetPeersResponseArguments,
    },

    GetSignedPeers {
        arguments: DHTGetSignedPeersResponseArguments,
    },

    NoValues {
        arguments: DHTNoValuesResponseArguments,
    },

    FindNode {
        arguments: DHTFindNodeResponseArguments,
    },

    Ping {
        arguments: DHTPingResponseArguments,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTErrorSpecific {
    pub error_info: (i32, String),
}

// === PING ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTPingRequestArguments {
    pub id: [u8; 20],
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTPingResponseArguments {
    pub id: [u8; 20],
}

// === FIND NODE ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTFindNodeRequestArguments {
    pub id: [u8; 20],
    pub target: [u8; 20],
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTFindNodeResponseArguments {
    pub id: [u8; 20],
    pub nodes: Box<[u8]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTNoValuesResponseArguments {
    pub id: [u8; 20],
    pub token: Box<[u8]>,
    pub nodes: Option<Box<[u8]>>,
}

// === Get Peers ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTGetPeersRequestArguments {
    pub id: [u8; 20],
    pub info_hash: [u8; 20],
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTGetPeersResponseArguments {
    pub id: [u8; 20],
    pub token: Box<[u8]>,
    pub nodes: Option<Box<[u8]>>,

    // values are not optional, because if they are missing this missing
    // we can just treat this as DHTNoValuesResponseArguments
    pub values: Vec<Box<[u8]>>,
}

// === Get Signed Peers ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTGetSignedPeersResponseArguments {
    pub id: [u8; 20],
    pub token: Box<[u8]>,
    pub nodes: Option<Box<[u8]>>,

    // peers are not optional, because if they are missing this missing
    // we can just treat this as DHTNoValuesResponseArguments
    pub peers: Vec<Box<[u8]>>,
}

// === Announce Peer ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTAnnouncePeerRequestArguments {
    pub id: [u8; 20],
    pub info_hash: [u8; 20],

    pub port: u16,
    pub token: Box<[u8]>,
    pub implied_port: Option<u8>,
}

// === Announce Signed Peer ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTAnnounceSignedPeerRequestArguments {
    pub id: [u8; 20],
    pub info_hash: [u8; 20],
    pub token: Box<[u8]>,
    pub k: [u8; 32],
    pub sig: [u8; 64],

    pub t: i64,
}

// === Get Value ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTGetValueRequestArguments {
    pub id: [u8; 20],
    pub target: [u8; 20],
    pub seq: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTGetImmutableResponseArguments {
    pub id: [u8; 20],
    pub token: Box<[u8]>,
    pub nodes: Option<Box<[u8]>>,
    pub v: Box<[u8]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTNoMoreRecentValueResponseArguments {
    pub id: [u8; 20],
    pub token: Box<[u8]>,
    pub nodes: Option<Box<[u8]>>,

    pub seq: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DHTGetMutableResponseArguments {
    pub id: [u8; 20],
    pub token: Box<[u8]>,
    pub nodes: Option<Box<[u8]>>,
    pub v: Box<[u8]>,
    pub k: [u8; 32],
    pub sig: [u8; 64],

    pub seq: i64,
}

// === Put Value ===

#[derive(Debug, Clone, PartialEq)]
pub struct DHTPutValueRequestArguments {
    pub id: [u8; 20],
    pub target: [u8; 20],
    pub token: Box<[u8]>,
    pub v: Box<[u8]>,
    pub k: Option<[u8; 32]>,
    pub sig: Option<[u8; 64]>,
    pub seq: Option<i64>,
    pub cas: Option<i64>,
    pub salt: Option<Box<[u8]>>,
}

// === Decoding ===

impl DHTMessage {
    pub fn from_bytes(bytes: &[u8]) -> Result<DHTMessage, BencodeError> {
        let Value::Dict(msg) = bencode::decode(bytes)? else {
            return Err(BencodeError::InvalidValue("message"));
        };
        let variant = match msg.bytes("y")? {
            b"q" => DHTMessageVariant::Request(DHTRequestSpecific::from_dict(&msg)?),
            b"r" => DHTMessageVariant::Response(DHTResponseSpecific::from_dict(msg.dict("r")?)?),
            b"e" => DHTMessageVariant::Error(DHTErrorSpecific::from_dict(&msg)?),
            _ => return Err(BencodeError::UnknownMessageType),
        };
        Ok(DHTMessage {
            transaction_id: msg.bytes("t")?.to_vec(),
            version: msg.opt_array("v")?,
            variant,
            ip: msg.opt_array("ip")?,
            read_only: msg.opt_int("ro")?,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut msg = Dict::default();
        msg.insert(b"t", Value::Bytes(&self.transaction_id));
        msg.insert_opt(b"v", self.version.as_ref().map(|v| Value::Bytes(v)));
        msg.insert_opt(b"ip", self.ip.as_ref().map(|ip| Value::Bytes(ip)));
        msg.insert_opt(b"ro", self.read_only.map(|ro| Value::Int(ro.into())));
        match &self.variant {
            DHTMessageVariant::Request(req) => {
                msg.insert(b"y", Value::Bytes(b"q"));
                let (name, arguments) = req.to_dict();
                msg.insert(b"q", Value::Bytes(name));
                msg.insert(b"a", Value::Dict(arguments));
            }
            DHTMessageVariant::Response(res) => {
                msg.insert(b"y", Value::Bytes(b"r"));
                msg.insert(b"r", Value::Dict(res.to_dict()));
            }
            DHTMessageVariant::Error(err) => {
                msg.insert(b"y", Value::Bytes(b"e"));
                let (code, description) = &err.error_info;
                let info = vec![
                    Value::Int((*code).into()),
                    Value::Bytes(description.as_bytes()),
                ];
                msg.insert(b"e", Value::List(info));
            }
        }
        let mut out = Vec::new();
        bencode::encode(&Value::Dict(msg), &mut out);
        out
    }
}

impl DHTRequestSpecific {
    fn from_dict(msg: &Dict<'_>) -> Result<Self, BencodeError> {
        let a = msg.dict("a")?;
        Ok(match msg.bytes("q")? {
            b"ping" => Self::Ping {
                arguments: DHTPingRequestArguments { id: a.array("id")? },
            },
            b"find_node" => Self::FindNode {
                arguments: DHTFindNodeRequestArguments {
                    id: a.array("id")?,
                    target: a.array("target")?,
                },
            },
            b"get_peers" => Self::GetPeers {
                arguments: DHTGetPeersRequestArguments::from_dict(a)?,
            },
            b"get_signed_peers" => Self::GetSignedPeers {
                arguments: DHTGetPeersRequestArguments::from_dict(a)?,
            },
            b"announce_peer" => Self::AnnouncePeer {
                arguments: DHTAnnouncePeerRequestArguments {
                    id: a.array("id")?,
                    info_hash: a.array("info_hash")?,
                    port: a.int("port")?,
                    token: a.bytes("token")?.into(),
                    implied_port: a.opt_int("implied_port")?,
                },
            },
            b"announce_signed_peer" => Self::AnnounceSignedPeer {
                arguments: DHTAnnounceSignedPeerRequestArguments {
                    id: a.array("id")?,
                    info_hash: a.array("info_hash")?,
                    token: a.bytes("token")?.into(),
                    k: a.array("k")?,
                    sig: a.array("sig")?,
                    t: a.int("t")?,
                },
            },
            b"get" => Self::GetValue {
                arguments: DHTGetValueRequestArguments {
                    id: a.array("id")?,
                    target: a.array("target")?,
                    seq: a.opt_int("seq")?,
                },
            },
            b"put" => Self::PutValue {
                arguments: DHTPutValueRequestArguments {
                    id: a.array("id")?,
                    target: a.array("target")?,
                    token: a.bytes("token")?.into(),
                    v: a.bytes("v")?.into(),
                    k: a.opt_array("k")?,
                    sig: a.opt_array("sig")?,
                    seq: a.opt_int("seq")?,
                    cas: a.opt_int("cas")?,
                    salt: a.opt_bytes("salt")?.map(Into::into),
                },
            },
            _ => return Err(BencodeError::UnknownMessageType),
        })
    }

    fn to_dict(&self) -> (&'static [u8], Dict<'_>) {
        let mut a = Dict::default();
        let name: &'static [u8] = match self {
            Self::Ping { arguments } => {
                a.insert(b"id", Value::Bytes(&arguments.id));
                b"ping"
            }
            Self::FindNode { arguments } => {
                a.insert(b"id", Value::Bytes(&arguments.id));
                a.insert(b"target", Value::Bytes(&arguments.target));
                b"find_node"
            }
            Self::GetPeers { arguments } => {
                arguments.to_dict(&mut a);
                b"get_peers"
            }
            Self::GetSignedPeers { arguments } => {
                arguments.to_dict(&mut a);
                b"get_signed_peers"
            }
            Self::AnnouncePeer { arguments } => {
                a.insert(b"id", Value::Bytes(&arguments.id));
                a.insert(b"info_hash", Value::Bytes(&arguments.info_hash));
                a.insert(b"port", Value::Int(arguments.port.into()));
                a.insert(b"token", Value::Bytes(&arguments.token));
                a.insert_opt(
                    b"implied_port",
                    arguments.implied_port.map(|p| Value::Int(p.into())),
                );
                b"announce_peer"
            }
            Self::AnnounceSignedPeer { arguments } => {
                a.insert(b"id", Value::Bytes(&arguments.id));
                a.insert(b"info_hash", Value::Bytes(&arguments.info_hash));
                a.insert(b"token", Value::Bytes(&arguments.token));
                a.insert(b"k", Value::Bytes(&arguments.k));
                a.insert(b"sig", Value::Bytes(&arguments.sig));
                a.insert(b"t", Value::Int(arguments.t));
                b"announce_signed_peer"
            }
            Self::GetValue { arguments } => {
                a.insert(b"id", Value::Bytes(&arguments.id));
                a.insert(b"target", Value::Bytes(&arguments.target));
                a.insert_opt(b"seq", arguments.seq.map(Value::Int));
                b"get"
            }
            Self::PutValue { arguments } => {
                a.insert(b"id", Value::Bytes(&arguments.id));
                a.insert(b"target", Value::Bytes(&arguments.target));
                a.insert(b"token", Value::Bytes(&arguments.token));
                a.insert(b"v", Value::Bytes(&arguments.v));
                a.insert_opt(b"k", arguments.k.as_ref().map(|k| Value::Bytes(k)));
                a.insert_opt(b"sig", arguments.sig.as_ref().map(|s| Value::Bytes(s)));
                a.insert_opt(b"seq", arguments.seq.map(Value::Int));
                a.insert_opt(b"cas", arguments.cas.map(Value::Int));
                a.insert_opt(b"salt", arguments.salt.as_deref().map(Value::Bytes));
                b"put"
            }
        };
        (name, a)
    }
}

impl DHTGetPeersRequestArguments {
    fn from_dict(a: &Dict<'_>) -> Result<Self, BencodeError> {
        Ok(Self {
            id: a.array("id")?,
            info_hash: a.array("info_hash")?,
        })
    }

    fn to_dict<'a>(&'a self, a: &mut Dict<'a>) {
        a.insert(b"id", Value::Bytes(&self.id));
        a.insert(b"info_hash", Value::Bytes(&self.info_hash));
    }
}

/// Fields shared by the responses to `get_peers`, `get_signed_peers` and `get`.
struct TokenFields {
    id: [u8; 20],
    token: Box<[u8]>,
    nodes: Option<Box<[u8]>>,
}

impl TokenFields {
    fn from_dict(r: &Dict<'_>) -> Result<Self, BencodeError> {
        Ok(Self {
            id: r.array("id")?,
            token: r.bytes("token")?.into(),
            nodes: r.opt_bytes("nodes")?.map(Into::into),
        })
    }
}

fn insert_token_fields<'a>(
    r: &mut Dict<'a>,
    id: &'a [u8; 20],
    token: &'a [u8],
    nodes: &'a Option<Box<[u8]>>,
) {
    r.insert(b"id", Value::Bytes(id));
    r.insert(b"token", Value::Bytes(token));
    r.insert_opt(b"nodes", nodes.as_deref().map(Value::Bytes));
}

fn bytes_list(items: &[Box<[u8]>]) -> Value<'_> {
    Value::List(items.iter().map(|b| Value::Bytes(b)).collect())
}

impl DHTResponseSpecific {
    pub(super) fn from_dict(r: &Dict<'_>) -> Result<Self, BencodeError> {
        // Same order as the variants; the first shape whose keys are all present and valid wins.
        let get_mutable = || {
            let f = TokenFields::from_dict(r)?;
            Ok(Self::GetMutable {
                arguments: DHTGetMutableResponseArguments {
                    v: r.bytes("v")?.into(),
                    k: r.array("k")?,
                    sig: r.array("sig")?,
                    seq: r.int("seq")?,
                    id: f.id,
                    token: f.token,
                    nodes: f.nodes,
                },
            })
        };
        let no_more_recent_value = || {
            let f = TokenFields::from_dict(r)?;
            Ok(Self::NoMoreRecentValue {
                arguments: DHTNoMoreRecentValueResponseArguments {
                    seq: r.int("seq")?,
                    id: f.id,
                    token: f.token,
                    nodes: f.nodes,
                },
            })
        };
        let get_immutable = || {
            let f = TokenFields::from_dict(r)?;
            Ok(Self::GetImmutable {
                arguments: DHTGetImmutableResponseArguments {
                    v: r.bytes("v")?.into(),
                    id: f.id,
                    token: f.token,
                    nodes: f.nodes,
                },
            })
        };
        let get_peers = || {
            let f = TokenFields::from_dict(r)?;
            Ok(Self::GetPeers {
                arguments: DHTGetPeersResponseArguments {
                    values: r.bytes_list("values")?,
                    id: f.id,
                    token: f.token,
                    nodes: f.nodes,
                },
            })
        };
        let get_signed_peers = || {
            let f = TokenFields::from_dict(r)?;
            Ok(Self::GetSignedPeers {
                arguments: DHTGetSignedPeersResponseArguments {
                    peers: r.bytes_list("peers")?,
                    id: f.id,
                    token: f.token,
                    nodes: f.nodes,
                },
            })
        };
        let no_values = || {
            let f = TokenFields::from_dict(r)?;
            Ok(Self::NoValues {
                arguments: DHTNoValuesResponseArguments {
                    id: f.id,
                    token: f.token,
                    nodes: f.nodes,
                },
            })
        };
        let find_node = || {
            Ok(Self::FindNode {
                arguments: DHTFindNodeResponseArguments {
                    id: r.array("id")?,
                    nodes: r.bytes("nodes")?.into(),
                },
            })
        };
        let ping = || {
            Ok(Self::Ping {
                arguments: DHTPingResponseArguments { id: r.array("id")? },
            })
        };
        let shapes: [&dyn Fn() -> Result<Self, BencodeError>; 8] = [
            &get_mutable,
            &no_more_recent_value,
            &get_immutable,
            &get_peers,
            &get_signed_peers,
            &no_values,
            &find_node,
            &ping,
        ];
        shapes
            .iter()
            .find_map(|shape| shape().ok())
            .ok_or(BencodeError::UnknownResponse)
    }

    fn to_dict(&self) -> Dict<'_> {
        let mut r = Dict::default();
        match self {
            Self::GetMutable { arguments: a } => {
                insert_token_fields(&mut r, &a.id, &a.token, &a.nodes);
                r.insert(b"v", Value::Bytes(&a.v));
                r.insert(b"k", Value::Bytes(&a.k));
                r.insert(b"sig", Value::Bytes(&a.sig));
                r.insert(b"seq", Value::Int(a.seq));
            }
            Self::NoMoreRecentValue { arguments: a } => {
                insert_token_fields(&mut r, &a.id, &a.token, &a.nodes);
                r.insert(b"seq", Value::Int(a.seq));
            }
            Self::GetImmutable { arguments: a } => {
                insert_token_fields(&mut r, &a.id, &a.token, &a.nodes);
                r.insert(b"v", Value::Bytes(&a.v));
            }
            Self::GetPeers { arguments: a } => {
                insert_token_fields(&mut r, &a.id, &a.token, &a.nodes);
                r.insert(b"values", bytes_list(&a.values));
            }
            Self::GetSignedPeers { arguments: a } => {
                insert_token_fields(&mut r, &a.id, &a.token, &a.nodes);
                r.insert(b"peers", bytes_list(&a.peers));
            }
            Self::NoValues { arguments: a } => {
                insert_token_fields(&mut r, &a.id, &a.token, &a.nodes);
            }
            Self::FindNode { arguments: a } => {
                r.insert(b"id", Value::Bytes(&a.id));
                r.insert(b"nodes", Value::Bytes(&a.nodes));
            }
            Self::Ping { arguments: a } => {
                r.insert(b"id", Value::Bytes(&a.id));
            }
        }
        r
    }
}

impl DHTErrorSpecific {
    fn from_dict(msg: &Dict<'_>) -> Result<Self, BencodeError> {
        let invalid = BencodeError::InvalidValue("e");
        let [Value::Int(code), Value::Bytes(description)] = msg.list("e")? else {
            return Err(invalid);
        };
        let code = i32::try_from(*code).map_err(|_| invalid.clone())?;
        let description = String::from_utf8(description.to_vec()).map_err(|_| invalid)?;
        Ok(Self {
            error_info: (code, description),
        })
    }
}
