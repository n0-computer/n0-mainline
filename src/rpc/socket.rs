//! UDP socket layer managing incoming/outgoing requests and responses.
//!
//! Uses [`noq_udp`] for all socket I/O — `sendmsg`/`recvmsg` with optimal
//! platform-specific configuration (ECN, buffer sizes, MTU discovery).

mod inflight_requests;
use crate::common::{ErrorSpecific, Message, MessageType, RequestSpecific, ResponseSpecific};
use inflight_requests::InflightRequests;
use std::io::{ErrorKind, IoSliceMut};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

use super::config::Config;

const VERSION: [u8; 4] = [82, 83, 0, 5]; // "RS" version 05
const MTU: usize = 2048;

pub const DEFAULT_PORT: u16 = 6881;
/// Default request timeout before abandoning an inflight request to a non-responding node.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(2000); // 2 seconds

/// Cleanup interval for expired inflight requests to avoid overhead on every recv
const INFLIGHT_CLEANUP_INTERVAL: Duration = Duration::from_millis(200);

/// A non-blocking IPv4 UDP socket backed by [`noq_udp`].
///
/// Wraps a `std::net::UdpSocket` with `noq_udp::UdpSocketState` so that
/// every send uses `sendmsg` and every receive uses `recvmsg`, picking up
/// platform optimisations (GRO, GSO, ECN, larger buffers) automatically.
///
/// All public methods are non-blocking — callers are expected to poll from
/// an async task (the DHT actor loop).
#[derive(Debug)]
struct UdpIo {
    socket: std::net::UdpSocket,
    state: noq_udp::UdpSocketState,
}

impl UdpIo {
    /// Bind to `addr` and configure the socket via noq-udp.
    fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let socket = std::net::UdpSocket::bind(addr)?;
        // UdpSocketState::new sets non-blocking and configures ECN / buffers / etc.
        let state = noq_udp::UdpSocketState::new(noq_udp::UdpSockRef::from(&socket))?;
        Ok(Self { socket, state })
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Non-blocking send via `sendmsg`.
    fn send_to(&self, buf: &[u8], dest: SocketAddr) -> std::io::Result<()> {
        let transmit = noq_udp::Transmit {
            destination: dest,
            ecn: None,
            contents: buf,
            segment_size: None,
            src_ip: None,
        };
        self.state
            .send(noq_udp::UdpSockRef::from(&self.socket), &transmit)
    }

    /// Non-blocking receive via `recvmsg`. Returns `(len, source_addr)` or
    /// `WouldBlock` when nothing is ready.
    fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        let mut iov = [IoSliceMut::new(buf)];
        let mut meta = [noq_udp::RecvMeta::default()];
        let n = self
            .state
            .recv(noq_udp::UdpSockRef::from(&self.socket), &mut iov, &mut meta)?;
        if n == 0 {
            return Err(std::io::Error::new(ErrorKind::WouldBlock, "no data"));
        }
        Ok((meta[0].len, meta[0].addr))
    }
}

/// A UdpSocket wrapper that formats and correlates DHT requests and responses.
#[derive(Debug)]
pub struct KrpcSocket {
    next_tid: u32,
    io: UdpIo,
    pub(crate) server_mode: bool,
    inflight_requests: InflightRequests,
    last_cleanup: Instant,
    local_addr: SocketAddrV4,
}

impl KrpcSocket {
    pub(crate) fn new(config: &Config) -> Result<Self, std::io::Error> {
        let request_timeout = config.request_timeout;
        let port = config.port;
        let bind_addr = config.bind_address.unwrap_or(Ipv4Addr::UNSPECIFIED);

        let io = if let Some(port) = port {
            UdpIo::bind(SocketAddr::from((bind_addr, port)))?
        } else {
            match UdpIo::bind(SocketAddr::from((bind_addr, DEFAULT_PORT))) {
                Ok(io) => Ok(io),
                Err(_) => UdpIo::bind(SocketAddr::from((bind_addr, 0))),
            }?
        };

        let local_addr = match io.local_addr()? {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unimplemented!("KrpcSocket does not support Ipv6"),
        };

        Ok(Self {
            io,
            next_tid: 0,
            server_mode: config.server_mode,
            inflight_requests: InflightRequests::new(request_timeout),
            last_cleanup: Instant::now(),
            local_addr,
        })
    }

    #[cfg(test)]
    pub(crate) fn server() -> Result<Self, std::io::Error> {
        Self::new(&Config {
            server_mode: true,
            bind_address: Some(Ipv4Addr::LOCALHOST),
            ..Default::default()
        })
    }

    #[cfg(test)]
    pub(crate) fn client() -> Result<Self, std::io::Error> {
        Self::new(&Config {
            bind_address: Some(Ipv4Addr::LOCALHOST),
            ..Default::default()
        })
    }

    // === Getters ===

    /// Returns the address the server is listening to.
    #[inline]
    pub fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }

    // === Public Methods ===

    /// Returns true if this message's transaction_id is still inflight
    pub fn inflight(&self, transaction_id: u32) -> bool {
        self.inflight_requests.contains(transaction_id)
    }

    /// Send a request to the given address and return the transaction_id
    pub fn request(&mut self, address: SocketAddrV4, request: RequestSpecific) -> u32 {
        let message = self.request_message(request);
        trace!(context = "socket_message_sending", message = ?message);

        let tid = message.transaction_id;
        self.inflight_requests.add(tid, address);
        let _ = self.send(address, message).map_err(|e| {
            debug!(?e, "Error sending request message");
        });
        tid
    }

    /// Send a response to the given address.
    pub fn response(
        &mut self,
        address: SocketAddrV4,
        transaction_id: u32,
        response: ResponseSpecific,
    ) {
        let message =
            self.response_message(MessageType::Response(response), address, transaction_id);
        trace!(context = "socket_message_sending", message = ?message);
        let _ = self.send(address, message).map_err(|e| {
            debug!(?e, "Error sending response message");
        });
    }

    /// Send an error to the given address.
    pub fn error(&mut self, address: SocketAddrV4, transaction_id: u32, error: ErrorSpecific) {
        let message = self.response_message(MessageType::Error(error), address, transaction_id);
        let _ = self.send(address, message).map_err(|e| {
            debug!(?e, "Error sending error message");
        });
    }

    /// Receives a single krpc message on the socket (non-blocking).
    /// On success, returns the dht message and the origin.
    pub fn recv_from(&mut self) -> Option<(Message, SocketAddrV4)> {
        let mut buf = [0u8; MTU];

        let now = Instant::now();
        if now.duration_since(self.last_cleanup) > INFLIGHT_CLEANUP_INTERVAL {
            self.last_cleanup = now;
            self.inflight_requests.cleanup();
        }

        match self.io.recv_from(&mut buf) {
            Ok((amt, SocketAddr::V4(from))) => {
                let bytes = &buf[..amt];

                if from.port() == 0 {
                    trace!(
                        context = "socket_validation",
                        message = "Response from port 0"
                    );
                    return None;
                }

                match Message::from_bytes(bytes) {
                    Ok(message) => {
                        let should_return = match message.message_type {
                            MessageType::Request(_) => {
                                trace!(
                                    context = "socket_message_receiving",
                                    ?message,
                                    ?from,
                                    "Received request message"
                                );
                                true
                            }
                            MessageType::Response(_) => {
                                trace!(
                                    context = "socket_message_receiving",
                                    ?message,
                                    ?from,
                                    "Received response message"
                                );
                                self.is_expected_response(&message, &from)
                            }
                            MessageType::Error(_) => {
                                trace!(
                                    context = "socket_message_receiving",
                                    ?message,
                                    ?from,
                                    "Received error message"
                                );
                                self.is_expected_response(&message, &from)
                            }
                        };

                        if should_return {
                            return Some((message, from));
                        }
                    }
                    Err(error) => {
                        trace!(
                            context = "socket_error",
                            ?error,
                            ?from,
                            message = ?String::from_utf8_lossy(bytes),
                            "Received invalid Bencode message."
                        );
                    }
                }
            }
            Ok((_, SocketAddr::V6(_))) => {
                trace!(
                    context = "socket_validation",
                    message = "Received IPv6 packet"
                );
            }
            Err(ref error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => {
                warn!("IO error {error}")
            }
        }

        None
    }

    // === Private Methods ===

    fn is_expected_response(&mut self, message: &Message, from: &SocketAddrV4) -> bool {
        // Find and remove the matching inflight request
        if let Some(_request) = self.inflight_requests.remove(message.transaction_id, from) {
            return true;
        } else {
            trace!(
                context = "socket_validation",
                message = "Unexpected response id or wrong address"
            );
        }
        false
    }

    /// Increments self.next_tid and returns the previous value.
    fn tid(&mut self) -> u32 {
        let tid = self.next_tid;
        self.next_tid = self.next_tid.wrapping_add(1);
        tid
    }

    /// Set transactin_id, version and read_only
    fn request_message(&mut self, message: RequestSpecific) -> Message {
        let transaction_id = self.tid();

        Message {
            transaction_id,
            message_type: MessageType::Request(message),
            version: Some(VERSION),
            read_only: !self.server_mode,
            requester_ip: None,
        }
    }

    /// Same as request_message but with request transaction_id and the requester_ip.
    fn response_message(
        &mut self,
        message: MessageType,
        requester_ip: SocketAddrV4,
        request_tid: u32,
    ) -> Message {
        Message {
            transaction_id: request_tid,
            message_type: message,
            version: Some(VERSION),
            read_only: !self.server_mode,
            // BEP_0042 Only relevant in responses.
            requester_ip: Some(requester_ip),
        }
    }

    /// Send a raw dht message (non-blocking via noq-udp sendmsg)
    fn send(&mut self, address: SocketAddrV4, message: Message) -> Result<(), SendMessageError> {
        let bytes = message.to_bytes()?;
        self.io.send_to(&bytes, SocketAddr::from(address))?;
        trace!(context = "socket_message_sending", message = ?message);
        Ok(())
    }
}

#[derive(Debug)]
/// Mainline crate error enum.
pub enum SendMessageError {
    /// Errors related to parsing DHT messages.
    BencodeError(serde_bencode::Error),

    /// Transparent [std::io::Error]
    IO(std::io::Error),
}

impl std::fmt::Display for SendMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendMessageError::BencodeError(e) => write!(f, "Failed to parse packet bytes: {e}"),
            SendMessageError::IO(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for SendMessageError {}

impl From<serde_bencode::Error> for SendMessageError {
    fn from(e: serde_bencode::Error) -> Self {
        SendMessageError::BencodeError(e)
    }
}

impl From<std::io::Error> for SendMessageError {
    fn from(e: std::io::Error) -> Self {
        SendMessageError::IO(e)
    }
}

#[cfg(test)]
mod test {
    use crate::common::{Id, PingResponseArguments, RequestTypeSpecific};
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn tid() {
        let mut socket = KrpcSocket::server().unwrap();

        assert_eq!(socket.tid(), 0);
        assert_eq!(socket.tid(), 1);
        assert_eq!(socket.tid(), 2);

        socket.next_tid = u32::MAX;

        assert_eq!(socket.tid(), 4294967295);
        assert_eq!(socket.tid(), 0);
    }

    #[tokio::test]
    async fn recv_request() {
        let mut server = KrpcSocket::server().unwrap();
        let server_address = server.local_addr();

        let mut client = KrpcSocket::client().unwrap();
        client.next_tid = 120;

        let client_address = client.local_addr();
        let request = RequestSpecific {
            requester_id: Id::random(),
            request_type: RequestTypeSpecific::Ping,
        };

        let expected_request = request.clone();

        client.request(server_address, request);

        // Poll for the message
        let (message, from) = loop {
            if let Some(result) = server.recv_from() {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        assert_eq!(from.port(), client_address.port());
        assert_eq!(message.transaction_id, 120);
        assert!(message.read_only, "Read-only should be true");
        assert_eq!(message.version, Some(VERSION), "Version should be 'RS'");
        assert_eq!(message.message_type, MessageType::Request(expected_request));
    }

    #[tokio::test]
    async fn recv_response() {
        let (tx, rx) = oneshot::channel();

        let mut client = KrpcSocket::client().unwrap();
        let client_address = client.local_addr();

        let responder_id = Id::random();
        let response = ResponseSpecific::Ping(PingResponseArguments { responder_id });

        let server_task = tokio::spawn(async move {
            let mut server = KrpcSocket::client().unwrap();
            let server_address = server.local_addr();
            tx.send(server_address).unwrap();

            loop {
                server.inflight_requests.add(8, client_address);

                if let Some((message, from)) = server.recv_from() {
                    assert_eq!(from.port(), client_address.port());
                    assert_eq!(message.transaction_id, 8);
                    assert!(message.read_only, "Read-only should be true");
                    assert_eq!(message.version, Some(VERSION), "Version should be 'RS'");
                    assert_eq!(
                        message.message_type,
                        MessageType::Response(ResponseSpecific::Ping(PingResponseArguments {
                            responder_id,
                        }))
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        let server_address = rx.await.unwrap();

        client.response(server_address, 8, response);

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn ignore_response_from_wrong_address() {
        let mut server = KrpcSocket::client().unwrap();
        let server_address = server.local_addr();

        let mut client = KrpcSocket::client().unwrap();

        let client_address = client.local_addr();

        server.inflight_requests.add(
            8,
            SocketAddrV4::new([127, 0, 0, 1].into(), client_address.port() + 1),
        );

        let response = ResponseSpecific::Ping(PingResponseArguments {
            responder_id: Id::random(),
        });

        client.response(server_address, 8, response);

        // Give a moment for the packet to arrive
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(
            server.recv_from().is_none(),
            "Should not receive a response from wrong address"
        );
    }
}
