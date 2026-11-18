//! Regression tests at the UDP boundary.

use super::*;
use crate::{actor::socket::KrpcSocket, common::*};
use std::time::Duration;

#[tokio::test]
async fn remote_put_errors_are_returned_without_panicking() {
    for code in [203, 301, 302] {
        let mut server = KrpcSocket::new(&Config {
            bootstrap: vec![],
            port: Some(0),
            mode: Mode::Server,
            ..Default::default()
        })
        .unwrap();
        let address = format!("127.0.0.1:{}", server.local_addr().port());
        let server_task = tokio::spawn(async move {
            let id = Id::from([4; 20]);
            loop {
                for (message, from) in server.recv_batch().await {
                    let MessageType::Request(request) = message.message_type else {
                        continue;
                    };
                    match request.request_type {
                        RequestTypeSpecific::Put(_) => server.error(
                            from,
                            message.transaction_id,
                            ErrorSpecific {
                                code,
                                description: "remote rejection".into(),
                            },
                        ),
                        RequestTypeSpecific::FindNode(_) => server.response(
                            from,
                            message.transaction_id,
                            ResponseSpecific::FindNode(FindNodeResponseArguments {
                                responder_id: id,
                                nodes: Box::new([]),
                            }),
                        ),
                        _ => server.response(
                            from,
                            message.transaction_id,
                            ResponseSpecific::NoValues(NoValuesResponseArguments {
                                responder_id: id,
                                token: Box::new([1]),
                                nodes: None,
                            }),
                        ),
                    }
                }
                server.flush().await;
            }
        });
        let dht = Dht::builder()
            .port(0)
            .bootstrap(&[address])
            .build()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            for error in [
                dht.put_immutable(b"value").await.unwrap_err(),
                dht.announce_peer(Id::from([9; 20]), Some(6881))
                    .await
                    .unwrap_err(),
            ] {
                assert!(matches!(error, PutQueryError::ErrorResponse(ref error)
                    if error.code == code && error.description == "remote rejection"));
            }
            let item = MutableItem::new(&SigningKey::from_bytes(&[7; 32]), b"value", 1, None);
            let error = dht.put_mutable(item, None).await.unwrap_err();
            assert!(matches!(
                (code, error),
                (
                    301,
                    PutMutableError::Concurrency(ConcurrencyError::CasFailed)
                ) | (
                    302,
                    PutMutableError::Concurrency(ConcurrencyError::NotMostRecent)
                ) | (203, PutMutableError::Query(PutQueryError::ErrorResponse(_)))
            ));
        })
        .await
        .unwrap();
        server_task.abort();
    }
}

#[tokio::test]
async fn server_echoes_opaque_transaction_ids_in_responses_and_errors() {
    let dht = Dht::builder()
        .port(0)
        .no_bootstrap()
        .mode(Mode::Server)
        .build()
        .unwrap();
    let address = (
        Ipv4Addr::LOCALHOST,
        dht.info().await.unwrap().local_addr().port(),
    );
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for tid in [
        &b""[..],
        b"a",
        b"aa",
        b"\0aa",
        b"\0\0aa",
        b"arbitrary length",
    ] {
        for reject in [false, true] {
            let prefix: &[u8] = if reject {
                // Standard BEP 44 put without a target; the invalid token must produce an error.
                b"d1:ad2:id20:abcdefghij01234567895:token1:x1:v5:helloe1:q3:put1:t"
            } else {
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t"
            };
            let mut request = prefix.to_vec();
            request.extend_from_slice(format!("{}:", tid.len()).as_bytes());
            request.extend_from_slice(tid);
            request.extend_from_slice(b"1:y1:qe");
            peer.send_to(&request, address).await.unwrap();
            let mut buf = [0; 2048];
            let (len, _) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
            let response = Message::from_bytes(&buf[..len]).unwrap();
            assert_eq!(&*response.transaction_id, tid);
            if reject {
                assert!(matches!(
                    response.message_type,
                    MessageType::Error(ErrorSpecific { code: 203, .. })
                ));
            } else {
                assert!(matches!(
                    response.message_type,
                    MessageType::Response(ResponseSpecific::Ping(_))
                ));
            }
        }
    }
}
