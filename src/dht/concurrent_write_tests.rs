//! Regression tests for overlapping writes and token-cache reuse.
use super::*;
use crate::{actor::socket::KrpcSocket, common::*};
use std::time::Duration;

#[tokio::test]
async fn find_node_then_announce_fetches_tokens() {
    let net = Testnet::new(3).await.unwrap();
    let client = Dht::builder()
        .port(0)
        .bootstrap(&net.bootstrap)
        .build()
        .unwrap();
    let target = Id::from([11; 20]);
    assert!(!client.find_node(target).await.unwrap().is_empty());
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        client.announce_peer(target, Some(1234)),
    )
    .await
    .unwrap();
    assert!(
        result.is_ok(),
        "announce after find_node failed: {result:?}"
    );
}

#[tokio::test]
async fn overlapping_puts_do_not_erase_cas_failure() {
    let mut server = KrpcSocket::new(&Config {
        bootstrap: vec![],
        port: Some(0),
        mode: Mode::Server,
        ..Default::default()
    })
    .unwrap();
    let address = format!("127.0.0.1:{}", server.local_addr().port());
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        let id = Id::from([4; 20]);
        let mut seen = Some(seen_tx);
        let mut release = Some(release_rx);
        loop {
            for (message, from) in server.recv_batch().await {
                let MessageType::Request(request) = message.message_type else {
                    continue;
                };
                match request.request_type {
                    RequestTypeSpecific::Put(PutRequest {
                        put_request_type: PutRequestSpecific::PutMutable(args),
                        ..
                    }) if args.cas.is_some() => server.error(
                        from,
                        message.transaction_id,
                        ErrorSpecific {
                            code: 301,
                            description: "CAS mismatch".into(),
                        },
                    ),
                    RequestTypeSpecific::Put(_) | RequestTypeSpecific::Ping => server.response(
                        from,
                        message.transaction_id,
                        ResponseSpecific::Ping(PingResponseArguments { responder_id: id }),
                    ),
                    RequestTypeSpecific::FindNode(_) => server.response(
                        from,
                        message.transaction_id,
                        ResponseSpecific::FindNode(FindNodeResponseArguments {
                            responder_id: id,
                            nodes: Box::new([]),
                        }),
                    ),
                    RequestTypeSpecific::GetValue(_) => {
                        if let Some(seen) = seen.take() {
                            let _ = seen.send(());
                            release.take().unwrap().await.unwrap();
                        }
                        server.response(
                            from,
                            message.transaction_id,
                            ResponseSpecific::NoValues(NoValuesResponseArguments {
                                responder_id: id,
                                token: Box::new([1]),
                                nodes: None,
                            }),
                        );
                    }
                    _ => {}
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
    let item = MutableItem::new(&SigningKey::from_bytes(&[7; 32]), b"value", 2, None);
    let conditional = dht.put_mutable(item.clone(), Some(99));
    tokio::pin!(conditional);
    assert!(futures::poll!(&mut conditional).is_pending());
    tokio::time::timeout(Duration::from_secs(2), seen_rx)
        .await
        .unwrap()
        .unwrap();
    let unconditional = dht.put_mutable(item, None);
    tokio::pin!(unconditional);
    assert!(futures::poll!(&mut unconditional).is_pending());
    dht.info().await.unwrap();
    release_tx.send(()).unwrap();
    let (conditional, unconditional) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(conditional, unconditional)
    })
    .await
    .unwrap();
    server_task.abort();
    assert!(unconditional.is_ok());
    assert!(
        matches!(
            conditional,
            Err(PutMutableError::Concurrency(ConcurrencyError::CasFailed))
        ),
        "conditional write borrowed the unconditional success: {conditional:?}"
    );
}
