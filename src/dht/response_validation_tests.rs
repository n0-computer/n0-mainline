//! Regression test for filtering remote mutable values by sequence number.
use super::*;
use crate::{actor::socket::KrpcSocket, common::*};
use futures::StreamExt;
use std::time::Duration;

fn responder(response: ResponseSpecific) -> (Dht, tokio::task::JoinHandle<()>) {
    let mut server = KrpcSocket::new(&Config {
        bootstrap: vec![],
        port: Some(0),
        mode: Mode::Server,
        ..Default::default()
    })
    .unwrap();
    let address = format!("127.0.0.1:{}", server.local_addr().port());
    let task = tokio::spawn(async move {
        loop {
            for (message, from) in server.recv_batch().await {
                let MessageType::Request(request) = message.message_type else {
                    continue;
                };
                let response = match request.request_type {
                    RequestTypeSpecific::GetValue(_) => response.clone(),
                    _ => ResponseSpecific::FindNode(FindNodeResponseArguments {
                        responder_id: Id::from([4; 20]),
                        nodes: Box::new([]),
                    }),
                };
                server.response(from, message.transaction_id, response);
            }
            server.flush().await;
        }
    });
    let dht = Dht::builder()
        .port(0)
        .bootstrap(&[address])
        .build()
        .unwrap();
    (dht, task)
}

#[tokio::test]
async fn remote_mutable_sequence_filter() {
    let item = MutableItem::new(&SigningKey::from_bytes(&[7; 32]), b"old value", 1, None);
    let (dht, task) = responder(ResponseSpecific::GetMutable(GetMutableResponseArguments {
        responder_id: Id::from([4; 20]),
        token: Box::new([1]),
        nodes: None,
        v: item.value().into(),
        k: *item.key(),
        seq: item.seq(),
        sig: *item.signature(),
    }));
    for (seq, expected) in [
        (None, true),
        (Some(0), true),
        (Some(1), false),
        (Some(2), false),
    ] {
        let mut stream = dht.get_mutable(item.key(), None, seq).await.unwrap();
        let values = tokio::time::timeout(Duration::from_secs(3), async {
            let mut values = Vec::new();
            while let Some(value) = stream.next().await {
                values.push(value);
            }
            values
        })
        .await
        .unwrap();
        assert_eq!(
            values,
            if expected { vec![item.clone()] } else { vec![] },
            "filter {seq:?}"
        );
    }
    // An authentic older value still proves the responder is usable.
    assert_eq!(
        dht.get_closest_nodes(*item.target()).await.unwrap().len(),
        1
    );
    task.abort();
}
