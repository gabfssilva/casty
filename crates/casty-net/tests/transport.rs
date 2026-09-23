//! What the transport promises, over real sockets on the loopback interface.
//!
//! These are the guarantees `tests/test_transport.py` checks of the implementation being replaced, with the ones it
//! could not reach through the public protocol added: a peer that breaks the wire, and a frame past the limit.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use casty_core::schema::msgpack::Reader;
use casty_net::compress::Name;
use casty_net::endpoint::{Config, Endpoint, Received};
use casty_net::frame::{Frame, VERSION};
use casty_net::limits::Limits;
use casty_net::pool::{Lost, Target, Traffic};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WITHIN: Duration = Duration::from_secs(20);

fn config(bind: Option<&str>) -> Config {
    Config {
        bind: bind.map(str::to_owned),
        min_compressed: 4096,
        ..Config::default()
    }
}

async fn node() -> Endpoint {
    Endpoint::start(config(Some("127.0.0.1:0")))
        .await
        .expect("a free port")
}

/// Receive `count` envelopes, or say what was missing.
async fn take(endpoint: &mut Endpoint, count: usize) -> Vec<Received> {
    let mut received = Vec::with_capacity(count);
    let taken = tokio::time::timeout(WITHIN, async {
        while received.len() < count {
            match endpoint.recv().await {
                Some(Ok(envelope)) => received.push(envelope),
                Some(Err(reason)) => panic!("refused: {reason}"),
                None => break,
            }
        }
    })
    .await;
    assert!(taken.is_ok(), "only {} of {count} arrived", received.len());
    received
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn it_delivers_everything_intact_and_in_order_per_pair_and_name() {
    let mut nodes = Vec::new();
    for _ in 0..3 {
        nodes.push(node().await);
    }
    let addresses: Vec<Target> = nodes
        .iter()
        .map(|node| Target::Node(node.node().clone()))
        .collect();
    let senders: Vec<_> = nodes.iter().map(Endpoint::sender).collect();
    let big = b"a line that repeats itself. ".repeat(40_000);
    assert!(big.len() > 1_000_000);

    let mut sending = Vec::new();
    for (from, sender) in senders.into_iter().enumerate() {
        let addresses = addresses.clone();
        let big = big.clone();
        sending.push(tokio::spawn(async move {
            for to in &addresses {
                sender.send(to, "actors", &[]).unwrap();
                sender.send(to, "replication", &big).unwrap();
                for index in 0..1_000_u32 {
                    let name = if index % 2 == 0 { "actors" } else { "replies" };
                    let mut payload = vec![u8::try_from(from).unwrap()];
                    payload.extend_from_slice(&index.to_be_bytes());
                    sender.send(to, name, &payload).unwrap();
                }
            }
        }));
    }
    for task in sending {
        task.await.unwrap();
    }

    // Each node hears from all three: one empty, one big and a thousand small, from each of them.
    for endpoint in &mut nodes {
        let received = take(endpoint, 3 * (2 + 1_000)).await;
        let mut ordered: HashMap<(u8, String), Vec<u32>> = HashMap::new();
        let mut empty = 0;
        let mut large = 0;
        for envelope in received {
            match envelope.payload.len() {
                0 => empty += 1,
                5 => {
                    let index = u32::from_be_bytes(envelope.payload[1..].try_into().unwrap());
                    ordered
                        .entry((envelope.payload[0], envelope.name))
                        .or_default()
                        .push(index);
                }
                _ => {
                    assert_eq!(envelope.payload, big, "a large envelope arrived changed");
                    large += 1;
                }
            }
        }
        assert_eq!((empty, large), (3, 3));
        assert_eq!(ordered.len(), 6, "one sequence per source and name");
        for ((from, name), indexes) in ordered {
            let mut expected: Vec<u32> = (0..1_000).collect();
            expected.retain(|index| (index % 2 == 0) == (name == "actors"));
            assert_eq!(indexes, expected, "out of order from {from} on {name}");
        }
    }
    for endpoint in nodes {
        endpoint.close(true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_never_blocks_the_sender_and_delivers_everything_later() {
    let sender = node().await;
    let mut receiver = node().await;
    let to = Target::Node(receiver.node().clone());
    let payload = vec![7_u8; 512 * 1024];
    let rounds = 100;

    let started = tokio::time::Instant::now();
    for index in 0..rounds {
        let mut one = payload.clone();
        one[..4].copy_from_slice(&u32::try_from(index).unwrap().to_be_bytes());
        sender.send(&to, "actors", &one).unwrap();
    }
    let queued = started.elapsed();

    assert!(
        queued < Duration::from_secs(5),
        "sending waited for the receiver: {queued:?}"
    );
    let arrived = take(&mut receiver, rounds).await;
    for (index, envelope) in arrived.iter().enumerate() {
        assert_eq!(
            u32::from_be_bytes(envelope.payload[..4].try_into().unwrap()),
            u32::try_from(index).unwrap(),
            "out of order"
        );
        assert_eq!(envelope.payload.len(), payload.len());
    }
    sender.close(true).await;
    receiver.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_drops_envelopes_for_the_old_incarnation_and_delivers_to_a_seed() {
    let sender = node().await;
    let first = node().await;
    let address = first
        .node()
        .address
        .clone()
        .expect("a bound node has an address");
    let old = first.node().clone();
    first.close(true).await;
    // The port is free again, and whatever binds it is another node.
    let mut second = Endpoint::start(config(Some(&address)))
        .await
        .expect("the freed port");
    assert_ne!(second.node().incarnation, old.incarnation);

    sender
        .send(&Target::Node(old), "actors", b"for the old one")
        .unwrap();
    sender
        .send(&Target::Seed(address), "actors", b"for whoever answers")
        .unwrap();

    let arrived = take(&mut second, 1).await;
    assert_eq!(arrived[0].payload, b"for whoever answers");
    // Nothing else arrives: the envelope for the old incarnation was dropped, not queued.
    let more = tokio::time::timeout(Duration::from_millis(300), second.recv()).await;
    assert!(
        more.is_err(),
        "an envelope for another incarnation was delivered"
    );
    sender.close(true).await;
    second.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_reports_a_seed_that_belongs_to_another_cluster() {
    let other = Endpoint::start(Config {
        cluster: "another".to_owned(),
        ..config(Some("127.0.0.1:0"))
    })
    .await
    .unwrap();
    let address = other.node().address.clone().unwrap();
    let mut joining = node().await;

    joining
        .send(&Target::Seed(address), "actors", b"hello")
        .unwrap();

    let refused = tokio::time::timeout(WITHIN, joining.recv()).await.unwrap();
    let Some(Err(reason)) = refused else {
        panic!("the seed did not refuse: {refused:?}");
    };
    assert!(reason.contains("another"), "{reason}");
    other.close(true).await;
    joining.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_reports_a_seed_whose_limits_differ() {
    let other = Endpoint::start(Config {
        limits: Limits {
            message: 8 * 1024 * 1024,
            ..Limits::default()
        },
        ..config(Some("127.0.0.1:0"))
    })
    .await
    .unwrap();
    let address = other.node().address.clone().unwrap();
    let mut joining = node().await;

    joining
        .send(&Target::Seed(address), "actors", b"hello")
        .unwrap();

    let refused = tokio::time::timeout(WITHIN, joining.recv()).await.unwrap();
    let Some(Err(reason)) = refused else {
        panic!("the seed did not refuse: {refused:?}");
    };
    assert!(reason.contains("message=8388608"), "{reason}");
    other.close(true).await;
    joining.close(true).await;
}

/// The hello names the cluster, the node, its role, the compressors and the limits, and no payload format: there is
/// only one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hello_names_no_payload_format() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let limits = Limits::default();
    let heard = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut frames = casty_net::frame::Decoder::new(limits.frame);
        let mut mux = casty_net::mux::Mux::new(limits, 1 << 30);
        let mut buffer = vec![0_u8; 8 * 1024];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0, "the connection ended before the hello");
            frames.feed(&buffer[..read]);
            while let Some(frame) = frames.frame().unwrap() {
                let records = mux.receive(frame).unwrap();
                if let Some(hello) = records.into_iter().find(|record| record.name == "hello") {
                    return hello.payload;
                }
            }
        }
    });
    let talker = node().await;
    talker
        .send(&Target::Seed(address), "actors", b"anyone there")
        .unwrap();

    let payload = tokio::time::timeout(WITHIN, heard).await.unwrap().unwrap();
    let mut reader = Reader::new(&payload);
    let mut names = Vec::new();
    for _ in 0..reader.read_map_len().unwrap() {
        names.push(reader.read_str().unwrap().to_owned());
        reader.skip().unwrap();
    }
    names.sort();
    assert_eq!(
        names,
        [
            "address",
            "cluster",
            "compression",
            "frame",
            "incarnation",
            "message",
            "role",
            "versions",
            "window",
        ]
    );
    talker.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_small_envelope_passes_a_transfer_of_state_that_is_still_going() {
    let sender = node().await;
    let mut receiver = node().await;
    let to = Target::Node(receiver.node().clone());
    let big = vec![3_u8; 1024 * 1024];

    for _ in 0..30 {
        sender.send(&to, "replication", &big).unwrap();
    }
    sender.send(&to, "actors", b"let me through").unwrap();

    let mut before = 0;
    let arrived = tokio::time::timeout(WITHIN, async {
        loop {
            let envelope = receiver.recv().await.unwrap().unwrap();
            if envelope.name == "actors" {
                return before;
            }
            before += 1;
        }
    })
    .await
    .expect("the small envelope never arrived");

    assert!(arrived < 15, "it waited for {arrived} of the 30 transfers");
    sender.close(true).await;
    receiver.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_past_the_limit_never_reaches_the_wire() {
    let sender = node().await;
    let mut receiver = node().await;
    let to = Target::Node(receiver.node().clone());
    let limits = Limits::default();

    let refused = sender.send(&to, "actors", &vec![0_u8; limits.message + 1]);
    assert!(refused.is_err(), "a payload past the limit was taken");
    sender
        .send(&to, "actors", &vec![1_u8; limits.message])
        .unwrap();

    let arrived = take(&mut receiver, 1).await;
    assert_eq!(arrived[0].payload.len(), limits.message);
    sender.close(true).await;
    receiver.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_that_breaks_the_wire_loses_its_connection_and_nothing_else() {
    let mut node = node().await;
    let address = node.node().address.clone().unwrap();

    let mut socket = TcpStream::connect(&address).await.unwrap();
    // Garbage where a frame header should be.
    socket.write_all(&[0xff; 64]).await.unwrap();
    let mut answer = Vec::new();
    let read = tokio::time::timeout(WITHIN, socket.read_to_end(&mut answer)).await;
    assert!(
        read.is_ok(),
        "the node kept a connection that broke the protocol"
    );

    // The node still works: it takes an envelope from itself and hands it over.
    node.send(&Target::Node(node.node().clone()), "actors", b"still here")
        .unwrap();
    assert_eq!(take(&mut node, 1).await[0].payload, b"still here");
    node.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_larger_than_the_limit_is_refused_before_it_is_read() {
    let node = node().await;
    let address = node.node().address.clone().unwrap();
    let mut socket = TcpStream::connect(&address).await.unwrap();

    // A header that declares more than the frame limit, and nothing after it.
    let mut header = Vec::new();
    Frame::Data {
        stream: 3,
        payload: Vec::new(),
        compressed: false,
    }
    .write(&mut header);
    header[8..12].copy_from_slice(
        &u32::try_from(Limits::default().frame + 1)
            .unwrap()
            .to_be_bytes(),
    );
    socket.write_all(&header).await.unwrap();

    let mut answer = Vec::new();
    let read = tokio::time::timeout(WITHIN, socket.read_to_end(&mut answer)).await;
    assert!(read.is_ok(), "the node waited for a frame past its limit");
    assert_eq!(VERSION, 1);
    node.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_it_compresses_is_what_crosses_the_wire() {
    let counted = Arc::new(AtomicUsize::new(0));
    let mut receiver = node().await;
    let address = receiver.node().address.clone().unwrap();
    let proxy = proxied(&address, Arc::clone(&counted)).await;
    let sender = Endpoint::start(Config {
        compression: Some(vec![Name::Zstd]),
        address_map: Some(Arc::new(move |_| proxy.clone())),
        ..config(Some("127.0.0.1:0"))
    })
    .await
    .unwrap();
    let payload = b"a line that repeats itself. ".repeat(40_000);

    sender
        .send(&Target::Seed(address), "actors", &payload)
        .unwrap();

    let arrived = take(&mut receiver, 1).await;
    assert_eq!(arrived[0].payload, payload);
    let crossed = counted.load(Ordering::SeqCst);
    assert!(
        crossed < payload.len() / 10,
        "{crossed} bytes crossed for a payload of {}",
        payload.len()
    );
    sender.close(true).await;
    receiver.close(true).await;
}

/// A listener that forwards to `target`, counting the bytes it passes on the way out.
async fn proxied(target: &str, counted: Arc<AtomicUsize>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let target = target.to_owned();
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            let Ok(mut outbound) = TcpStream::connect(&target).await else {
                continue;
            };
            let counted = Arc::clone(&counted);
            tokio::spawn(async move {
                let (mut reading, mut writing) = inbound.split();
                let (mut back, mut forth) = outbound.split();
                let out = async {
                    let mut buffer = vec![0_u8; 64 * 1024];
                    loop {
                        let read = reading.read(&mut buffer).await.unwrap_or(0);
                        if read == 0 || forth.write_all(&buffer[..read]).await.is_err() {
                            return;
                        }
                        counted.fetch_add(read, Ordering::SeqCst);
                    }
                };
                let back = tokio::io::copy(&mut back, &mut writing);
                tokio::select! {
                    () = out => {}
                    _ = back => {}
                }
            });
        }
    });
    address
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_connection_stays_up_and_a_peer_that_stops_answering_loses_it() {
    let short = Limits {
        keepalive_after: Duration::from_millis(100),
        keepalive_timeout: Duration::from_millis(200),
        ..Limits::default()
    };
    let sender = Endpoint::start(Config {
        limits: short,
        ..config(Some("127.0.0.1:0"))
    })
    .await
    .unwrap();
    let mut receiver = Endpoint::start(Config {
        limits: short,
        ..config(Some("127.0.0.1:0"))
    })
    .await
    .unwrap();
    let to = Target::Node(receiver.node().clone());

    sender.send(&to, "actors", b"first").unwrap();
    assert_eq!(take(&mut receiver, 1).await[0].payload, b"first");
    // Several keepalive periods with nothing to carry: the pings keep the connection, they do not end it.
    tokio::time::sleep(Duration::from_millis(900)).await;
    sender.send(&to, "actors", b"second").unwrap();

    assert_eq!(take(&mut receiver, 1).await[0].payload, b"second");
    sender.close(true).await;
    receiver.close(true).await;

    // A peer that shakes hands and then says nothing at all loses the connection.
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = silent.local_addr().unwrap().to_string();
    let listening = address.clone();
    let deaf = tokio::spawn(async move {
        let (mut socket, _) = silent.accept().await.unwrap();
        let mut frames = casty_net::frame::Decoder::new(short.frame);
        let mut mux = casty_net::mux::Mux::new(short, 1 << 30);
        let mut buffer = vec![0_u8; 8 * 1024];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            frames.feed(&buffer[..read]);
            let mut said = None;
            while let Some(frame) = frames.frame().unwrap() {
                for record in mux.receive(frame).unwrap() {
                    said =
                        Some(casty_net::handshake::decode(&record.name, &record.payload).unwrap());
                }
            }
            if let Some(casty_net::handshake::Message::Hello(hello)) = said {
                let ack = casty_net::handshake::Message::Ack(casty_net::handshake::Ack {
                    version: 1,
                    node: casty_core::node::NodeId::fresh(Some(listening.clone())),
                    role: casty_net::handshake::Role::Member,
                    compression: None,
                });
                let _ = hello;
                let (name, payload) = casty_net::handshake::encode(&ack);
                mux.send(casty_net::mux::CONTROL, name, &payload);
                let out = mux.output();
                socket.write_all(&out).await.unwrap();
                break;
            }
        }
        // From here on it answers nothing, not even a ping, and waits for the other side to give up.
        let mut ignored = Vec::new();
        socket.read_to_end(&mut ignored).await.unwrap();
    });

    let talker = Endpoint::start(Config {
        limits: short,
        ..config(Some("127.0.0.1:0"))
    })
    .await
    .unwrap();
    talker
        .send(&Target::Seed(address), "actors", b"anyone there")
        .unwrap();

    let given_up = tokio::time::timeout(Duration::from_secs(5), deaf).await;
    assert!(given_up.is_ok(), "the connection to a silent peer was kept");
    talker.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_reports_the_peer_of_a_connection_that_ended() {
    let (lost, mut heard) = tokio::sync::mpsc::unbounded_channel();
    let reporting: Lost = Arc::new(move |peer: &casty_core::node::NodeId| {
        let _ = lost.send(peer.clone());
    });
    let mut watching = Endpoint::start(Config {
        lost: Some(reporting),
        ..config(Some("127.0.0.1:0"))
    })
    .await
    .unwrap();
    let peer = node().await;
    let gone = peer.node().clone();
    peer.send(&Target::Node(watching.node().clone()), "actors", b"hello")
        .unwrap();
    take(&mut watching, 1).await;

    peer.close(true).await;

    let reported = tokio::time::timeout(WITHIN, heard.recv()).await;
    assert_eq!(
        reported,
        Ok(Some(gone)),
        "the lost connection was not reported"
    );
    watching.close(true).await;
}

/// What `endpoint` counts once `holds` says yes of it, or what it counted last when that never happens.
async fn counted(endpoint: &Endpoint, holds: impl Fn(&Traffic) -> bool) -> Traffic {
    let deadline = tokio::time::Instant::now() + WITHIN;
    loop {
        let traffic = endpoint.meter().traffic();
        if holds(&traffic) || tokio::time::Instant::now() >= deadline {
            return traffic;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_counts_the_connections_it_holds_and_the_bytes_they_carry() {
    let plain = || Config {
        compression: Some(Vec::new()),
        ..config(Some("127.0.0.1:0"))
    };
    let mut receiver = Endpoint::start(plain()).await.unwrap();
    let sender = Endpoint::start(plain()).await.unwrap();
    assert_eq!(sender.meter().traffic(), Traffic::default());
    let payload = vec![7_u8; 100_000];
    let size = payload.len() as u64;

    sender
        .send(&Target::Node(receiver.node().clone()), "actors", &payload)
        .unwrap();
    take(&mut receiver, 1).await;

    // Uncompressed, each side carried the payload and the handshake and frames around it. A write is counted once it
    // returns, which can be after the other side has read it, so both are waited for.
    let out = counted(&sender, |traffic| {
        traffic.connections == 1 && traffic.sent > size
    })
    .await;
    assert!(out.connections == 1 && out.sent > size, "{out:?}");
    let into = counted(&receiver, |traffic| {
        traffic.connections == 1 && traffic.received > size
    })
    .await;
    assert!(into.connections == 1 && into.received > size, "{into:?}");

    sender.close(true).await;
    let gone = counted(&receiver, |traffic| traffic.connections == 0).await;
    assert_eq!(
        gone.connections, 0,
        "a connection that ended is still counted"
    );
    assert!(gone.received >= into.received, "a count went back");
    receiver.close(true).await;
}
