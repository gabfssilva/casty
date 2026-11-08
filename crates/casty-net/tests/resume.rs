//! A connection that breaks without a goodbye is taken up again, over real sockets on the loopback interface, through
//! a relay that cuts or freezes the connections passing through it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use casty_net::endpoint::{Config, Endpoint};
use casty_net::limits::Limits;
use casty_net::pool::{AddressMap, Dialing, Heard, Peer, Target};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::sync::watch;

const WITHIN: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Open,
    /// Nothing passes either way, and neither side hears that the other closed.
    Frozen,
    /// What was held goes through, late, then the connection closes.
    Released,
    /// Both sockets close at once, as a gateway that drops the connection.
    Cut,
}

/// A relay to one node, as the gateway in front of it: it can cut the connections passing through it now, or freeze
/// them and later deliver what they held. Connections opened after either pass normally.
#[derive(Clone)]
struct Gateway {
    address: String,
    passing: Arc<Mutex<Vec<watch::Sender<Mode>>>>,
    /// Connections accepted from now on are held open and go nowhere: a local relay whose way out never opens.
    deaf: Arc<AtomicBool>,
}

impl Gateway {
    async fn to(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = Self {
            address: listener.local_addr().unwrap().to_string(),
            passing: Arc::new(Mutex::new(Vec::new())),
            deaf: Arc::new(AtomicBool::new(false)),
        };
        let passing = Arc::clone(&gateway.passing);
        let deaf = Arc::clone(&gateway.deaf);
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((inbound, _)) = listener.accept().await {
                if deaf.load(Ordering::SeqCst) {
                    held.push(inbound);
                    continue;
                }
                let Ok(outbound) = TcpStream::connect(&target).await else {
                    continue;
                };
                let (mode, watched) = watch::channel(Mode::Open);
                passing.lock().unwrap().push(mode);
                tokio::spawn(relay(inbound, outbound, watched));
            }
        });
        gateway
    }

    fn set(&self, mode: Mode) {
        for passing in self.passing.lock().unwrap().drain(..) {
            let _ = passing.send(mode);
        }
    }

    fn freeze(&self) -> Frozen {
        let frozen: Vec<_> = self.passing.lock().unwrap().drain(..).collect();
        for passing in &frozen {
            let _ = passing.send(Mode::Frozen);
        }
        Frozen(frozen)
    }
}

/// The connections a gateway froze.
struct Frozen(Vec<watch::Sender<Mode>>);

impl Frozen {
    fn release(self) {
        for passing in self.0 {
            let _ = passing.send(Mode::Released);
        }
    }
}

async fn relay(mut a: TcpStream, mut b: TcpStream, mut mode: watch::Receiver<Mode>) {
    let (mut a_read, mut a_write) = a.split();
    let (mut b_read, mut b_write) = b.split();
    let (mut to_b, mut to_a) = (Vec::new(), Vec::new());
    let (mut a_open, mut b_open) = (true, true);
    let mut a_buffer = vec![0_u8; 64 * 1024];
    let mut b_buffer = vec![0_u8; 64 * 1024];
    loop {
        let now = *mode.borrow_and_update();
        match now {
            Mode::Cut => return,
            Mode::Released => {
                let _ = b_write.write_all(&to_b).await;
                let _ = a_write.write_all(&to_a).await;
                // Long enough for what was late to be read before the sockets close.
                tokio::time::sleep(Duration::from_millis(500)).await;
                return;
            }
            Mode::Open | Mode::Frozen => {}
        }
        let frozen = now == Mode::Frozen;
        tokio::select! {
            read = a_read.read(&mut a_buffer), if a_open => match read {
                Ok(0) | Err(_) if frozen => a_open = false,
                Ok(0) | Err(_) => return,
                Ok(read) if frozen => to_b.extend_from_slice(&a_buffer[..read]),
                Ok(read) => if b_write.write_all(&a_buffer[..read]).await.is_err() { return },
            },
            read = b_read.read(&mut b_buffer), if b_open => match read {
                Ok(0) | Err(_) if frozen => b_open = false,
                Ok(0) | Err(_) => return,
                Ok(read) if frozen => to_a.extend_from_slice(&b_buffer[..read]),
                Ok(read) => if a_write.write_all(&b_buffer[..read]).await.is_err() { return },
            },
            changed = mode.changed() => if changed.is_err() { return },
        }
    }
}

/// A node whose dials to the addresses in `through` go to the gateway in front of each, which tells what it hears of
/// its peers to the receiver it comes with.
async fn node(
    limits: Limits,
    through: Arc<Mutex<HashMap<String, String>>>,
) -> (Endpoint, UnboundedReceiver<Peer>) {
    let (told, heard) = unbounded_channel();
    let reporting: Heard = Arc::new(move |peer: Peer| {
        let _ = told.send(peer);
    });
    let map: AddressMap = Arc::new(move |address: &str| -> Dialing {
        let dialed = through
            .lock()
            .unwrap()
            .get(address)
            .cloned()
            .unwrap_or_else(|| address.to_owned());
        Box::pin(async move { dialed })
    });
    let endpoint = Endpoint::start(Config {
        bind: Some("127.0.0.1:0".to_owned()),
        limits,
        heard: Some(reporting),
        address_map: Some(map),
        ..Config::default()
    })
    .await
    .unwrap();
    (endpoint, heard)
}

/// Two nodes that reach each other only through a gateway in front of each: `a`'s in front of `a`, `b`'s of `b`.
struct Pair {
    a: Endpoint,
    b: Endpoint,
    heard_a: UnboundedReceiver<Peer>,
    heard_b: UnboundedReceiver<Peer>,
    gateway_a: Gateway,
    gateway_b: Gateway,
}

impl Pair {
    async fn start(limits_a: Limits, limits_b: Limits) -> Self {
        let through = Arc::new(Mutex::new(HashMap::new()));
        let (a, heard_a) = node(limits_a, Arc::clone(&through)).await;
        let (b, heard_b) = node(limits_b, Arc::clone(&through)).await;
        let address_a = a.node().address.clone().unwrap();
        let address_b = b.node().address.clone().unwrap();
        let gateway_a = Gateway::to(address_a.clone()).await;
        let gateway_b = Gateway::to(address_b.clone()).await;
        through.lock().unwrap().extend([
            (address_a, gateway_a.address.clone()),
            (address_b, gateway_b.address.clone()),
        ]);
        Self {
            a,
            b,
            heard_a,
            heard_b,
            gateway_a,
            gateway_b,
        }
    }

    /// Both nodes send `count` numbered envelopes of one order to each other, one every millisecond and every tenth one
    /// large enough to cross in many frames, while `meanwhile` breaks what carries them; each must arrive once, in
    /// order, with no peer reported lost.
    async fn exchange(mut self, count: u32, meanwhile: impl Future<Output = ()>) {
        let senders = [
            (self.a.sender(), Target::Node(self.b.node().clone())),
            (self.b.sender(), Target::Node(self.a.node().clone())),
        ];
        let sending = senders.map(|(sender, to)| {
            tokio::spawn(async move {
                for index in 0..count {
                    let size = if index % 10 == 9 { 300_000 } else { 64 };
                    let mut payload = vec![7_u8; size];
                    payload[..4].copy_from_slice(&index.to_be_bytes());
                    sender.send(&to, "actors", 1, None, &payload).unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        });
        meanwhile.await;
        for task in sending {
            task.await.unwrap();
        }
        for endpoint in [&mut self.a, &mut self.b] {
            let mut arrived = Vec::new();
            let taken = tokio::time::timeout(WITHIN, async {
                while arrived.len() < count as usize {
                    let envelope = endpoint.recv().await.unwrap().unwrap();
                    arrived.push(u32::from_be_bytes(
                        envelope.payload[..4].try_into().unwrap(),
                    ));
                }
            })
            .await;
            assert!(taken.is_ok(), "only {} of {count} arrived", arrived.len());
            assert_eq!(
                arrived,
                (0..count).collect::<Vec<_>>(),
                "not each once and in order"
            );
            let more = tokio::time::timeout(Duration::from_millis(300), endpoint.recv()).await;
            assert!(more.is_err(), "an envelope arrived twice: {more:?}");
        }
        for heard in [&mut self.heard_a, &mut self.heard_b] {
            assert!(heard.try_recv().is_err(), "a peer was reported");
        }
        self.a.close(true).await;
        self.b.close(true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_cut_without_a_goodbye_is_taken_up_where_it_stopped() {
    let pair = Pair::start(Limits::default(), Limits::default()).await;
    let gateways = (pair.gateway_a.clone(), pair.gateway_b.clone());
    pair.exchange(600, async move {
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            gateways.0.set(Mode::Cut);
            gateways.1.set(Mode::Cut);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bytes_a_half_open_connection_delivers_late_are_never_received_twice() {
    // Only `b` notices that the frozen connection went quiet, so `a` holds it open until `b` takes the session up on
    // another one, and then reads what was frozen on it.
    let quick = Limits {
        keepalive_after: Duration::from_millis(100),
        keepalive_timeout: Duration::from_millis(200),
        ..Limits::default()
    };
    let pair = Pair::start(Limits::default(), quick).await;
    let gateways = (pair.gateway_a.clone(), pair.gateway_b.clone());
    pair.exchange(600, async move {
        for _ in 0..2 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let frozen = (gateways.0.freeze(), gateways.1.freeze());
            tokio::time::sleep(Duration::from_millis(700)).await;
            frozen.0.release();
            frozen.1.release();
        }
    })
    .await;
}

/// Both connections of a pair are cut at once and both nodes dial again, but the dials of one of them, the one with
/// the smaller incarnation or the other, go into a relay that accepts them and never reaches the peer. The pair must
/// take its session up over the dial that does arrive: nothing sent meanwhile is lost, and neither node hears of a
/// lost or unreached peer.
async fn crossed_with_a_dial_that_never_arrives(smaller_is_deaf: bool) {
    let mut pair = Pair::start(Limits::default(), Limits::default()).await;
    let (a, b) = (pair.a.node().clone(), pair.b.node().clone());
    // A gateway sits in front of the node it reaches: the deaf one is in front of the peer of the deaf node.
    let deaf = if (a.incarnation < b.incarnation) == smaller_is_deaf {
        pair.gateway_b.clone()
    } else {
        pair.gateway_a.clone()
    };
    let to_b = Target::Node(b.clone());
    let to_a = Target::Node(a.clone());
    pair.a
        .send(&to_b, "actors", 1, None, &u32::MAX.to_be_bytes())
        .unwrap();
    pair.b
        .send(&to_a, "actors", 1, None, &u32::MAX.to_be_bytes())
        .unwrap();
    for endpoint in [&mut pair.a, &mut pair.b] {
        let first = tokio::time::timeout(WITHIN, endpoint.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.payload, u32::MAX.to_be_bytes());
    }

    deaf.deaf.store(true, Ordering::SeqCst);
    pair.gateway_a.set(Mode::Cut);
    pair.gateway_b.set(Mode::Cut);
    // Both keep sending, every 200 ms, for longer than a dial takes to give up.
    let count = 60_u32;
    for index in 0..count {
        pair.a
            .send(&to_b, "actors", 1, None, &index.to_be_bytes())
            .unwrap();
        pair.b
            .send(&to_a, "actors", 1, None, &index.to_be_bytes())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for endpoint in [&mut pair.a, &mut pair.b] {
        let mut got = Vec::new();
        let taken = tokio::time::timeout(WITHIN, async {
            while got.len() < count as usize {
                let envelope = endpoint.recv().await.unwrap().unwrap();
                got.push(u32::from_be_bytes(
                    envelope.payload[..4].try_into().unwrap(),
                ));
            }
        })
        .await;
        assert!(taken.is_ok(), "only {got:?} arrived");
        assert_eq!(got, (0..count).collect::<Vec<_>>());
    }
    for heard in [&mut pair.heard_a, &mut pair.heard_b] {
        assert!(heard.try_recv().is_err(), "a peer was reported");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_smaller_incarnation_takes_the_dial_that_arrives_when_its_own_never_does() {
    crossed_with_a_dial_that_never_arrives(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_larger_incarnation_takes_the_dial_that_arrives_when_its_own_never_does() {
    crossed_with_a_dial_that_never_arrives(false).await;
}
