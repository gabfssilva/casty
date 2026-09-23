//! What TLS adds: the certificate authority is what says who is a member of the cluster.

use std::time::Duration;

use casty_net::compress::Name;
use casty_net::endpoint::{Config, Endpoint};
use casty_net::limits::Limits;
use casty_net::pool::Target;
use casty_net::tls::Tls;
use rcgen::{CertificateParams, Issuer, KeyPair, KeyUsagePurpose};

const WITHIN: Duration = Duration::from_secs(20);

/// A certificate authority and the files of a node it signed for.
struct Authority {
    key: KeyPair,
    params: CertificateParams,
    written: String,
}

impl Authority {
    fn new(named: &str, at: &std::path::Path) -> Self {
        let mut params = CertificateParams::new(vec![named.to_owned()]).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap().pem();
        let written = at
            .join(format!("{named}-ca.pem"))
            .to_string_lossy()
            .into_owned();
        std::fs::write(&written, &certificate).unwrap();
        Self {
            key,
            params,
            written,
        }
    }

    /// A certificate and key this authority signed, written next to it, as a `Tls` a node can use.
    fn node(&self, named: &str, at: &std::path::Path, require_client_cert: bool) -> Tls {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
        params.use_authority_key_identifier_extension = true;
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let issuer = Issuer::from_params(&self.params, &self.key);
        let certificate = params.signed_by(&key, &issuer).unwrap().pem();
        let cert = at
            .join(format!("{named}.pem"))
            .to_string_lossy()
            .into_owned();
        let secret = at
            .join(format!("{named}-key.pem"))
            .to_string_lossy()
            .into_owned();
        std::fs::write(&cert, &certificate).unwrap();
        std::fs::write(&secret, key.serialize_pem()).unwrap();
        Tls {
            cert,
            key: secret,
            ca: Some(self.written.clone()),
            require_client_cert,
        }
    }
}

fn secured(tls: Tls, compression: Option<Vec<Name>>) -> Config {
    Config {
        bind: Some("127.0.0.1:0".to_owned()),
        tls: Some(tls),
        compression,
        limits: Limits {
            handshake: Duration::from_secs(2),
            dial: Duration::from_secs(2),
            ..Limits::default()
        },
        ..Config::default()
    }
}

fn scratch(named: &str) -> std::path::PathBuf {
    let at = std::env::temp_dir().join(format!("casty-tls-{named}"));
    std::fs::create_dir_all(&at).unwrap();
    at
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_of_the_same_authority_reach_each_other_with_every_compressor() {
    let at = scratch("same");
    let authority = Authority::new("cluster", &at);

    for compressor in [Name::Zstd, Name::Lz4, Name::Zlib] {
        let named = compressor.name();
        let first = secured(
            authority.node(&format!("{named}-a"), &at, true),
            Some(vec![compressor]),
        );
        let second = secured(
            authority.node(&format!("{named}-b"), &at, true),
            Some(vec![compressor]),
        );
        let sender = Endpoint::start(first).await.unwrap();
        let mut receiver = Endpoint::start(second).await.unwrap();
        let payload = b"a line that repeats itself. ".repeat(20_000);

        sender
            .send(
                &Target::Seed(receiver.node().address.clone().unwrap()),
                "actors",
                &payload,
            )
            .unwrap();

        let arrived = tokio::time::timeout(WITHIN, receiver.recv()).await;
        let Ok(Some(Ok(envelope))) = arrived else {
            panic!("{named}: nothing arrived: {arrived:?}");
        };
        assert_eq!(envelope.payload, payload, "{named} changed what it carried");
        sender.close(true).await;
        receiver.close(true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_of_another_authority_reaches_nothing() {
    let at = scratch("other");
    let ours = Authority::new("ours", &at);
    let theirs = Authority::new("theirs", &at);

    let mut member = Endpoint::start(secured(ours.node("member", &at, true), None))
        .await
        .unwrap();
    let address = member.node().address.clone().unwrap();
    let stranger = Endpoint::start(secured(theirs.node("stranger", &at, true), None))
        .await
        .unwrap();
    let known = Endpoint::start(secured(ours.node("known", &at, true), None))
        .await
        .unwrap();

    stranger
        .send(&Target::Seed(address.clone()), "actors", b"let me in")
        .unwrap();
    // Whatever the stranger does, the one with a certificate of the right authority still gets through.
    known
        .send(&Target::Seed(address), "actors", b"i belong here")
        .unwrap();

    let arrived = tokio::time::timeout(WITHIN, member.recv()).await;
    let Ok(Some(Ok(envelope))) = arrived else {
        panic!("the member of the cluster did not get through: {arrived:?}");
    };
    assert_eq!(envelope.payload, b"i belong here");
    let more = tokio::time::timeout(Duration::from_millis(500), member.recv()).await;
    assert!(more.is_err(), "a node of another authority was let in");

    stranger.close(true).await;
    known.close(true).await;
    member.close(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_client_certificate_the_server_still_answers_when_it_does_not_require_one() {
    let at = scratch("optional");
    let authority = Authority::new("open", &at);
    let mut member = Endpoint::start(secured(authority.node("open-a", &at, false), None))
        .await
        .unwrap();
    let address = member.node().address.clone().unwrap();
    let other = Endpoint::start(secured(authority.node("open-b", &at, false), None))
        .await
        .unwrap();

    other
        .send(&Target::Seed(address), "actors", b"hello")
        .unwrap();

    let arrived = tokio::time::timeout(WITHIN, member.recv()).await;
    assert!(matches!(arrived, Ok(Some(Ok(_)))), "{arrived:?}");
    other.close(true).await;
    member.close(true).await;
}
