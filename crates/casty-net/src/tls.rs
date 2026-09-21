//! The TLS of a cluster: both sides present a certificate, and the CA is what says who is a member.
//!
//! There is no hostname verification. Nodes are addressed by IP and authenticated by the certificate authority, so
//! the name in a certificate says nothing a cluster cares about.

use std::io;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};

/// Where the certificate, the key and the authority of a node are.
#[derive(Debug, Clone)]
pub struct Tls {
    pub cert: String,
    pub key: String,
    pub ca: Option<String>,
    pub require_client_cert: bool,
}

/// What a node uses to speak TLS in both directions.
#[derive(Debug, Clone)]
pub struct Identity {
    pub client: Arc<ClientConfig>,
    pub server: Arc<ServerConfig>,
}

impl Tls {
    /// Read the files and build the two configurations, or say what is wrong with them.
    pub fn identity(&self) -> io::Result<Identity> {
        let chain = certificates(&self.cert)?;
        let key = private_key(&self.key)?;
        let authority = match &self.ca {
            None => None,
            Some(path) => {
                let mut roots = RootCertStore::empty();
                for certificate in certificates(path)? {
                    roots
                        .add(certificate)
                        .map_err(|error| io::Error::other(format!("{path}: {error}")))?;
                }
                Some(Arc::new(roots))
            }
        };
        // Whether or not there is an authority, the name in a certificate is not checked: nodes are addressed by
        // IP and a cluster says nothing about names.
        let verifier: Arc<dyn ServerCertVerifier> = match &authority {
            None => Arc::new(Unverified),
            Some(roots) => Arc::new(Signed {
                roots: roots.clone(),
                provider: rustls::crypto::ring::default_provider(),
            }),
        };
        let client = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(chain.clone(), key.clone_key())
            .map_err(io::Error::other)?;
        let server = match (&authority, self.require_client_cert) {
            (Some(roots), true) => {
                let verifier = WebPkiClientVerifier::builder(roots.clone())
                    .build()
                    .map_err(io::Error::other)?;
                ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(chain, key)
                    .map_err(io::Error::other)?
            }
            _ => ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(chain, key)
                .map_err(io::Error::other)?,
        };
        Ok(Identity {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}

fn certificates(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let raw = std::fs::read(path)?;
    let found: Result<Vec<_>, _> = rustls_pemfile::certs(&mut raw.as_slice()).collect();
    let found = found.map_err(|error| io::Error::other(format!("{path}: {error}")))?;
    if found.is_empty() {
        return Err(io::Error::other(format!("{path} holds no certificate")));
    }
    Ok(found)
}

fn private_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let raw = std::fs::read(path)?;
    rustls_pemfile::private_key(&mut raw.as_slice())?
        .ok_or_else(|| io::Error::other(format!("{path} holds no private key")))
}

/// A certificate the authority signed, whatever name it carries.
#[derive(Debug)]
struct Signed {
    roots: Arc<RootCertStore>,
    provider: rustls::crypto::CryptoProvider,
}

impl ServerCertVerifier for Signed {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let parsed = rustls::server::ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &parsed,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Without a certificate authority there is nothing to verify against, and the connection is only encrypted.
#[derive(Debug)]
struct Unverified;

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
