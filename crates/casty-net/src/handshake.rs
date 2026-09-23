//! What two nodes tell each other before the first envelope crosses.
//!
//! No protocol version is negotiated: the version is in every frame header, and a peer of another one is refused at
//! its first frame.
//!
//! The messages travel on the control stream as msgpack maps of named fields. They name no payload format: there is
//! one, msgpack.

use casty_core::node::NodeId;
use casty_core::schema::msgpack::{self, Int, Kind, Reader};

use crate::compress::Name;
use crate::frame::ProtocolError;

/// Why a hello was rejected. `Duplicate` means the rejecting node is opening its own connection to the sender, and
/// that connection is the one both sides keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Cluster = 1,
    Itself = 4,
    Duplicate = 5,
    Limits = 6,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub cluster: String,
    pub node: NodeId,
    pub compression: Vec<Name>,
    /// `frame`, `message` and `window` of `Limits`, which a node bounds what it receives by. Each side sends by its
    /// own, so a peer with other sizes would break the connection at the first frame, envelope or window past them:
    /// the handshake refuses it instead.
    pub sizes: [usize; 3],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    pub node: NodeId,
    pub compression: Option<Name>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject {
    pub code: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Hello(Hello),
    Ack(Ack),
    Reject(Reject),
}

/// The reply to `hello` from the node described by `local`, before duplicate connections are considered.
pub fn answer(hello: &Hello, local: &Hello) -> Result<Ack, Reject> {
    if hello.cluster != local.cluster {
        return Err(Reject {
            code: Rejection::Cluster as i64,
            reason: format!("cluster {:?} is not {:?}", hello.cluster, local.cluster),
        });
    }
    if hello.sizes != local.sizes {
        return Err(Reject {
            code: Rejection::Limits as i64,
            reason: format!(
                "limits (frame, message, window) {:?} are not {:?}",
                hello.sizes, local.sizes
            ),
        });
    }
    if hello.node == local.node {
        return Err(Reject {
            code: Rejection::Itself as i64,
            reason: "connection to itself".to_owned(),
        });
    }
    Ok(Ack {
        node: local.node.clone(),
        compression: crate::compress::chosen(&hello.compression, &local.compression),
    })
}

/// The name and payload of the envelope that carries `message` on the control stream.
#[must_use]
pub fn encode(message: &Message) -> (&'static str, Vec<u8>) {
    let mut out = Vec::new();
    match message {
        Message::Hello(hello) => {
            msgpack::write_map_len(&mut out, 7);
            key(&mut out, "cluster");
            msgpack::write_str(&mut out, &hello.cluster);
            key(&mut out, "address");
            address(&mut out, &hello.node);
            key(&mut out, "incarnation");
            msgpack::write_bin(&mut out, &hello.node.incarnation);
            key(&mut out, "compression");
            msgpack::write_array_len(&mut out, hello.compression.len());
            for name in &hello.compression {
                msgpack::write_str(&mut out, name.name());
            }
            for (name, size) in ["frame", "message", "window"].into_iter().zip(hello.sizes) {
                key(&mut out, name);
                msgpack::write_int(&mut out, Int::Unsigned(size as u64));
            }
            ("hello", out)
        }
        Message::Ack(ack) => {
            msgpack::write_map_len(&mut out, 3);
            key(&mut out, "address");
            address(&mut out, &ack.node);
            key(&mut out, "incarnation");
            msgpack::write_bin(&mut out, &ack.node.incarnation);
            key(&mut out, "compression");
            match ack.compression {
                None => msgpack::write_nil(&mut out),
                Some(name) => msgpack::write_str(&mut out, name.name()),
            }
            ("hello-ack", out)
        }
        Message::Reject(reject) => {
            msgpack::write_map_len(&mut out, 2);
            key(&mut out, "code");
            msgpack::write_int(&mut out, Int::Signed(reject.code));
            key(&mut out, "reason");
            msgpack::write_str(&mut out, &reject.reason);
            ("hello-reject", out)
        }
    }
}

pub fn decode(name: &str, payload: &[u8]) -> Result<Message, ProtocolError> {
    let malformed = || ProtocolError::new(format!("malformed handshake message {name:?}"));
    let mut fields = Fields::read(payload).ok_or_else(malformed)?;
    match name {
        "hello" => Ok(Message::Hello(Hello {
            cluster: fields.text("cluster").ok_or_else(malformed)?,
            node: fields.node().ok_or_else(malformed)?,
            // A compressor this build does not have is one it cannot choose.
            compression: fields
                .texts("compression")
                .ok_or_else(malformed)?
                .iter()
                .filter_map(|name| Name::of(name))
                .collect(),
            sizes: [
                fields.size("frame").ok_or_else(malformed)?,
                fields.size("message").ok_or_else(malformed)?,
                fields.size("window").ok_or_else(malformed)?,
            ],
        })),
        "hello-ack" => {
            let compression = match fields.take("compression") {
                Some(Value::Nil) => None,
                Some(Value::Text(name)) => Some(Name::of(&name).ok_or_else(malformed)?),
                _ => return Err(malformed()),
            };
            Ok(Message::Ack(Ack {
                node: fields.node().ok_or_else(malformed)?,
                compression,
            }))
        }
        "hello-reject" => Ok(Message::Reject(Reject {
            code: fields.integer("code").ok_or_else(malformed)?,
            reason: fields.text("reason").ok_or_else(malformed)?,
        })),
        _ => Err(malformed()),
    }
}

fn key(out: &mut Vec<u8>, name: &str) {
    msgpack::write_str(out, name);
}

fn address(out: &mut Vec<u8>, node: &NodeId) {
    match &node.address {
        None => msgpack::write_nil(out),
        Some(address) => msgpack::write_str(out, address),
    }
}

/// A handshake message read as named values, which is all these maps hold.
#[derive(Debug)]
struct Fields(Vec<(String, Value)>);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    Nil,
    Int(i64),
    Text(String),
    Bin(Vec<u8>),
    List(Vec<Value>),
}

impl Fields {
    fn read(payload: &[u8]) -> Option<Self> {
        let mut reader = Reader::new(payload);
        let len = reader.read_map_len().ok()?;
        let mut found = Vec::with_capacity(len);
        for _ in 0..len {
            let name = reader.read_str().ok()?.to_owned();
            found.push((name, value(&mut reader)?));
        }
        Some(Self(found))
    }

    fn take(&mut self, name: &str) -> Option<Value> {
        let at = self.0.iter().position(|(held, _)| held == name)?;
        Some(self.0.remove(at).1)
    }

    fn integer(&mut self, name: &str) -> Option<i64> {
        match self.take(name)? {
            Value::Int(value) => Some(value),
            _ => None,
        }
    }

    fn size(&mut self, name: &str) -> Option<usize> {
        usize::try_from(self.integer(name)?).ok()
    }

    fn text(&mut self, name: &str) -> Option<String> {
        match self.take(name)? {
            Value::Text(value) => Some(value),
            _ => None,
        }
    }

    fn texts(&mut self, name: &str) -> Option<Vec<String>> {
        match self.take(name)? {
            Value::List(items) => Some(
                items
                    .into_iter()
                    .filter_map(|item| match item {
                        Value::Text(value) => Some(value),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    fn node(&mut self) -> Option<NodeId> {
        let address = match self.take("address")? {
            Value::Nil => None,
            Value::Text(address) => Some(address),
            _ => return None,
        };
        let Value::Bin(raw) = self.take("incarnation")? else {
            return None;
        };
        Some(NodeId {
            address,
            incarnation: raw.try_into().ok()?,
        })
    }
}

fn value(reader: &mut Reader<'_>) -> Option<Value> {
    Some(match reader.kind().ok()? {
        Kind::None => {
            reader.read_nil().ok()?;
            Value::Nil
        }
        Kind::Int => match reader.read_int().ok()? {
            Int::Signed(held) => Value::Int(held),
            Int::Unsigned(held) => Value::Int(i64::try_from(held).ok()?),
        },
        Kind::Str => Value::Text(reader.read_str().ok()?.to_owned()),
        Kind::Bytes => Value::Bin(reader.read_bin().ok()?.to_vec()),
        Kind::List => {
            let len = reader.read_array_len().ok()?;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(value(reader)?);
            }
            Value::List(items)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use casty_core::node::NodeId;

    use super::{Ack, Hello, Message, Reject, Rejection, answer, decode, encode};
    use crate::compress::{Name, PREFERENCE};
    use crate::limits::Limits;

    fn node(address: Option<&str>, tag: u8) -> NodeId {
        NodeId {
            address: address.map(str::to_owned),
            incarnation: [tag; 16],
        }
    }

    fn hello(cluster: &str, node: NodeId) -> Hello {
        let limits = Limits::default();
        Hello {
            cluster: cluster.to_owned(),
            node,
            compression: PREFERENCE.to_vec(),
            sizes: [limits.frame, limits.message, limits.window],
        }
    }

    #[test]
    fn every_message_reads_back_as_it_was_written() {
        let messages = [
            Message::Hello(hello("casty", node(Some("127.0.0.1:7400"), 1))),
            Message::Hello(hello("casty", node(None, 2))),
            Message::Ack(Ack {
                node: node(Some("10.0.0.1:1"), 3),
                compression: Some(Name::Lz4),
            }),
            Message::Ack(Ack {
                node: node(None, 4),
                compression: None,
            }),
            Message::Reject(Reject {
                code: Rejection::Cluster as i64,
                reason: "cluster 'other' is not 'casty'".to_owned(),
            }),
        ];
        for message in messages {
            let (name, payload) = encode(&message);
            assert_eq!(decode(name, &payload), Ok(message));
        }
    }

    #[test]
    fn it_answers_a_hello_it_can_speak_to() {
        let local = hello("casty", node(Some("a:1"), 1));
        let ack = answer(&hello("casty", node(Some("b:1"), 2)), &local).unwrap();

        assert_eq!(ack.node, local.node);
        assert_eq!(ack.compression, Some(Name::Zstd));
    }

    #[test]
    fn it_rejects_what_it_cannot_speak_to() {
        let local = hello("casty", node(Some("a:1"), 1));
        let refused = |theirs: Hello| answer(&theirs, &local).unwrap_err().code;

        let mut other = hello("other", node(Some("b:1"), 2));
        assert_eq!(refused(other.clone()), Rejection::Cluster as i64);
        other = hello("casty", node(Some("b:1"), 2));
        other.sizes[1] *= 8;
        assert_eq!(refused(other), Rejection::Limits as i64);
        assert_eq!(
            refused(hello("casty", node(Some("a:1"), 1))),
            Rejection::Itself as i64
        );
    }

    #[test]
    fn a_payload_that_is_not_a_handshake_is_refused() {
        assert!(decode("hello", &[0xc0]).is_err());
        assert!(decode("hello-ack", &[]).is_err());
        assert!(
            decode(
                "what",
                &encode(&Message::Reject(Reject {
                    code: 1,
                    reason: String::new()
                }))
                .1
            )
            .is_err()
        );
    }
}
