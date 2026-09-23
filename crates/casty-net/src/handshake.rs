//! What two nodes tell each other before the first envelope crosses.
//!
//! No protocol version is negotiated: the version is in every frame header, and a peer of another one is refused at
//! its first frame.
//!
//! Each message is a msgpack array of its fields in the order they are declared, on the control stream under the
//! name of the message.

use casty_core::node::NodeId;
use casty_core::schema::msgpack::{self, Int, Kind, Reader};

use crate::compress::Name;
use crate::frame::ProtocolError;

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
pub enum Message {
    Hello(Hello),
    Ack(Ack),
    /// The hello was refused, for the reason given.
    Reject(String),
    /// The rejecting node is opening its own connection to the sender, and that connection is the one both sides
    /// keep.
    Duplicate,
}

/// The reply to `hello` from the node described by `local`, or why it is refused, before duplicate connections are
/// considered.
pub fn answer(hello: &Hello, local: &Hello) -> Result<Ack, String> {
    if hello.cluster != local.cluster {
        return Err(format!(
            "cluster {:?} is not {:?}",
            hello.cluster, local.cluster
        ));
    }
    if hello.sizes != local.sizes {
        return Err(format!(
            "limits (frame, message, window) {:?} are not {:?}",
            hello.sizes, local.sizes
        ));
    }
    if hello.node == local.node {
        return Err("connection to itself".to_owned());
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
    let name = match message {
        Message::Hello(hello) => {
            msgpack::write_array_len(&mut out, 7);
            msgpack::write_str(&mut out, &hello.cluster);
            write_node(&mut out, &hello.node);
            msgpack::write_array_len(&mut out, hello.compression.len());
            for name in &hello.compression {
                msgpack::write_str(&mut out, name.name());
            }
            for size in hello.sizes {
                msgpack::write_int(&mut out, Int::Unsigned(size as u64));
            }
            "hello"
        }
        Message::Ack(ack) => {
            msgpack::write_array_len(&mut out, 3);
            write_node(&mut out, &ack.node);
            write_optional(&mut out, ack.compression.map(Name::name));
            "hello-ack"
        }
        Message::Reject(reason) => {
            msgpack::write_array_len(&mut out, 1);
            msgpack::write_str(&mut out, reason);
            "hello-reject"
        }
        Message::Duplicate => {
            msgpack::write_array_len(&mut out, 0);
            "hello-duplicate"
        }
    };
    (name, out)
}

pub fn decode(name: &str, payload: &[u8]) -> Result<Message, ProtocolError> {
    read(name, &mut Reader::new(payload))
        .ok_or_else(|| ProtocolError::new(format!("malformed handshake message {name:?}")))
}

fn read(name: &str, reader: &mut Reader<'_>) -> Option<Message> {
    Some(match (name, reader.read_array_len().ok()?) {
        ("hello", 7) => Message::Hello(Hello {
            cluster: reader.read_str().ok()?.to_owned(),
            node: read_node(reader)?,
            compression: {
                let mut names = Vec::new();
                for _ in 0..reader.read_array_len().ok()? {
                    // A compressor this build does not have is one it cannot choose.
                    names.extend(Name::of(reader.read_str().ok()?));
                }
                names
            },
            sizes: [size(reader)?, size(reader)?, size(reader)?],
        }),
        ("hello-ack", 3) => Message::Ack(Ack {
            node: read_node(reader)?,
            compression: match read_optional(reader).ok()? {
                None => None,
                Some(written) => Some(Name::of(written)?),
            },
        }),
        ("hello-reject", 1) => Message::Reject(reader.read_str().ok()?.to_owned()),
        ("hello-duplicate", 0) => Message::Duplicate,
        _ => return None,
    })
}

/// A node takes two fields: its address or nil, and its incarnation.
fn write_node(out: &mut Vec<u8>, node: &NodeId) {
    write_optional(out, node.address.as_deref());
    msgpack::write_bin(out, &node.incarnation);
}

fn read_node(reader: &mut Reader<'_>) -> Option<NodeId> {
    Some(NodeId {
        address: read_optional(reader).ok()?.map(str::to_owned),
        incarnation: reader.read_bin().ok()?.try_into().ok()?,
    })
}

fn write_optional(out: &mut Vec<u8>, text: Option<&str>) {
    match text {
        None => msgpack::write_nil(out),
        Some(text) => msgpack::write_str(out, text),
    }
}

fn read_optional<'a>(reader: &mut Reader<'a>) -> msgpack::Result<Option<&'a str>> {
    if reader.kind()? == Kind::None {
        return reader.read_nil().map(|()| None);
    }
    reader.read_str().map(Some)
}

fn size(reader: &mut Reader<'_>) -> Option<usize> {
    match reader.read_int().ok()? {
        Int::Unsigned(size) => usize::try_from(size).ok(),
        Int::Signed(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use casty_core::node::NodeId;

    use super::{Ack, Hello, Message, answer, decode, encode};
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
            Message::Reject("cluster 'other' is not 'casty'".to_owned()),
            Message::Duplicate,
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
        let refused = |theirs: Hello| answer(&theirs, &local).unwrap_err();

        assert_eq!(
            refused(hello("other", node(Some("b:1"), 2))),
            r#"cluster "other" is not "casty""#
        );
        let mut larger = hello("casty", node(Some("b:1"), 2));
        larger.sizes[1] *= 8;
        assert!(refused(larger).starts_with("limits"));
        assert_eq!(
            refused(hello("casty", node(Some("a:1"), 1))),
            "connection to itself"
        );
    }

    #[test]
    fn a_payload_that_is_not_a_handshake_is_refused() {
        assert!(decode("hello", &[0xc0]).is_err());
        assert!(decode("hello-ack", &[]).is_err());
        assert!(decode("what", &encode(&Message::Reject(String::new())).1).is_err());
    }
}
