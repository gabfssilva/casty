//! The messages the `membership` components send each other.
//!
//! A member sends its own record, so that every message also carries an observation of the sender. A client is not a
//! member and sends only its identity.

use std::collections::BTreeSet;

use casty_core::membership::broadcast::{Broadcast, EventId};
use casty_core::membership::table::{Record, Status};
use casty_core::membership::views::View;
use casty_core::node::NodeId;
use casty_core::schema::msgpack::Malformed;
use casty_core::wire::{Reading, Result, Writer};

/// What travels between the `membership` components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub sender: Sender,
    pub body: Body,
}

/// Who sent a message, and what it says about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sender {
    Member(Record),
    Client(NodeId),
}

impl Sender {
    #[must_use]
    pub fn node(&self) -> &NodeId {
        match self {
            Self::Member(record) => &record.node,
            Self::Client(node) => node,
        }
    }

    #[must_use]
    pub fn record(&self) -> Option<&Record> {
        match self {
            Self::Member(record) => Some(record),
            Self::Client(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    View(View),
    Broadcast(Broadcast),
    /// The whole table of the sender, answered with the table of the receiver.
    Sync(Vec<Record>),
    SyncReply(Vec<Record>),
    Ping,
    Ack,
}

#[must_use]
pub fn encode(message: &Message) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Message", 2);
    writer.name("sender");
    match &message.sender {
        Sender::Member(record) => tagged_record(&mut writer, record),
        Sender::Client(node) => writer.tagged_node(node),
    }
    writer.name("body");
    body(&mut writer, &message.body);
    writer.finish()
}

pub fn decode(payload: &[u8]) -> Result<Message> {
    let mut reading = Reading::new(payload);
    let (tag, fields) = reading.tagged()?;
    if tag != "Message" || fields != 2 {
        return Err(Malformed::Marker(0));
    }
    let mut sender = None;
    let mut body = None;
    for _ in 0..fields {
        match reading.name()? {
            "sender" => sender = Some(read_sender(&mut reading)?),
            "body" => body = Some(read_body(&mut reading)?),
            _ => reading.skip()?,
        }
    }
    Ok(Message {
        sender: sender.ok_or(Malformed::Truncated)?,
        body: body.ok_or(Malformed::Truncated)?,
    })
}

fn tagged_record(writer: &mut Writer, record: &Record) {
    writer.tagged("Record", 4);
    record_fields(writer, record);
}

fn record(writer: &mut Writer, held: &Record) {
    writer.fields(4);
    record_fields(writer, held);
}

fn record_fields(writer: &mut Writer, held: &Record) {
    writer.name("node");
    writer.node(&held.node);
    writer.name("incarnation");
    writer.unsigned(held.incarnation);
    writer.name("status");
    writer.text(held.status.name());
    writer.name("types");
    writer.items(held.types.len());
    for name in &held.types {
        writer.text(name);
    }
}

fn event(writer: &mut Writer, id: &EventId) {
    writer.fields(2);
    writer.name("origin");
    writer.node(&id.node);
    writer.name("sequence");
    writer.unsigned(id.sequence);
}

fn records(writer: &mut Writer, held: &[Record]) {
    writer.items(held.len());
    for one in held {
        record(writer, one);
    }
}

fn nodes(writer: &mut Writer, held: &[NodeId]) {
    writer.items(held.len());
    for node in held {
        writer.node(node);
    }
}

#[allow(clippy::too_many_lines)]
fn body(writer: &mut Writer, held: &Body) {
    match held {
        Body::View(View::Join) => writer.tagged("Join", 0),
        Body::View(View::ForwardJoin { joiner, ttl }) => {
            writer.tagged("ForwardJoin", 2);
            writer.name("joiner");
            writer.node(joiner);
            writer.name("ttl");
            writer.unsigned(u64::from(*ttl));
        }
        Body::View(View::Neighbor { priority }) => {
            writer.tagged("Neighbor", 1);
            writer.name("priority");
            writer.bool(*priority);
        }
        Body::View(View::NeighborReply { accepted }) => {
            writer.tagged("NeighborReply", 1);
            writer.name("accepted");
            writer.bool(*accepted);
        }
        Body::View(View::Disconnect) => writer.tagged("Disconnect", 0),
        Body::View(View::Shuffle {
            origin,
            sample,
            ttl,
        }) => {
            writer.tagged("Shuffle", 3);
            writer.name("origin");
            writer.node(origin);
            writer.name("sample");
            nodes(writer, sample);
            writer.name("ttl");
            writer.unsigned(u64::from(*ttl));
        }
        Body::View(View::ShuffleReply { sample }) => {
            writer.tagged("ShuffleReply", 1);
            writer.name("sample");
            nodes(writer, sample);
        }
        Body::Broadcast(Broadcast::Gossip { id, record: held }) => {
            writer.tagged("Gossip", 2);
            writer.name("id");
            event(writer, id);
            writer.name("record");
            record(writer, held);
        }
        Body::Broadcast(Broadcast::IHave(id)) => {
            writer.tagged("IHave", 1);
            writer.name("id");
            event(writer, id);
        }
        Body::Broadcast(Broadcast::Graft(id)) => {
            writer.tagged("Graft", 1);
            writer.name("id");
            event(writer, id);
        }
        Body::Broadcast(Broadcast::Prune) => writer.tagged("Prune", 0),
        Body::Sync(held) => {
            writer.tagged("Sync", 1);
            writer.name("records");
            records(writer, held);
        }
        Body::SyncReply(held) => {
            writer.tagged("SyncReply", 1);
            writer.name("records");
            records(writer, held);
        }
        Body::Ping => writer.tagged("Ping", 0),
        Body::Ack => writer.tagged("Ack", 0),
    }
}

fn read_sender(reading: &mut Reading<'_>) -> Result<Sender> {
    let (tag, fields) = reading.tagged()?;
    match tag.as_str() {
        "Record" => Ok(Sender::Member(read_record_fields(reading, fields)?)),
        "NodeId" => Ok(Sender::Client(read_node_fields(reading, fields)?)),
        _ => Err(Malformed::Marker(0)),
    }
}

fn read_record(reading: &mut Reading<'_>) -> Result<Record> {
    let fields = reading.fields()?;
    read_record_fields(reading, fields)
}

fn read_record_fields(reading: &mut Reading<'_>, fields: usize) -> Result<Record> {
    let mut node = None;
    let mut incarnation = None;
    let mut status = None;
    let mut types = BTreeSet::new();
    for _ in 0..fields {
        match reading.name()? {
            "node" => node = Some(reading.node()?),
            "incarnation" => incarnation = Some(reading.unsigned()?),
            "status" => status = Status::of(&reading.text()?),
            "types" => {
                let count = reading.items()?;
                for _ in 0..count {
                    types.insert(reading.text()?);
                }
            }
            _ => reading.skip()?,
        }
    }
    Ok(Record {
        node: node.ok_or(Malformed::Truncated)?,
        incarnation: incarnation.ok_or(Malformed::Truncated)?,
        status: status.ok_or(Malformed::Truncated)?,
        types,
    })
}

fn read_node_fields(reading: &mut Reading<'_>, fields: usize) -> Result<NodeId> {
    let mut address = None;
    let mut incarnation: Option<Vec<u8>> = None;
    for _ in 0..fields {
        match reading.name()? {
            "address" => address = reading.address()?,
            "incarnation" => incarnation = Some(reading.bytes()?),
            _ => reading.skip()?,
        }
    }
    Ok(NodeId {
        address,
        incarnation: incarnation
            .ok_or(Malformed::Truncated)?
            .try_into()
            .map_err(|_| Malformed::Truncated)?,
    })
}

fn read_event(reading: &mut Reading<'_>) -> Result<EventId> {
    let fields = reading.fields()?;
    let mut node = None;
    let mut sequence = None;
    for _ in 0..fields {
        match reading.name()? {
            "origin" => node = Some(reading.node()?),
            "sequence" => sequence = Some(reading.unsigned()?),
            _ => reading.skip()?,
        }
    }
    Ok(EventId {
        node: node.ok_or(Malformed::Truncated)?,
        sequence: sequence.ok_or(Malformed::Truncated)?,
    })
}

fn read_nodes(reading: &mut Reading<'_>) -> Result<Vec<NodeId>> {
    let count = reading.items()?;
    (0..count).map(|_| reading.node()).collect()
}

fn read_records(reading: &mut Reading<'_>) -> Result<Vec<Record>> {
    let count = reading.items()?;
    (0..count).map(|_| read_record(reading)).collect()
}

#[allow(clippy::too_many_lines)]
fn read_body(reading: &mut Reading<'_>) -> Result<Body> {
    let (tag, fields) = reading.tagged()?;
    let mut node = None;
    let mut sample: Option<Vec<NodeId>> = None;
    let mut ttl = None;
    let mut flag = None;
    let mut id = None;
    let mut held = None;
    let mut listed: Option<Vec<Record>> = None;
    for _ in 0..fields {
        match reading.name()? {
            "joiner" | "origin" => node = Some(reading.node()?),
            "ttl" => {
                ttl = Some(u32::try_from(reading.unsigned()?).map_err(|_| Malformed::Truncated)?);
            }
            "priority" | "accepted" => flag = Some(reading.bool()?),
            "sample" => sample = Some(read_nodes(reading)?),
            "id" => id = Some(read_event(reading)?),
            "record" => held = Some(read_record(reading)?),
            "records" => listed = Some(read_records(reading)?),
            _ => reading.skip()?,
        }
    }
    let missing = || Malformed::Truncated;
    Ok(match tag.as_str() {
        "Join" => Body::View(View::Join),
        "ForwardJoin" => Body::View(View::ForwardJoin {
            joiner: node.ok_or_else(missing)?,
            ttl: ttl.ok_or_else(missing)?,
        }),
        "Neighbor" => Body::View(View::Neighbor {
            priority: flag.ok_or_else(missing)?,
        }),
        "NeighborReply" => Body::View(View::NeighborReply {
            accepted: flag.ok_or_else(missing)?,
        }),
        "Disconnect" => Body::View(View::Disconnect),
        "Shuffle" => Body::View(View::Shuffle {
            origin: node.ok_or_else(missing)?,
            sample: sample.ok_or_else(missing)?,
            ttl: ttl.ok_or_else(missing)?,
        }),
        "ShuffleReply" => Body::View(View::ShuffleReply {
            sample: sample.ok_or_else(missing)?,
        }),
        "Gossip" => Body::Broadcast(Broadcast::Gossip {
            id: id.ok_or_else(missing)?,
            record: held.ok_or_else(missing)?,
        }),
        "IHave" => Body::Broadcast(Broadcast::IHave(id.ok_or_else(missing)?)),
        "Graft" => Body::Broadcast(Broadcast::Graft(id.ok_or_else(missing)?)),
        "Prune" => Body::Broadcast(Broadcast::Prune),
        "Sync" => Body::Sync(listed.ok_or_else(missing)?),
        "SyncReply" => Body::SyncReply(listed.ok_or_else(missing)?),
        "Ping" => Body::Ping,
        "Ack" => Body::Ack,
        _ => return Err(Malformed::Marker(0)),
    })
}
