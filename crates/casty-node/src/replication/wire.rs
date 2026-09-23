//! What an owner, the replicas of a key and a range that is changing hands say to each other.
//!
//! The three families share one band, so they share one decoder: a message is read by its tag, whatever component
//! it belongs to.

use casty_core::handoff::messages::Pull;
use casty_core::node::NodeId;
use casty_core::placement::Range;
use casty_core::replication::messages::{Copy, Epoch, Reply, Request, Stamp};
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

/// What travels on the band of the component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Request(Request),
    Reply(Reply),
    Pull(Pull),
}

#[must_use]
pub fn encode(message: &Message) -> Vec<u8> {
    let mut writer = Writer::new();
    match message {
        Message::Request(request) => request_of(&mut writer, request),
        Message::Reply(reply) => reply_of(&mut writer, reply),
        Message::Pull(pull) => pull_of(&mut writer, pull),
    }
    writer.finish()
}

pub fn decode(payload: &[u8]) -> Result<Message> {
    let mut reading = Reading::new(payload);
    let (tag, fields) = reading.tagged()?;
    let mut held = Held::default();
    for _ in 0..fields {
        held.field(&mut reading, &tag)?;
    }
    held.into_message(&tag)
}

fn request_of(writer: &mut Writer, request: &Request) {
    match request {
        Request::Prepare { actor, key, epoch } => {
            writer.entity("Prepare", 3, actor, key);
            writer.name("epoch");
            epoch_of(writer, epoch);
        }
        Request::Accept {
            actor,
            key,
            stamp,
            base,
            part,
            final_part,
            pages,
            dropped,
        } => {
            writer.entity("Accept", 8, actor, key);
            writer.name("stamp");
            stamp_of(writer, stamp);
            writer.name("base");
            writer.optional("Stamp", base.as_ref(), stamp_of);
            writer.name("part");
            writer.unsigned(u64::from(*part));
            writer.name("final");
            writer.bool(*final_part);
            writer.name("pages");
            pages_of(writer, pages);
            writer.name("dropped");
            names_of(writer, dropped);
        }
        Request::FetchPages {
            actor,
            key,
            epoch,
            names,
            part,
        } => {
            writer.entity("FetchPages", 5, actor, key);
            writer.name("epoch");
            epoch_of(writer, epoch);
            writer.name("names");
            names_of(writer, names);
            writer.name("part");
            writer.unsigned(u64::from(*part));
        }
        Request::Bury {
            actor,
            key,
            stamp,
            node,
        } => {
            writer.entity("Bury", 4, actor, key);
            writer.name("stamp");
            stamp_of(writer, stamp);
            writer.name("node");
            writer.node(node);
        }
    }
}

#[allow(clippy::too_many_lines)]
fn reply_of(writer: &mut Writer, reply: &Reply) {
    match reply {
        Reply::Promise {
            actor,
            key,
            epoch,
            replica,
            accepted,
            sizes,
            pages,
            receiving,
        } => {
            writer.entity("Promise", 8, actor, key);
            writer.name("epoch");
            epoch_of(writer, epoch);
            writer.name("replica");
            writer.node(replica);
            writer.name("accepted");
            writer.optional("Stamp", accepted.as_ref(), stamp_of);
            writer.name("sizes");
            sizes_of(writer, sizes);
            writer.name("pages");
            pages_of(writer, pages);
            writer.name("receiving");
            writer.bool(*receiving);
        }
        Reply::Rejected {
            actor,
            key,
            replica,
            promised,
        } => {
            writer.entity("Rejected", 4, actor, key);
            writer.name("replica");
            writer.node(replica);
            writer.name("promised");
            epoch_of(writer, promised);
        }
        Reply::Pages {
            actor,
            key,
            epoch,
            accepted,
            part,
            final_part,
            pages,
        } => {
            writer.entity("Pages", 7, actor, key);
            writer.name("epoch");
            epoch_of(writer, epoch);
            writer.name("accepted");
            writer.optional("Stamp", accepted.as_ref(), stamp_of);
            writer.name("part");
            writer.unsigned(u64::from(*part));
            writer.name("final");
            writer.bool(*final_part);
            writer.name("pages");
            pages_of(writer, pages);
        }
        Reply::Accepted {
            actor,
            key,
            stamp,
            replica,
            receiving,
        } => {
            writer.entity("Accepted", 5, actor, key);
            writer.name("stamp");
            stamp_of(writer, stamp);
            writer.name("replica");
            writer.node(replica);
            writer.name("receiving");
            writer.bool(*receiving);
        }
        Reply::NeedFull {
            actor,
            key,
            stamp,
            replica,
        } => {
            writer.entity("NeedFull", 4, actor, key);
            writer.name("stamp");
            stamp_of(writer, stamp);
            writer.name("replica");
            writer.node(replica);
        }
        Reply::Buried {
            actor,
            key,
            stamp,
            replica,
            receiving,
        } => {
            writer.entity("Buried", 5, actor, key);
            writer.name("stamp");
            stamp_of(writer, stamp);
            writer.name("replica");
            writer.node(replica);
            writer.name("receiving");
            writer.bool(*receiving);
        }
    }
}

fn pull_of(writer: &mut Writer, pull: &Pull) {
    match pull {
        Pull::PullRange {
            actor,
            node,
            transfer,
            ranges,
        } => {
            writer.tagged("PullRange", 4);
            writer.name("actor");
            writer.text(actor);
            writer.name("node");
            writer.node(node);
            writer.name("transfer");
            writer.unsigned(*transfer);
            writer.name("ranges");
            writer.items(ranges.len());
            for range in ranges {
                writer.fields(2);
                writer.name("start");
                writer.unsigned(range.start);
                writer.name("end");
                writer.unsigned(range.end);
            }
        }
        Pull::RangeKeys {
            actor,
            replica,
            transfer,
            stream,
            part,
            final_part,
            receiving,
            keys,
        } => {
            writer.tagged("RangeKeys", 8);
            writer.name("actor");
            writer.text(actor);
            writer.name("replica");
            writer.node(replica);
            writer.name("transfer");
            writer.unsigned(*transfer);
            writer.name("stream");
            writer.unsigned(*stream);
            writer.name("part");
            writer.unsigned(u64::from(*part));
            writer.name("final");
            writer.bool(*final_part);
            writer.name("receiving");
            writer.bool(*receiving);
            writer.name("keys");
            copies_of(writer, keys);
        }
        Pull::HandKeys {
            actor,
            node,
            stream,
            part,
            final_part,
            keys,
        } => {
            writer.tagged("HandKeys", 6);
            writer.name("actor");
            writer.text(actor);
            writer.name("node");
            writer.node(node);
            writer.name("stream");
            writer.unsigned(*stream);
            writer.name("part");
            writer.unsigned(u64::from(*part));
            writer.name("final");
            writer.bool(*final_part);
            writer.name("keys");
            copies_of(writer, keys);
        }
        Pull::TookKeys {
            actor,
            replica,
            keys,
        } => {
            writer.tagged("TookKeys", 3);
            writer.name("actor");
            writer.text(actor);
            writer.name("replica");
            writer.node(replica);
            writer.name("keys");
            names_of(writer, keys);
        }
    }
}

fn epoch_of(writer: &mut Writer, epoch: &Epoch) {
    writer.fields(2);
    writer.name("round");
    writer.unsigned(epoch.round);
    writer.name("node");
    writer.node(&epoch.node);
}

fn stamp_of(writer: &mut Writer, stamp: &Stamp) {
    writer.fields(2);
    writer.name("epoch");
    epoch_of(writer, &stamp.epoch);
    writer.name("version");
    writer.unsigned(stamp.version);
}

fn copies_of(writer: &mut Writer, keys: &[Copy]) {
    writer.items(keys.len());
    for copy in keys {
        writer.fields(6);
        writer.name("key");
        writer.text(&copy.key);
        writer.name("accepted");
        writer.optional("Stamp", copy.accepted.as_ref(), stamp_of);
        writer.name("promised");
        writer.optional("Epoch", copy.promised.as_ref(), epoch_of);
        writer.name("pages");
        pages_of(writer, &copy.pages);
        writer.name("part");
        writer.unsigned(u64::from(copy.part));
        writer.name("final");
        writer.bool(copy.final_part);
    }
}

fn sizes_of(writer: &mut Writer, sizes: &[(String, usize)]) {
    writer.pairs(sizes.len());
    for (name, size) in sizes {
        writer.pair();
        writer.text(name);
        writer.unsigned(*size as u64);
    }
}

fn pages_of(writer: &mut Writer, pages: &Pages) {
    writer.pairs(pages.len());
    for (name, data) in pages {
        writer.pair();
        writer.text(name);
        writer.bytes(data);
    }
}

fn names_of(writer: &mut Writer, names: &[String]) {
    writer.items(names.len());
    for name in names {
        writer.text(name);
    }
}

/// Every field these messages have, read in whatever order they came in.
#[derive(Debug, Default)]
struct Held {
    actor: String,
    key: String,
    epoch: Option<Epoch>,
    promised: Option<Epoch>,
    stamp: Option<Stamp>,
    base: Option<Stamp>,
    accepted: Option<Stamp>,
    replica: Option<NodeId>,
    node: Option<NodeId>,
    sizes: Vec<(String, usize)>,
    pages: Pages,
    names: Vec<String>,
    dropped: Vec<String>,
    ranges: Vec<Range>,
    copies: Vec<Copy>,
    transfer: u64,
    stream: u64,
    part: u32,
    final_part: bool,
    receiving: bool,
}

impl Held {
    fn field(&mut self, reading: &mut Reading<'_>, tag: &str) -> Result<()> {
        match reading.name()? {
            "actor" => self.actor = reading.text()?,
            "key" => self.key = reading.text()?,
            "epoch" => self.epoch = Some(read_epoch(reading)?),
            "promised" => self.promised = Some(read_epoch(reading)?),
            "stamp" => self.stamp = Some(read_stamp(reading)?),
            "base" => self.base = reading.optional(read_stamp)?,
            "accepted" => self.accepted = reading.optional(read_stamp)?,
            "replica" => self.replica = Some(reading.node()?),
            "node" => self.node = Some(reading.node()?),
            "sizes" => self.sizes = read_sizes(reading)?,
            "pages" => self.pages = read_pages(reading)?,
            "names" => self.names = read_names(reading)?,
            "dropped" => self.dropped = read_names(reading)?,
            "ranges" => self.ranges = read_ranges(reading)?,
            // The only field whose type depends on the message it belongs to.
            "keys" => {
                if tag == "TookKeys" {
                    self.names = read_names(reading)?;
                } else {
                    self.copies = read_copies(reading)?;
                }
            }
            "transfer" => self.transfer = reading.unsigned()?,
            "stream" => self.stream = reading.unsigned()?,
            "part" => self.part = small(reading.unsigned()?)?,
            "final" => self.final_part = reading.bool()?,
            "receiving" => self.receiving = reading.bool()?,
            _ => reading.skip()?,
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn into_message(self, tag: &str) -> Result<Message> {
        let missing = || Malformed::Truncated;
        Ok(match tag {
            "Prepare" => Message::Request(Request::Prepare {
                actor: self.actor,
                key: self.key,
                epoch: self.epoch.ok_or_else(missing)?,
            }),
            "Accept" => Message::Request(Request::Accept {
                actor: self.actor,
                key: self.key,
                stamp: self.stamp.ok_or_else(missing)?,
                base: self.base,
                part: self.part,
                final_part: self.final_part,
                pages: self.pages,
                dropped: self.dropped,
            }),
            "FetchPages" => Message::Request(Request::FetchPages {
                actor: self.actor,
                key: self.key,
                epoch: self.epoch.ok_or_else(missing)?,
                names: self.names,
                part: self.part,
            }),
            "Bury" => Message::Request(Request::Bury {
                actor: self.actor,
                key: self.key,
                stamp: self.stamp.ok_or_else(missing)?,
                node: self.node.ok_or_else(missing)?,
            }),
            "Promise" => Message::Reply(Reply::Promise {
                actor: self.actor,
                key: self.key,
                epoch: self.epoch.ok_or_else(missing)?,
                replica: self.replica.ok_or_else(missing)?,
                accepted: self.accepted,
                sizes: self.sizes,
                pages: self.pages,
                receiving: self.receiving,
            }),
            "Rejected" => Message::Reply(Reply::Rejected {
                actor: self.actor,
                key: self.key,
                replica: self.replica.ok_or_else(missing)?,
                promised: self.promised.ok_or_else(missing)?,
            }),
            "Pages" => Message::Reply(Reply::Pages {
                actor: self.actor,
                key: self.key,
                epoch: self.epoch.ok_or_else(missing)?,
                accepted: self.accepted,
                part: self.part,
                final_part: self.final_part,
                pages: self.pages,
            }),
            "Accepted" => Message::Reply(Reply::Accepted {
                actor: self.actor,
                key: self.key,
                stamp: self.stamp.ok_or_else(missing)?,
                replica: self.replica.ok_or_else(missing)?,
                receiving: self.receiving,
            }),
            "NeedFull" => Message::Reply(Reply::NeedFull {
                actor: self.actor,
                key: self.key,
                stamp: self.stamp.ok_or_else(missing)?,
                replica: self.replica.ok_or_else(missing)?,
            }),
            "Buried" => Message::Reply(Reply::Buried {
                actor: self.actor,
                key: self.key,
                stamp: self.stamp.ok_or_else(missing)?,
                replica: self.replica.ok_or_else(missing)?,
                receiving: self.receiving,
            }),
            "PullRange" => Message::Pull(Pull::PullRange {
                actor: self.actor,
                node: self.node.ok_or_else(missing)?,
                transfer: self.transfer,
                ranges: self.ranges,
            }),
            "RangeKeys" => Message::Pull(Pull::RangeKeys {
                actor: self.actor,
                replica: self.replica.ok_or_else(missing)?,
                transfer: self.transfer,
                stream: self.stream,
                part: self.part,
                final_part: self.final_part,
                receiving: self.receiving,
                keys: self.copies,
            }),
            "HandKeys" => Message::Pull(Pull::HandKeys {
                actor: self.actor,
                node: self.node.ok_or_else(missing)?,
                stream: self.stream,
                part: self.part,
                final_part: self.final_part,
                keys: self.copies,
            }),
            "TookKeys" => Message::Pull(Pull::TookKeys {
                actor: self.actor,
                replica: self.replica.ok_or_else(missing)?,
                keys: self.names,
            }),
            _ => return Err(Malformed::Marker(0)),
        })
    }
}

fn small(value: u64) -> Result<u32> {
    u32::try_from(value).map_err(|_| Malformed::Truncated)
}

fn read_epoch(reading: &mut Reading<'_>) -> Result<Epoch> {
    let fields = reading.fields()?;
    let mut round = None;
    let mut node = None;
    for _ in 0..fields {
        match reading.name()? {
            "round" => round = Some(reading.unsigned()?),
            "node" => node = Some(reading.node()?),
            _ => reading.skip()?,
        }
    }
    Ok(Epoch {
        round: round.ok_or(Malformed::Truncated)?,
        node: node.ok_or(Malformed::Truncated)?,
    })
}

fn read_stamp(reading: &mut Reading<'_>) -> Result<Stamp> {
    let fields = reading.fields()?;
    let mut epoch = None;
    let mut version = None;
    for _ in 0..fields {
        match reading.name()? {
            "epoch" => epoch = Some(read_epoch(reading)?),
            "version" => version = Some(reading.unsigned()?),
            _ => reading.skip()?,
        }
    }
    Ok(Stamp {
        epoch: epoch.ok_or(Malformed::Truncated)?,
        version: version.ok_or(Malformed::Truncated)?,
    })
}

fn read_pages(reading: &mut Reading<'_>) -> Result<Pages> {
    let count = reading.items()?;
    let mut pages = Pages::new();
    for _ in 0..count {
        reading.items()?;
        let name = reading.text()?;
        pages.insert(name, reading.bytes()?);
    }
    Ok(pages)
}

fn read_sizes(reading: &mut Reading<'_>) -> Result<Vec<(String, usize)>> {
    let count = reading.items()?;
    let mut sizes = Vec::with_capacity(count);
    for _ in 0..count {
        reading.items()?;
        let name = reading.text()?;
        let size = usize::try_from(reading.unsigned()?).map_err(|_| Malformed::Truncated)?;
        sizes.push((name, size));
    }
    Ok(sizes)
}

fn read_names(reading: &mut Reading<'_>) -> Result<Vec<String>> {
    let count = reading.items()?;
    (0..count).map(|_| reading.text()).collect()
}

fn read_ranges(reading: &mut Reading<'_>) -> Result<Vec<Range>> {
    let count = reading.items()?;
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        let fields = reading.fields()?;
        let mut start = None;
        let mut end = None;
        for _ in 0..fields {
            match reading.name()? {
                "start" => start = Some(reading.unsigned()?),
                "end" => end = Some(reading.unsigned()?),
                _ => reading.skip()?,
            }
        }
        ranges.push(Range {
            start: start.ok_or(Malformed::Truncated)?,
            end: end.ok_or(Malformed::Truncated)?,
        });
    }
    Ok(ranges)
}

fn read_copies(reading: &mut Reading<'_>) -> Result<Vec<Copy>> {
    let count = reading.items()?;
    let mut copies = Vec::with_capacity(count);
    for _ in 0..count {
        let fields = reading.fields()?;
        let mut key = String::new();
        let mut accepted = None;
        let mut promised = None;
        let mut pages = Pages::new();
        let mut part = 0;
        let mut final_part = false;
        for _ in 0..fields {
            match reading.name()? {
                "key" => key = reading.text()?,
                "accepted" => accepted = reading.optional(read_stamp)?,
                "promised" => promised = reading.optional(read_epoch)?,
                "pages" => pages = read_pages(reading)?,
                "part" => part = small(reading.unsigned()?)?,
                "final" => final_part = reading.bool()?,
                _ => reading.skip()?,
            }
        }
        copies.push(Copy {
            key,
            accepted,
            promised,
            pages,
            part,
            final_part,
        });
    }
    Ok(copies)
}

#[cfg(test)]
mod tests {
    use casty_core::replication::messages::{Epoch, Reply, Request, Stamp};
    use casty_core::rolls::Rolls;

    use super::{Message, decode, encode};

    #[test]
    fn a_burial_and_its_answer_read_back_as_they_were_written() {
        let ids = Rolls::seeded(81).nodes(2);
        let stamp = Stamp {
            epoch: Epoch {
                round: 3,
                node: ids[0].clone(),
            },
            version: 9,
        };
        let messages = [
            Message::Request(Request::Bury {
                actor: "tests.app:ledger".to_owned(),
                key: "key-1".to_owned(),
                stamp: stamp.clone(),
                node: ids[1].clone(),
            }),
            Message::Reply(Reply::Buried {
                actor: "tests.app:ledger".to_owned(),
                key: "key-1".to_owned(),
                stamp,
                replica: ids[0].clone(),
                receiving: true,
            }),
        ];
        for message in messages {
            assert_eq!(decode(&encode(&message)).ok(), Some(message));
        }
    }
}
