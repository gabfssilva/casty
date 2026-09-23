//! The protocol messages of the components, written in the msgpack the schema of a value produces.
//!
//! A component sends dataclasses, and the schema writes one as a map of its fields, tagged by `__qualname__` when it
//! is the top of a value or an alternative of a union. Nothing here reflects anything: the shape of each message is
//! written out, which is what lets a node of either implementation read the other.

use crate::node::{NodeId, Target};
use crate::schema::msgpack::{self, Int, Kind, Malformed, Reader};

pub type Result<T> = core::result::Result<T, Malformed>;

/// A value being written out, field by field.
#[derive(Debug, Default)]
pub struct Writer(Vec<u8>);

impl Writer {
    #[must_use]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.0
    }

    /// The tag a dataclass travels under at the top of a value or inside a union, ahead of the map of its fields.
    pub fn tag(&mut self, qualname: &str) {
        msgpack::write_array_len(&mut self.0, 2);
        msgpack::write_str(&mut self.0, qualname);
    }

    /// A dataclass under its tag, which is how it travels at the top of a value or inside a union.
    pub fn tagged(&mut self, qualname: &str, fields: usize) {
        self.tag(qualname);
        self.fields(fields);
    }

    /// A dataclass under its tag whose first two fields, of the `fields` it has, name the entity it concerns.
    pub fn entity(&mut self, qualname: &str, fields: usize, actor: &str, key: &str) {
        self.tagged(qualname, fields);
        self.name("actor");
        self.text(actor);
        self.name("key");
        self.text(key);
    }

    /// A dataclass as a field of another one, which carries no tag.
    pub fn fields(&mut self, count: usize) {
        msgpack::write_map_len(&mut self.0, count);
    }

    /// `T | None`, where `write` writes the dataclass `T` as a field. A union, so the dataclass goes under its tag.
    pub fn optional<T>(
        &mut self,
        qualname: &str,
        value: Option<&T>,
        write: impl FnOnce(&mut Self, &T),
    ) {
        match value {
            Some(value) => {
                self.tag(qualname);
                write(self, value);
            }
            None => self.nil(),
        }
    }

    pub fn name(&mut self, name: &str) {
        msgpack::write_str(&mut self.0, name);
    }

    pub fn nil(&mut self) {
        msgpack::write_nil(&mut self.0);
    }

    pub fn bool(&mut self, value: bool) {
        msgpack::write_bool(&mut self.0, value);
    }

    pub fn int(&mut self, value: i64) {
        msgpack::write_int(&mut self.0, Int::Signed(value));
    }

    pub fn unsigned(&mut self, value: u64) {
        msgpack::write_int(&mut self.0, Int::Unsigned(value));
    }

    pub fn float(&mut self, value: f64) {
        msgpack::write_f64(&mut self.0, value);
    }

    pub fn text(&mut self, value: &str) {
        msgpack::write_str(&mut self.0, value);
    }

    pub fn bytes(&mut self, value: &[u8]) {
        msgpack::write_bin(&mut self.0, value);
    }

    pub fn items(&mut self, count: usize) {
        msgpack::write_array_len(&mut self.0, count);
    }

    /// A `str | None`, which is how an address travels.
    pub fn address(&mut self, value: Option<&str>) {
        match value {
            None => self.nil(),
            Some(address) => self.text(address),
        }
    }

    /// `NodeId` as a field: a map of the address and the sixteen bytes of the incarnation.
    pub fn node(&mut self, node: &NodeId) {
        self.fields(2);
        self.name("address");
        self.address(node.address.as_deref());
        self.name("incarnation");
        self.bytes(&node.incarnation);
    }

    /// `NodeId` as an alternative of a union, which carries its tag.
    /// A ref, as the schema writes one: where it points and how to reach it.
    pub fn target(&mut self, target: &Target) {
        match target {
            Target::Entity { actor, key } => {
                self.items(3);
                self.text("e");
                self.text(actor);
                self.text(key);
            }
            Target::Reply { node, id } => {
                self.items(4);
                self.text("r");
                self.address(node.address.as_deref());
                self.bytes(&node.incarnation);
                self.int(*id);
            }
        }
    }

    pub fn tagged_node(&mut self, node: &NodeId) {
        self.tagged("NodeId", 2);
        self.name("address");
        self.address(node.address.as_deref());
        self.name("incarnation");
        self.bytes(&node.incarnation);
    }

    /// A mapping as the schema writes it: a list of pairs, so that keys are not limited to strings.
    pub fn pairs(&mut self, count: usize) {
        msgpack::write_array_len(&mut self.0, count);
    }

    pub fn pair(&mut self) {
        msgpack::write_array_len(&mut self.0, 2);
    }
}

/// A value being read back, field by field.
#[derive(Debug)]
pub struct Reading<'a>(Reader<'a>);

impl<'a> Reading<'a> {
    #[must_use]
    pub fn new(data: &'a [u8]) -> Self {
        Self(Reader::new(data))
    }

    /// The tag of a dataclass, which the map of its fields follows.
    pub fn tag(&mut self) -> Result<&'a str> {
        if self.0.read_array_len()? != 2 {
            return Err(Malformed::Marker(0));
        }
        self.0.read_str()
    }

    /// The tag of a dataclass and how many fields follow it.
    pub fn tagged(&mut self) -> Result<(String, usize)> {
        let tag = self.tag()?.to_owned();
        Ok((tag, self.fields()?))
    }

    pub fn fields(&mut self) -> Result<usize> {
        self.0.read_map_len()
    }

    pub fn name(&mut self) -> Result<&'a str> {
        self.0.read_str()
    }

    pub fn bool(&mut self) -> Result<bool> {
        self.0.read_bool()
    }

    pub fn int(&mut self) -> Result<i64> {
        match self.0.read_int()? {
            Int::Signed(value) => Ok(value),
            Int::Unsigned(value) => i64::try_from(value).map_err(|_| Malformed::Marker(0)),
        }
    }

    pub fn unsigned(&mut self) -> Result<u64> {
        match self.0.read_int()? {
            Int::Unsigned(value) => Ok(value),
            Int::Signed(value) => u64::try_from(value).map_err(|_| Malformed::Marker(0)),
        }
    }

    pub fn float(&mut self) -> Result<f64> {
        self.0.read_f64()
    }

    pub fn text(&mut self) -> Result<String> {
        Ok(self.0.read_str()?.to_owned())
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        Ok(self.0.read_bin()?.to_vec())
    }

    pub fn items(&mut self) -> Result<usize> {
        self.0.read_array_len()
    }

    /// Whether the next value is nothing, which is how a `tell` says it waits for no answer. It is taken when it is.
    pub fn nil(&mut self) -> Result<bool> {
        if self.0.kind()? == Kind::None {
            self.0.read_nil()?;
            return Ok(true);
        }
        Ok(false)
    }

    /// `T | None`, where `read` reads the dataclass `T` as a field. The tag it travels under is stepped over.
    pub fn optional<T>(&mut self, read: impl FnOnce(&mut Self) -> Result<T>) -> Result<Option<T>> {
        if self.nil()? {
            return Ok(None);
        }
        self.tag()?;
        read(self).map(Some)
    }

    pub fn address(&mut self) -> Result<Option<String>> {
        match self.0.kind()? {
            Kind::None => {
                self.0.read_nil()?;
                Ok(None)
            }
            _ => Ok(Some(self.text()?)),
        }
    }

    /// `NodeId` as a field, whatever order its two fields were written in.
    pub fn node(&mut self) -> Result<NodeId> {
        let count = self.fields()?;
        let mut address = None;
        let mut incarnation = None;
        for _ in 0..count {
            match self.name()? {
                "address" => address = self.address()?,
                "incarnation" => incarnation = Some(self.bytes()?),
                _ => self.skip()?,
            }
        }
        let incarnation = incarnation.ok_or(Malformed::Truncated)?;
        Ok(NodeId {
            address,
            incarnation: incarnation.try_into().map_err(|_| Malformed::Truncated)?,
        })
    }

    /// A ref, as the schema writes one.
    pub fn target(&mut self) -> Result<Target> {
        let held = self.items()?;
        match (self.text()?.as_str(), held) {
            ("e", 3) => Ok(Target::Entity {
                actor: self.text()?,
                key: self.text()?,
            }),
            ("r", 4) => {
                let address = self.address()?;
                let incarnation = <[u8; 16]>::try_from(self.bytes()?.as_slice())
                    .map_err(|_| Malformed::Truncated)?;
                Ok(Target::Reply {
                    node: NodeId {
                        address,
                        incarnation,
                    },
                    id: self.int()?,
                })
            }
            _ => Err(Malformed::Truncated),
        }
    }

    pub fn skip(&mut self) -> Result<()> {
        self.0.skip()
    }

    /// Whether the payload has been read to the end, which says a message held nothing extra.
    #[must_use]
    pub fn done(&self) -> bool {
        self.0.done()
    }
}

#[cfg(test)]
mod tests {
    use super::{Reading, Writer};
    use crate::node::NodeId;

    fn node(address: Option<&str>, tag: u8) -> NodeId {
        NodeId {
            address: address.map(str::to_owned),
            incarnation: [tag; 16],
        }
    }

    #[test]
    fn a_tagged_dataclass_reads_back_as_it_was_written() {
        let mut writer = Writer::new();
        writer.tagged("Record", 4);
        writer.name("node");
        writer.node(&node(Some("127.0.0.1:7400"), 3));
        writer.name("incarnation");
        writer.unsigned(7);
        writer.name("status");
        writer.text("alive");
        writer.name("types");
        writer.items(2);
        writer.text("account");
        writer.text("order");
        let written = writer.finish();

        let mut reading = Reading::new(&written);
        let (tag, fields) = reading.tagged().unwrap();

        assert_eq!((tag.as_str(), fields), ("Record", 4));
        assert_eq!(reading.name().unwrap(), "node");
        assert_eq!(reading.node().unwrap(), node(Some("127.0.0.1:7400"), 3));
        assert_eq!(reading.name().unwrap(), "incarnation");
        assert_eq!(reading.unsigned().unwrap(), 7);
        assert_eq!(reading.name().unwrap(), "status");
        assert_eq!(reading.text().unwrap(), "alive");
        assert_eq!(reading.name().unwrap(), "types");
        assert_eq!(reading.items().unwrap(), 2);
        assert_eq!(reading.text().unwrap(), "account");
        assert_eq!(reading.text().unwrap(), "order");
        assert!(reading.done());
    }

    #[test]
    fn a_field_this_version_does_not_know_is_stepped_over() {
        let mut writer = Writer::new();
        writer.fields(3);
        writer.name("address");
        writer.address(None);
        writer.name("tags");
        writer.items(1);
        writer.text("x");
        writer.name("incarnation");
        writer.bytes(&[9; 16]);
        let written = writer.finish();

        let mut reading = Reading::new(&written);

        assert_eq!(reading.node().unwrap(), node(None, 9));
        assert!(reading.done());
    }
}
