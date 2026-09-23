//! The randomness the views need, and the seed a simulation needs to fail the same way twice.

use crate::node::NodeId;

#[derive(Debug)]
pub struct Rolls(u64);

impl Rolls {
    /// A generator seeded by the operating system, which is what a running node uses.
    #[must_use]
    pub fn fresh() -> Self {
        use std::hash::{BuildHasher, RandomState};
        Self::seeded(RandomState::new().hash_one(0_u8))
    }

    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    pub fn roll(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number in `0..limit`.
    pub fn upto(&mut self, limit: usize) -> usize {
        #[allow(clippy::cast_possible_truncation)]
        {
            (self.roll() % limit as u64) as usize
        }
    }

    #[cfg(test)]
    pub fn between(&mut self, low: usize, high: usize) -> usize {
        low + self.upto(high - low + 1)
    }

    /// Whether something of probability `chance` happens.
    #[cfg(test)]
    pub fn chance(&mut self, chance: f64) -> bool {
        #[allow(clippy::cast_precision_loss)]
        let drawn = (self.roll() >> 11) as f64 / (1_u64 << 53) as f64;
        drawn < chance
    }

    pub fn shuffled<T: Clone>(&mut self, held: &[T]) -> Vec<T> {
        let mut found = held.to_vec();
        for at in (1..found.len()).rev() {
            found.swap(at, self.upto(at + 1));
        }
        found
    }

    pub fn pick<'a, T>(&mut self, held: &'a [T]) -> &'a T {
        &held[self.upto(held.len())]
    }

    /// Nodes with distinct addresses and incarnations, which is what a cluster of a simulation is made of.
    pub fn nodes(&mut self, count: usize) -> Vec<NodeId> {
        (0..count)
            .map(|_| {
                let mut incarnation = [0_u8; 16];
                incarnation[..8].copy_from_slice(&self.roll().to_be_bytes());
                incarnation[8..].copy_from_slice(&self.roll().to_be_bytes());
                NodeId {
                    address: Some(format!("127.0.0.1:{}", 7400 + self.upto(20_000))),
                    incarnation,
                }
            })
            .collect()
    }
}
