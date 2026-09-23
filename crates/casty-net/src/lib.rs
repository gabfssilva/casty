//! The wire between nodes: frames, streams, the handshake and the compression they negotiate.
//!
//! Nothing here knows what an envelope carries. The frames, the envelopes inside them and the handshake are the wire
//! format, which every node of a cluster writes the same way.

pub mod compress;
pub mod connection;
pub mod endpoint;
pub mod frame;
pub mod handshake;
pub mod limits;
pub mod mux;
pub mod pool;
pub mod tls;
