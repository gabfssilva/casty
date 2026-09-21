//! The wire between nodes: frames, streams, the handshake and the compression they negotiate.
//!
//! Nothing here knows what an envelope carries. The format is the one the Python implementation writes, because a
//! node of each forms one cluster while the port lasts.

pub mod compress;
pub mod connection;
pub mod endpoint;
pub mod frame;
pub mod handshake;
pub mod limits;
pub mod mux;
pub mod pool;
pub mod tls;
