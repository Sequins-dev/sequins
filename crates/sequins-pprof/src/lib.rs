//! sequins-pprof — pprof binary format parser for Sequins
//!
//! Isolates the `pprof`, `prost`, and `prost-types` dependencies so that
//! crates that don't need profile parsing don't pull in those heavy deps.

pub mod pprof_parser;
