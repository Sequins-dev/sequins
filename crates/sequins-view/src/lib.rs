//! Reactive view model layer for Sequins.
//!
//! Transforms raw [`SeqlStream`] data into entity-level [`ViewDelta`]s that
//! frontends apply directly to their `@Observable` graph — no full re-renders.
//!
//! # Architecture
//! ```text
//! SeqlStream (FlightData frames)
//!     ↓ ViewStrategy::transform()
//! ViewDeltaStream (ViewDelta items)
//!     ↓ FFI polling loop
//! CViewDelta callbacks → Swift @Observable mutations
//! ```
//!
//! # Strategies
//! - [`TableStrategy`] — logs, spans: append/expire row-level deltas
//! - [`AggregateStrategy`] — health: full-replace table deltas
//! - [`FlamegraphStrategy`] — profiles: entity-level incremental flamegraph
//!
//! # Usage
//! ```no_run
//! use sequins_view::{FlamegraphStrategy, ViewStrategy};
//! // stream: SeqlStream from DataFusionBackend::query_live(...)
//! // let delta_stream = FlamegraphStrategy::new(3_600_000_000_000).transform(stream).await;
//! ```

pub mod delta;
pub mod strategies;
pub mod strategy;

pub use delta::ViewDelta;
pub use strategies::aggregate::AggregateStrategy;
pub use strategies::flamegraph::FlamegraphStrategy;
pub use strategies::table::TableStrategy;
pub use strategy::{ViewDeltaStream, ViewStrategy};
