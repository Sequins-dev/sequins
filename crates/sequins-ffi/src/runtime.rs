use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

/// Global tokio runtime for all FFI async operations
///
/// All async FFI functions spawn tasks on this runtime.
/// Callbacks are invoked from tokio worker threads, so Swift must be fast.
pub static RUNTIME: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Failed to create tokio runtime"));
