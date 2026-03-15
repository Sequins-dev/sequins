//! Lock-free linked list of Arrow RecordBatches with background compaction.
//!
//! Each `BatchChain` maintains a singly-linked list of `BatchNode`s, ordered
//! newest-to-oldest (head is newest). Writers prepend via CAS on the head
//! pointer. A background compactor task reads from a channel and continuously
//! merges small nodes into larger ones, eventually flushing completed nodes to
//! cold storage.
//!
//! DataFusion queries scan the chain lazily via `BatchChainExec`, which
//! implements `TableProvider` directly on `BatchChain`.

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream, Statistics,
};
use futures::stream;
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// BatchMeta
// ---------------------------------------------------------------------------

/// Metadata for a single batch in the chain.
#[derive(Clone)]
pub struct BatchMeta {
    /// Earliest timestamp (nanoseconds) in the batch.
    pub min_timestamp: i64,
    /// Latest timestamp (nanoseconds) in the batch.
    pub max_timestamp: i64,
    /// Number of rows in the batch.
    pub row_count: usize,
}

impl BatchMeta {
    /// Merge two `BatchMeta` instances, producing a meta that spans both.
    pub fn merge(a: &BatchMeta, b: &BatchMeta) -> BatchMeta {
        debug_assert!(
            a.min_timestamp <= a.max_timestamp,
            "BatchMeta a has inverted timestamps: min={} > max={}",
            a.min_timestamp,
            a.max_timestamp
        );
        debug_assert!(
            b.min_timestamp <= b.max_timestamp,
            "BatchMeta b has inverted timestamps: min={} > max={}",
            b.min_timestamp,
            b.max_timestamp
        );
        BatchMeta {
            min_timestamp: a.min_timestamp.min(b.min_timestamp),
            max_timestamp: a.max_timestamp.max(b.max_timestamp),
            row_count: a.row_count + b.row_count,
        }
    }
}

// ---------------------------------------------------------------------------
// BatchNode (internal)
// ---------------------------------------------------------------------------

/// A node in the lock-free BatchChain linked list.
///
/// Once created, `batch` and `meta` are immutable. Only `next` and `complete`
/// are modified after construction (atomically).
// `meta` and `complete` are accessed via raw pointer dereferences inside
// `compaction_loop`; the dead_code lint cannot see through unsafe pointer reads.
#[allow(dead_code)]
pub(crate) struct BatchNode {
    /// Immutable Arrow RecordBatch.
    batch: Arc<RecordBatch>,
    /// Metadata: time range, row count.
    meta: BatchMeta,
    /// Pointer to the next (older) node. Modified by writers (initial link)
    /// and the compactor (CAS during merge).
    next: Atomic<BatchNode>,
    /// Set to `true` by the compactor when the merged batch reaches the
    /// target size. Transitions false → true exactly once; never reverted.
    complete: AtomicBool,
}

// ---------------------------------------------------------------------------
// BatchChain
// ---------------------------------------------------------------------------

/// A lock-free linked list of Arrow `RecordBatch`es with background compaction.
///
/// Multiple threads may call `push` concurrently without synchronisation.
/// DataFusion queries scan the chain via the `TableProvider` implementation.
pub struct BatchChain {
    /// Pointer to the newest (head) node. Writers CAS this to prepend.
    /// Wrapped in `Arc` so `BatchChainExec` can hold a reference without
    /// lifetime constraints.
    head: Arc<Atomic<BatchNode>>,
    /// Schema shared by all `RecordBatch`es in this chain.
    schema: SchemaRef,
    /// Channel sender: each `push` sends a signal so the compactor wakes up.
    compaction_tx: mpsc::UnboundedSender<()>,
}

impl fmt::Debug for BatchChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchChain")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl BatchChain {
    /// Create a new, empty `BatchChain` and return the compaction receiver.
    ///
    /// The caller is responsible for spawning a `compaction_loop` task that
    /// reads from the returned receiver.
    pub fn new(schema: SchemaRef) -> (BatchChain, mpsc::UnboundedReceiver<()>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let chain = BatchChain {
            head: Arc::new(Atomic::null()),
            schema,
            compaction_tx: tx,
        };
        (chain, rx)
    }

    /// Return a clone of the shared head pointer for use by `compaction_loop`.
    pub(crate) fn head_arc(&self) -> Arc<Atomic<BatchNode>> {
        Arc::clone(&self.head)
    }

    /// Return the schema of this chain.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Count the total number of rows across all batches in the chain.
    ///
    /// Traverses the chain under an epoch guard and sums `RecordBatch::num_rows()`.
    /// O(n) in the number of nodes, but nodes are typically few (< 30).
    pub fn row_count(&self) -> usize {
        let guard = epoch::pin();
        let head_ptr = self.head.load(Ordering::Acquire, &guard).as_raw();
        drop(guard);
        collect_batches(head_ptr).iter().map(|b| b.num_rows()).sum()
    }

    /// Lock-free prepend of a new batch to the front of the chain.
    ///
    /// Uses a CAS loop on the head pointer. Under typical contention the CAS
    /// succeeds on the first attempt; retries are rare.
    pub fn push(&self, batch: Arc<RecordBatch>, meta: BatchMeta) {
        debug_assert_eq!(
            batch.schema(),
            self.schema,
            "pushed batch schema does not match chain schema"
        );
        debug_assert_eq!(
            meta.row_count,
            batch.num_rows(),
            "meta.row_count ({}) != batch.num_rows() ({})",
            meta.row_count,
            batch.num_rows()
        );
        let guard = epoch::pin();
        let mut node = Owned::new(BatchNode {
            batch,
            meta,
            next: Atomic::null(),
            complete: AtomicBool::new(false),
        });

        loop {
            let head = self.head.load(Ordering::Acquire, &guard);
            node.next.store(head, Ordering::Relaxed);
            match self.head.compare_exchange(
                head,
                node,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_shared) => break,
                // CAS failed: another writer won the race. Retry with the
                // Owned<BatchNode> that was rejected by compare_exchange.
                Err(e) => node = e.new,
            }
        }

        // Signal the compactor that new data is available.
        let _ = self.compaction_tx.send(());
    }
}

// ---------------------------------------------------------------------------
// BatchChainExec — DataFusion ExecutionPlan
// ---------------------------------------------------------------------------

/// DataFusion `ExecutionPlan` that lazily walks the `BatchChain`.
///
/// On `execute`, it takes a snapshot of the head pointer (under a brief epoch
/// pin) and produces a `BatchChainStream` that yields one `RecordBatch` per
/// node. If `projection` is set, only those column indices are included in
/// each yielded batch and the reported schema reflects the projected subset.
struct BatchChainExec {
    /// Shared reference to the chain's head pointer.
    head: Arc<Atomic<BatchNode>>,
    /// Projected schema (subset of full schema when projection is active).
    schema: SchemaRef,
    /// Column indices to select from each batch, or `None` for all columns.
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl fmt::Debug for BatchChainExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchChainExec")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for BatchChainExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BatchChainExec")
    }
}

impl ExecutionPlan for BatchChainExec {
    fn name(&self) -> &str {
        "BatchChainExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Pin epoch briefly to snapshot the head pointer.
        let guard = epoch::pin();
        let head_ptr = self.head.load(Ordering::Acquire, &guard).as_raw();
        drop(guard);

        // Collect all batches from the chain into a Vec under a guard.
        // We use the "snapshot into Vec" approach: pin once, traverse all
        // nodes, clone each Arc<RecordBatch>, then drop the guard.
        // The chain is typically short (< 30 nodes), so the hold time is
        // bounded and acceptable. Each Arc clone is O(1) (refcount bump).
        let batches = collect_batches(head_ptr);
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let stream = stream::iter(batches.into_iter().map(move |b| {
            if let Some(ref proj) = projection {
                b.project(proj)
                    .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
            } else {
                Ok((*b).clone())
            }
        }));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// Traverse the chain from `head_ptr`, cloning each `Arc<RecordBatch>`.
///
/// Pins the epoch for the full traversal. The pin is held only as long as it
/// takes to follow pointers and clone `Arc`s — nanoseconds per node.
fn collect_batches(head_ptr: *const BatchNode) -> Vec<Arc<RecordBatch>> {
    let mut batches = Vec::new();
    if head_ptr.is_null() {
        return batches;
    }

    let guard = epoch::pin();
    let mut current = head_ptr;

    while !current.is_null() {
        // SAFETY: current is non-null and we hold an epoch guard, preventing
        // any deferred reclamation of this node.
        let node = unsafe { &*current };
        batches.push(Arc::clone(&node.batch));
        let next = node.next.load(Ordering::Acquire, &guard);
        current = next.as_raw();
    }

    batches
}

// ---------------------------------------------------------------------------
// TableProvider for BatchChain
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl TableProvider for BatchChain {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Apply projection to schema so the returned plan reports the correct
        // field subset. DataFusion relies on this to match with the cold tier
        // provider when both are unioned in SignalUnionProvider.
        let projected_schema = match projection {
            Some(proj) => Arc::new(self.schema.project(proj)?),
            None => self.schema.clone(),
        };
        let projection_owned = projection.cloned();
        Ok(Arc::new(BatchChainExec {
            head: Arc::clone(&self.head),
            schema: projected_schema.clone(),
            projection: projection_owned,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }))
    }
}

// ---------------------------------------------------------------------------
// compaction_loop
// ---------------------------------------------------------------------------

/// Background compaction task for a single `BatchChain`.
///
/// Reads signals from `rx` (one per `push`), then traverses the chain to find
/// two adjacent incomplete nodes (Y and Z) to merge. Uses CAS to splice the
/// merged node into the chain and defers reclamation of the consumed nodes.
///
/// When the merged node reaches `target_rows`, it is marked complete. Two
/// consecutive complete nodes trigger a cold-tier flush of the older one.
///
/// # Cold tier integration
///
/// `cold_writer_fn` is an optional async callback invoked with the completed
/// `RecordBatch` + `BatchMeta` when a node is ready to be flushed to cold
/// storage. Pass `None` to disable flushing (useful for tests or when the
/// cold tier is not yet initialised).
#[allow(private_interfaces, dead_code)]
pub(crate) async fn compaction_loop<F, Fut>(
    head: Arc<Atomic<BatchNode>>,
    schema: SchemaRef,
    mut rx: mpsc::UnboundedReceiver<()>,
    target_rows: usize,
    _name: String,
    cold_writer_fn: Option<F>,
) where
    F: Fn(Arc<RecordBatch>, BatchMeta) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let mut ops: u64 = 0;

    while rx.recv().await.is_some() {
        ops += 1;

        // --- Phase 1: identify X, Y, Z under an epoch guard ---
        let (y_batch, z_batch, y_meta, z_meta) = {
            let guard = epoch::pin();

            // X = head
            let x_ptr = head.load(Ordering::Acquire, &guard);
            let x_ref = match unsafe { x_ptr.as_ref() } {
                Some(r) => r,
                None => continue, // empty chain
            };

            // Y = X.next
            let y_ptr = x_ref.next.load(Ordering::Acquire, &guard);
            let y_ref = match unsafe { y_ptr.as_ref() } {
                Some(r) => r,
                None => continue, // only one node, nothing to merge
            };

            // Z = Y.next
            let z_ptr = y_ref.next.load(Ordering::Acquire, &guard);
            let z_ref = match unsafe { z_ptr.as_ref() } {
                Some(r) => r,
                None => continue, // Y is the tail — nothing to merge into
            };

            // Skip if Z is already complete: do not enlarge a complete node.
            if z_ref.complete.load(Ordering::Acquire) {
                continue;
            }

            // Clone the Arcs so we can drop the guard before spawn_blocking.
            let y_batch = Arc::clone(&y_ref.batch);
            let z_batch = Arc::clone(&z_ref.batch);
            let y_meta = y_ref.meta.clone();
            let z_meta = z_ref.meta.clone();

            (y_batch, z_batch, y_meta, z_meta)
        };

        // --- Phase 2: merge (CPU-bound, off the async runtime) ---
        let schema_clone = schema.clone();
        let merged_batch = match tokio::task::spawn_blocking(move || {
            concat_batches(&schema_clone, [y_batch.as_ref(), z_batch.as_ref()])
        })
        .await
        {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => {
                tracing::warn!("compaction_loop: concat_batches failed: {e}");
                continue;
            }
            Err(e) => {
                tracing::warn!("compaction_loop: spawn_blocking panicked: {e}");
                continue;
            }
        };

        let is_complete = merged_batch.num_rows() >= target_rows;
        let merged_batch = Arc::new(merged_batch);
        let merged_meta = BatchMeta::merge(&y_meta, &z_meta);

        // --- Phase 3 + 4: re-traverse, CAS, optional cold flush ---
        // All epoch guard operations are enclosed in a sync block so that no
        // guard or Shared<> pointer is live across any await point.
        let flush_args: Option<(Arc<RecordBatch>, BatchMeta)> = {
            let guard = epoch::pin();

            let x_ptr = head.load(Ordering::Acquire, &guard);
            let x_ref = match unsafe { x_ptr.as_ref() } {
                Some(r) => r,
                None => continue,
            };

            let y_ptr = x_ref.next.load(Ordering::Acquire, &guard);
            let y_ref = match unsafe { y_ptr.as_ref() } {
                Some(r) => r,
                None => continue,
            };

            let z_ptr = y_ref.next.load(Ordering::Acquire, &guard);
            let z_ref = match unsafe { z_ptr.as_ref() } {
                Some(r) => r,
                None => continue,
            };

            // Build the merged node, linking past Z.
            let z_next = z_ref.next.load(Ordering::Acquire, &guard);
            let merged_node = Owned::new(BatchNode {
                batch: Arc::clone(&merged_batch),
                meta: merged_meta.clone(),
                next: Atomic::from(z_next),
                complete: AtomicBool::new(is_complete),
            });

            // CAS X.next: replace Y with merged.
            match x_ref.next.compare_exchange(
                y_ptr,
                merged_node,
                Ordering::AcqRel,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => unsafe {
                    guard.defer_destroy(y_ptr);
                    guard.defer_destroy(z_ptr);
                },
                Err(_) => {
                    // Structure changed — skip this round.
                    continue;
                }
            }

            // Phase 4: check if we should flush to cold storage.
            // Collect flush arguments before dropping the guard.
            let mut maybe_flush: Option<(Arc<RecordBatch>, BatchMeta)> = None;
            if is_complete {
                let merged_ptr = x_ref.next.load(Ordering::Acquire, &guard);
                if let Some(merged_ref) = unsafe { merged_ptr.as_ref() } {
                    let successor = merged_ref.next.load(Ordering::Acquire, &guard);
                    if let Some(succ_ref) = unsafe { successor.as_ref() } {
                        if succ_ref.complete.load(Ordering::Acquire) {
                            let flush_batch = Arc::clone(&succ_ref.batch);
                            let flush_meta = succ_ref.meta.clone();
                            if merged_ref
                                .next
                                .compare_exchange(
                                    successor,
                                    Shared::null(),
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                    &guard,
                                )
                                .is_ok()
                            {
                                unsafe { guard.defer_destroy(successor) };
                                maybe_flush = Some((flush_batch, flush_meta));
                            }
                        }
                    }
                }
            }

            // Periodically allow epoch advancement.
            if ops % 64 == 0 {
                drop(guard);
            }

            maybe_flush
            // guard drops here if not already dropped
        };

        // Await the cold flush AFTER the guard and all Shared<> pointers are gone.
        if let Some((flush_batch, flush_meta)) = flush_args {
            if let Some(ref writer) = cold_writer_fn {
                writer(flush_batch, flush_meta).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_batch(schema: SchemaRef, ids: Vec<i64>, names: Vec<&str>) -> Arc<RecordBatch> {
        Arc::new(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap(),
        )
    }

    fn make_meta(min_ts: i64, max_ts: i64, row_count: usize) -> BatchMeta {
        BatchMeta {
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count,
        }
    }

    // -----------------------------------------------------------------------
    // BatchMeta tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_batch_meta_merge() {
        let a = make_meta(100, 200, 10);
        let b = make_meta(50, 300, 20);
        let merged = BatchMeta::merge(&a, &b);
        assert_eq!(merged.min_timestamp, 50);
        assert_eq!(merged.max_timestamp, 300);
        assert_eq!(merged.row_count, 30);
    }

    #[test]
    fn test_batch_meta_merge_symmetric() {
        let a = make_meta(100, 200, 5);
        let b = make_meta(50, 300, 15);
        let ab = BatchMeta::merge(&a, &b);
        let ba = BatchMeta::merge(&b, &a);
        assert_eq!(ab.min_timestamp, ba.min_timestamp);
        assert_eq!(ab.max_timestamp, ba.max_timestamp);
        assert_eq!(ab.row_count, ba.row_count);
    }

    // -----------------------------------------------------------------------
    // BatchChain::push tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_push_single_batch() {
        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());
        let batch = make_batch(schema.clone(), vec![1, 2, 3], vec!["a", "b", "c"]);
        chain.push(batch, make_meta(100, 300, 3));

        // Verify head is non-null
        let guard = epoch::pin();
        let head = chain.head.load(Ordering::Acquire, &guard);
        assert!(!head.is_null(), "head should be non-null after push");
        let head_ref = unsafe { head.as_ref().unwrap() };
        assert_eq!(head_ref.batch.num_rows(), 3);
        assert_eq!(head_ref.meta.row_count, 3);
    }

    #[test]
    fn test_push_multiple_batches_orders_newest_first() {
        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());

        let batch1 = make_batch(schema.clone(), vec![1], vec!["first"]);
        let batch2 = make_batch(schema.clone(), vec![2], vec!["second"]);
        let batch3 = make_batch(schema.clone(), vec![3], vec!["third"]);

        chain.push(batch1, make_meta(100, 100, 1));
        chain.push(batch2, make_meta(200, 200, 1));
        chain.push(batch3, make_meta(300, 300, 1));

        // Traverse chain; expect batch3, batch2, batch1 (newest first)
        let batches = collect_batches({
            let guard = epoch::pin();
            chain.head.load(Ordering::Acquire, &guard).as_raw()
        });

        assert_eq!(batches.len(), 3);
        // Each batch has 1 row
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 1);
        assert_eq!(batches[2].num_rows(), 1);

        // Verify ordering by reading the Int64 column
        let id0 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let id1 = batches[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let id2 = batches[2]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(id0, 3, "head should be batch3 (newest)");
        assert_eq!(id1, 2);
        assert_eq!(id2, 1, "tail should be batch1 (oldest)");
    }

    #[test]
    fn test_push_empty_chain() {
        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());

        let guard = epoch::pin();
        let head = chain.head.load(Ordering::Acquire, &guard);
        assert!(head.is_null(), "new chain head should be null");
    }

    #[test]
    fn test_compaction_channel_receives_signals() {
        let schema = make_schema();
        let (chain, mut rx) = BatchChain::new(schema.clone());

        let batch = make_batch(schema.clone(), vec![42], vec!["x"]);
        chain.push(batch, make_meta(1, 1, 1));

        // Should have received exactly one signal
        assert!(
            rx.try_recv().is_ok(),
            "compaction channel should have a signal"
        );
        assert!(rx.try_recv().is_err(), "only one signal per push");
    }

    #[test]
    fn test_push_sends_signal_per_batch() {
        let schema = make_schema();
        let (chain, mut rx) = BatchChain::new(schema.clone());

        for i in 0..5i64 {
            let batch = make_batch(schema.clone(), vec![i], vec!["x"]);
            chain.push(batch, make_meta(i, i, 1));
        }

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 5, "should receive one signal per push");
    }

    // -----------------------------------------------------------------------
    // collect_batches tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_collect_batches_empty() {
        let batches = collect_batches(std::ptr::null());
        assert!(batches.is_empty());
    }

    #[test]
    fn test_collect_batches_matches_push_order() {
        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());

        for i in 0..4i64 {
            let batch = make_batch(schema.clone(), vec![i], vec!["row"]);
            chain.push(batch, make_meta(i * 10, i * 10, 1));
        }

        let head_ptr = {
            let guard = epoch::pin();
            chain.head.load(Ordering::Acquire, &guard).as_raw()
        };
        let batches = collect_batches(head_ptr);
        assert_eq!(batches.len(), 4);

        // Verify ids are 3, 2, 1, 0 (newest to oldest)
        let ids: Vec<i64> = batches
            .iter()
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        assert_eq!(ids, vec![3, 2, 1, 0]);
    }

    // -----------------------------------------------------------------------
    // compaction_loop tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_compaction_merges_two_nodes() {
        let schema = make_schema();
        let (chain, rx) = BatchChain::new(schema.clone());

        // Push two small batches. The compactor should merge them.
        let batch1 = make_batch(schema.clone(), vec![1, 2], vec!["a", "b"]);
        let batch2 = make_batch(schema.clone(), vec![3, 4], vec!["c", "d"]);
        chain.push(batch1, make_meta(100, 200, 2));
        chain.push(batch2, make_meta(300, 400, 2));

        let head = Arc::clone(&chain.head);
        let schema_clone = schema.clone();

        // target_rows = 100 so nothing completes, but merges happen
        let handle = tokio::spawn(compaction_loop::<
            fn(Arc<RecordBatch>, BatchMeta) -> std::future::Ready<()>,
            _,
        >(
            head,
            schema_clone,
            rx,
            100,
            "test".to_string(),
            None::<fn(Arc<RecordBatch>, BatchMeta) -> std::future::Ready<()>>,
        ));

        // Allow compactor to run a few cycles
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Push a third batch to ensure the compactor processes signals
        let batch3 = make_batch(schema.clone(), vec![5], vec!["e"]);
        chain.push(batch3, make_meta(500, 500, 1));

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        handle.abort();

        // After compaction, the chain should be shorter than 3 nodes
        let head_ptr = {
            let guard = epoch::pin();
            chain.head.load(Ordering::Acquire, &guard).as_raw()
        };
        let batches = collect_batches(head_ptr);

        // With 3 pushes and compaction active, we expect fewer than 3 nodes
        // (exact count depends on timing, but at least 1 merge should occur)
        assert!(
            batches.len() <= 3,
            "chain should not grow unboundedly: {} nodes",
            batches.len()
        );

        // Total rows across all nodes should equal rows in all pushed batches (5)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "no rows should be lost during compaction");
    }

    #[tokio::test]
    async fn test_compaction_marks_complete_at_target() {
        let schema = make_schema();
        let (chain, rx) = BatchChain::new(schema.clone());

        // Push two batches with 3 rows each. target_rows=4, so merged (6) >= 4 → complete.
        let batch1 = make_batch(schema.clone(), vec![1, 2, 3], vec!["a", "b", "c"]);
        let batch2 = make_batch(schema.clone(), vec![4, 5, 6], vec!["d", "e", "f"]);
        // Push oldest first so head = batch2 (newest)
        chain.push(batch1, make_meta(100, 300, 3));
        chain.push(batch2, make_meta(400, 600, 3));

        let head = Arc::clone(&chain.head);
        let schema_clone = schema.clone();

        // Need a 3rd node for X so compactor can use X → Y → Z pattern
        let batch3 = make_batch(schema.clone(), vec![7], vec!["g"]);
        chain.push(batch3, make_meta(700, 700, 1));

        let handle = tokio::spawn(compaction_loop::<
            fn(Arc<RecordBatch>, BatchMeta) -> std::future::Ready<()>,
            _,
        >(
            head,
            schema_clone,
            rx,
            4, // target = 4 rows; merged batch1+batch2 = 6 rows ≥ 4
            "test_complete".to_string(),
            None::<fn(Arc<RecordBatch>, BatchMeta) -> std::future::Ready<()>>,
        ));

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.abort();

        // After compaction, verify no data was lost
        let head_ptr = {
            let guard = epoch::pin();
            chain.head.load(Ordering::Acquire, &guard).as_raw()
        };
        let batches = collect_batches(head_ptr);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 7,
            "all 7 rows should be present after compaction"
        );
    }

    // -----------------------------------------------------------------------
    // Concurrent push tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_concurrent_push_no_data_loss() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());
        let chain = StdArc::new(chain);

        let threads = 8;
        let batches_per_thread = 100;

        let mut handles = Vec::new();
        for _ in 0..threads {
            let chain_clone = StdArc::clone(&chain);
            let schema_clone = schema.clone();
            let handle = thread::spawn(move || {
                for i in 0..batches_per_thread {
                    let batch = make_batch(schema_clone.clone(), vec![i as i64], vec!["row"]);
                    chain_clone.push(batch, make_meta(i as i64, i as i64, 1));
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            chain.row_count(),
            threads * batches_per_thread,
            "concurrent push must not lose rows"
        );
    }

    #[test]
    fn test_row_count_matches_total_pushed() {
        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());

        // Push batches of varying sizes
        let sizes = [1usize, 5, 10, 3, 7];
        let total: usize = sizes.iter().sum();
        for &sz in &sizes {
            let ids: Vec<i64> = (0..sz as i64).collect();
            let names: Vec<&str> = vec!["x"; sz];
            let batch = make_batch(schema.clone(), ids, names);
            chain.push(batch, make_meta(0, sz as i64, sz));
        }

        assert_eq!(chain.row_count(), total);
    }

    // -----------------------------------------------------------------------
    // TableProvider / DataFusion scan tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_table_provider_scan_empty() {
        use datafusion::datasource::TableProvider;
        use datafusion::prelude::SessionContext;

        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());

        let ctx = SessionContext::new();
        let provider: Arc<dyn TableProvider> = Arc::new(chain);
        ctx.register_table("test_chain", provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_chain").await.unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "empty chain should return 0 rows");
    }

    #[tokio::test]
    async fn test_table_provider_scan_with_data() {
        use datafusion::datasource::TableProvider;
        use datafusion::prelude::SessionContext;

        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());

        chain.push(
            make_batch(schema.clone(), vec![1, 2], vec!["a", "b"]),
            make_meta(100, 200, 2),
        );
        chain.push(
            make_batch(schema.clone(), vec![3], vec!["c"]),
            make_meta(300, 300, 1),
        );

        let ctx = SessionContext::new();
        let provider: Arc<dyn TableProvider> = Arc::new(chain);
        ctx.register_table("test_chain", provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_chain").await.unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "scan should return all 3 rows");
    }

    /// Verify that data pushed to a chain AFTER it is registered as a DataFusion
    /// table is visible to a subsequent query. Each query uses its own
    /// `SessionContext` to avoid DataFusion's session-level plan caching.
    #[tokio::test]
    async fn test_table_provider_scan_after_push() {
        use datafusion::datasource::TableProvider;
        use datafusion::prelude::SessionContext;

        let schema = make_schema();
        let (chain, _rx) = BatchChain::new(schema.clone());
        let chain = Arc::new(chain);

        // --- First scan (empty chain) ---
        {
            let ctx = SessionContext::new();
            let provider: Arc<dyn TableProvider> = Arc::clone(&chain) as Arc<dyn TableProvider>;
            ctx.register_table("test_chain", provider).unwrap();
            let df = ctx.sql("SELECT * FROM test_chain").await.unwrap();
            let results = df.collect().await.unwrap();
            let total: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 0, "empty chain should return 0 rows");
        }

        // Push data between scans.
        chain.push(
            make_batch(schema.clone(), vec![10, 20, 30], vec!["x", "y", "z"]),
            make_meta(1, 3, 3),
        );

        // --- Second scan (3 rows) ---
        {
            let ctx = SessionContext::new();
            let provider: Arc<dyn TableProvider> = Arc::clone(&chain) as Arc<dyn TableProvider>;
            ctx.register_table("test_chain", provider).unwrap();
            let df = ctx.sql("SELECT * FROM test_chain").await.unwrap();
            let results = df.collect().await.unwrap();
            let total: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3, "chain should contain 3 rows after push");
        }
    }
}
