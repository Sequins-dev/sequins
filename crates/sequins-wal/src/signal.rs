/// WAL-local signal type — covers the four ingestable OTLP signal kinds.
///
/// This is intentionally separate from `sequins_query::ast::Signal` (which has
/// 14 variants and pulls in DataFusion) so that `sequins-wal` remains a
/// zero-internal-dependency crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalSignal {
    Traces,
    Logs,
    Metrics,
    Profiles,
}
