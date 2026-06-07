/// WAL-local signal type — covers the four ingestable OTLP signal kinds.
///
/// This is intentionally separate from `seql_ast::ast::Signal` (which has more
/// query-only variants) so that `sequins-wal` remains a
/// zero-internal-dependency crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalSignal {
    Traces,
    Logs,
    Metrics,
    Profiles,
}
