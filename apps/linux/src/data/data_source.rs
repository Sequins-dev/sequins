//! Data source abstraction — local storage and remote connection.
//!
//! Three types:
//!
//! - `LocalServer` — always-on background OTLP ingest server. Owns the
//!   `Storage` + `DataFusionBackend` and starts the OTLP gRPC/HTTP server on
//!   startup. Lives for the lifetime of the app regardless of which profile is
//!   selected.
//!
//! - `DataSource` — enum that dispatches queries to either the local in-process
//!   `DataFusionBackend` (Local variant) or a remote `RemoteClient` via Arrow
//!   Flight SQL gRPC (Remote variant). Mirrors the FFI crate's `DataSourceImpl`
//!   pattern without any C boundary.
//!
//! - `AppDataSource` — thin convenience wrapper over `DataSource` exposing the
//!   same public API that tab components already use (snapshot, snapshot_batches,
//!   live_view). Zero changes needed in tab components when switching profiles.

use anyhow::{Context, Result};
use sequins_client::RemoteClient;
use sequins_query::{QueryApi, SeqlStream};
use sequins_server::OtlpServer;
use sequins_storage::{DataFusionBackend, Storage, StorageConfig};
use sequins_view::{ViewDeltaStream, ViewStrategy};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

// ── OtlpPorts ─────────────────────────────────────────────────────────────────

/// Ports the OTLP server listens on.
#[derive(Debug, Clone, Copy)]
pub struct OtlpPorts {
    pub grpc: u16,
    pub http: u16,
}

impl Default for OtlpPorts {
    fn default() -> Self {
        Self {
            grpc: 4317,
            http: 4318,
        }
    }
}

// ── LocalServer ───────────────────────────────────────────────────────────────

/// Always-on local OTLP ingest server.
///
/// Owns `Arc<Storage>` and `Arc<DataFusionBackend>`. The gRPC and HTTP servers
/// are managed as independent tasks so either can be restarted without affecting
/// the other. Stored as `Arc<LocalServer>` so restart closures can hold a
/// reference without consuming the value.
pub struct LocalServer {
    pub(crate) storage: Arc<Storage>,
    backend: Arc<DataFusionBackend>,
    grpc_handle: Mutex<Option<JoinHandle<()>>>,
    http_handle: Mutex<Option<JoinHandle<()>>>,
}

impl LocalServer {
    /// Create storage and backend. Does NOT start any servers yet.
    pub async fn new(config: StorageConfig) -> Result<Self> {
        let storage = Arc::new(
            Storage::new(config)
                .await
                .context("Failed to create Storage")?,
        );
        let backend = Arc::new(DataFusionBackend::new(storage.clone()));
        Ok(Self {
            storage,
            backend,
            grpc_handle: Mutex::new(None),
            http_handle: Mutex::new(None),
        })
    }

    /// Start the gRPC server in the background and wait until it has bound the
    /// port before returning.
    pub async fn start_grpc(&self, port: u16) -> Result<u16> {
        let old = self.grpc_handle.lock().unwrap().take();
        if let Some(handle) = old {
            handle.abort();
            let _ = handle.await; // wait for the task to fully stop and release the port
        }

        let server = OtlpServer::new(self.storage.clone());
        let addr = format!("0.0.0.0:{port}");
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<std::result::Result<(), String>>();

        let handle = tokio::spawn(async move {
            if let Err(e) = server.serve_grpc_only(&addr, ready_tx).await {
                tracing::error!(error = %e, "gRPC OTLP server exited with error");
            }
        });

        tokio::task::spawn_blocking(move || {
            ready_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .context("gRPC server did not become ready within 5s")?
                .map_err(|e| anyhow::anyhow!("gRPC server failed to start: {}", e))
        })
        .await
        .context("spawn_blocking panicked")??;

        *self.grpc_handle.lock().unwrap() = Some(handle);
        Ok(port)
    }

    /// Start the HTTP server in the background and wait until it has bound the
    /// port before returning.
    pub async fn start_http(&self, port: u16) -> Result<u16> {
        let old = self.http_handle.lock().unwrap().take();
        if let Some(handle) = old {
            handle.abort();
            let _ = handle.await; // wait for the task to fully stop and release the port
        }

        let server = OtlpServer::new(self.storage.clone());
        let addr = format!("0.0.0.0:{port}");
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<std::result::Result<(), String>>();

        let handle = tokio::spawn(async move {
            if let Err(e) = server.serve_http_only(&addr, ready_tx).await {
                tracing::error!(error = %e, "HTTP OTLP server exited with error");
            }
        });

        tokio::task::spawn_blocking(move || {
            ready_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .context("HTTP server did not become ready within 5s")?
                .map_err(|e| anyhow::anyhow!("HTTP server failed to start: {}", e))
        })
        .await
        .context("spawn_blocking panicked")??;

        *self.http_handle.lock().unwrap() = Some(handle);
        Ok(port)
    }

    /// Convenience: start both gRPC and HTTP servers.
    pub async fn start_both(&self, ports: OtlpPorts) -> Result<OtlpPorts> {
        self.start_grpc(ports.grpc).await?;
        self.start_http(ports.http).await?;
        Ok(ports)
    }

    /// Return a reference to the shared backend for the local DataSource variant.
    pub fn backend(&self) -> Arc<DataFusionBackend> {
        self.backend.clone()
    }

    /// Abort both server tasks.
    pub fn stop(&self) {
        if let Some(h) = self.grpc_handle.lock().unwrap().take() {
            h.abort();
        }
        if let Some(h) = self.http_handle.lock().unwrap().take() {
            h.abort();
        }
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl std::fmt::Debug for LocalServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalServer").finish_non_exhaustive()
    }
}

// ── DataSource ────────────────────────────────────────────────────────────────

/// Unified query interface for local (in-process) and remote (Flight SQL) sources.
///
/// Mirrors `DataSourceImpl` from `sequins-ffi` but without a C boundary.
/// Both variants implement the same `query()` / `query_live()` dispatch.
pub enum DataSource {
    /// In-process queries via `DataFusionBackend` — shares `Arc<Storage>` with
    /// `LocalServer`, no network hop.
    Local { backend: Arc<DataFusionBackend> },
    /// Remote queries via Arrow Flight SQL gRPC.
    Remote { client: Arc<RemoteClient> },
}

impl DataSource {
    /// Create a local datasource from the always-on `LocalServer`.
    pub fn for_local(server: &LocalServer) -> Self {
        Self::Local {
            backend: server.backend(),
        }
    }

    /// Create a remote datasource connecting to `url` (e.g. `"http://host:4319"`).
    pub fn for_remote(url: &str) -> Result<Self> {
        let client = RemoteClient::new(url)
            .map_err(|e| anyhow::anyhow!("Failed to create remote client: {e}"))?;
        Ok(Self::Remote {
            client: Arc::new(client),
        })
    }

    /// Execute a snapshot SeQL query.
    pub async fn query(&self, seql: &str) -> Result<SeqlStream> {
        match self {
            Self::Local { backend } => backend
                .query(seql)
                .await
                .context("Local snapshot query failed"),
            Self::Remote { client } => client
                .query(seql)
                .await
                .context("Remote snapshot query failed"),
        }
    }

    /// Execute a live streaming SeQL query.
    pub async fn query_live(&self, seql: &str) -> Result<SeqlStream> {
        match self {
            Self::Local { backend } => backend
                .query_live(seql)
                .await
                .context("Local live query failed"),
            Self::Remote { client } => client
                .query_live(seql)
                .await
                .context("Remote live query failed"),
        }
    }
}

impl std::fmt::Debug for DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { .. } => write!(f, "DataSource::Local"),
            Self::Remote { .. } => write!(f, "DataSource::Remote"),
        }
    }
}

// ── AppDataSource ─────────────────────────────────────────────────────────────

/// Convenience wrapper over `DataSource` used by all tab components.
///
/// Provides the same public API regardless of whether the active profile is
/// local or remote. Tab components receive `Arc<AppDataSource>` and call
/// `snapshot()`, `snapshot_batches()`, or `live_view()` — zero changes needed
/// when switching profiles.
#[derive(Debug)]
pub struct AppDataSource {
    inner: DataSource,
}

impl AppDataSource {
    pub fn new(inner: DataSource) -> Self {
        Self { inner }
    }

    /// Execute a snapshot SeQL query, returning the raw `SeqlStream`.
    pub async fn snapshot(&self, seql: &str) -> Result<SeqlStream> {
        self.inner.query(seql).await
    }

    /// Execute a snapshot SeQL query and return all result IPC batches.
    pub async fn snapshot_batches(&self, seql: &str) -> Result<Vec<Vec<u8>>> {
        use futures::StreamExt;
        let stream = self.snapshot(seql).await?;
        futures::pin_mut!(stream);
        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            let fd = result.context("Snapshot stream error")?;
            if !fd.data_body.is_empty() {
                batches.push(fd.data_body.to_vec());
            }
        }
        Ok(batches)
    }

    /// Execute a live streaming SeQL query and apply a view strategy.
    pub async fn live_view(
        &self,
        seql: &str,
        strategy: &dyn ViewStrategy,
    ) -> Result<ViewDeltaStream> {
        let stream = self.inner.query_live(seql).await?;
        Ok(strategy.transform(stream).await)
    }
}
