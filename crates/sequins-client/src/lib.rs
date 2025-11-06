//! Remote client for Sequins Query and Management APIs
//!
//! This crate provides `RemoteClient` which implements `QueryApi` and `ManagementApi`
//! via HTTP calls to a remote Sequins daemon.
//!
//! # Architecture
//!
//! The client enables the **remote mode** deployment:
//! - Local mode: App uses `Storage` directly (implements all three traits)
//! - Remote mode: App uses `RemoteClient` for queries + management (HTTP to daemon)
//!
//! **Note:** OTLP ingestion is NOT done through RemoteClient. Applications send OTLP
//! data directly to the daemon's OTLP endpoints (ports 4317/4318).
//!
//! # Usage Example
//!
//! ```rust,no_run
//! use sequins_client::RemoteClient;
//! use sequins_core::traits::{QueryApi, ManagementApi};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Connect to remote daemon
//! let client = RemoteClient::new("http://localhost:8080", "http://localhost:8081")?;
//!
//! // Use QueryApi methods
//! let services = client.get_services().await?;
//!
//! // Use ManagementApi methods
//! let stats = client.get_storage_stats().await?;
//! # Ok(())
//! # }
//! ```

mod error;

pub use error::{Error, Result};

use reqwest::Client;
use sequins_core::{
    models::{
        LogEntry, LogId, LogQuery, MaintenanceStats, Metric, MetricId, MetricQuery, Profile,
        ProfileId, ProfileQuery, QueryTrace, RetentionPolicy, Service, Span, SpanId,
        StorageStats, TraceId, TraceQuery,
    },
    traits::{ManagementApi, QueryApi},
};

/// HTTP client for remote Sequins daemon
///
/// Implements `QueryApi` and `ManagementApi` via HTTP calls to separate servers:
/// - Query API: port 8080 (read-only operations)
/// - Management API: port 8081 (administrative operations)
pub struct RemoteClient {
    client: Client,
    query_base_url: String,
    management_base_url: String,
}

impl RemoteClient {
    /// Create a new remote client
    ///
    /// # Arguments
    ///
    /// * `query_url` - Base URL for query API (e.g., "http://localhost:8080")
    /// * `management_url` - Base URL for management API (e.g., "http://localhost:8081")
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created
    pub fn new(query_url: &str, management_url: &str) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Http(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            query_base_url: query_url.trim_end_matches('/').to_string(),
            management_base_url: management_url.trim_end_matches('/').to_string(),
        })
    }

    /// Create a client using default localhost URLs
    ///
    /// Query API: http://localhost:8080
    /// Management API: http://localhost:8081
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created
    pub fn localhost() -> Result<Self> {
        Self::new("http://localhost:8080", "http://localhost:8081")
    }
}

// Implement QueryApi via HTTP
#[async_trait::async_trait]
impl QueryApi for RemoteClient {
    async fn get_services(&self) -> sequins_core::error::Result<Vec<Service>> {
        let url = format!("{}/api/services", self.query_base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let services = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(services)
    }

    async fn query_traces(&self, query: TraceQuery) -> sequins_core::error::Result<Vec<QueryTrace>> {
        let url = format!("{}/api/traces", self.query_base_url);
        let response = self
            .client
            .post(&url)
            .json(&query)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let traces = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(traces)
    }

    async fn get_spans(&self, trace_id: TraceId) -> sequins_core::error::Result<Vec<Span>> {
        let url = format!("{}/api/traces/{}/spans", self.query_base_url, trace_id.to_hex());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let spans = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(spans)
    }

    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> sequins_core::error::Result<Option<Span>> {
        let url = format!(
            "{}/api/traces/{}/spans/{}",
            self.query_base_url,
            trace_id.to_hex(),
            span_id.to_hex()
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let span = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(span)
    }

    async fn query_logs(&self, query: LogQuery) -> sequins_core::error::Result<Vec<LogEntry>> {
        let url = format!("{}/api/logs", self.query_base_url);
        let response = self
            .client
            .post(&url)
            .json(&query)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let logs = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(logs)
    }

    async fn get_log(&self, log_id: LogId) -> sequins_core::error::Result<Option<LogEntry>> {
        let url = format!("{}/api/logs/{}", self.query_base_url, log_id.to_hex());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let log = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(log)
    }

    async fn query_metrics(&self, query: MetricQuery) -> sequins_core::error::Result<Vec<Metric>> {
        let url = format!("{}/api/metrics", self.query_base_url);
        let response = self
            .client
            .post(&url)
            .json(&query)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let metrics = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(metrics)
    }

    async fn get_metric(&self, metric_id: MetricId) -> sequins_core::error::Result<Option<Metric>> {
        let url = format!("{}/api/metrics/{}", self.query_base_url, metric_id.to_hex());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let metric = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(metric)
    }

    async fn get_profiles(&self, query: ProfileQuery) -> sequins_core::error::Result<Vec<Profile>> {
        let url = format!("{}/api/profiles", self.query_base_url);
        let response = self
            .client
            .post(&url)
            .json(&query)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let profiles = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(profiles)
    }

    async fn get_profile(&self, profile_id: ProfileId) -> sequins_core::error::Result<Option<Profile>> {
        let url = format!("{}/api/profiles/{}", self.query_base_url, profile_id.to_hex());
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let profile = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(profile)
    }
}

// Implement ManagementApi via HTTP
#[async_trait::async_trait]
impl ManagementApi for RemoteClient {
    async fn run_retention_cleanup(&self) -> sequins_core::error::Result<usize> {
        let url = format!("{}/api/retention/cleanup", self.management_base_url);
        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        #[derive(serde::Deserialize)]
        struct Response {
            deleted_count: usize,
        }

        let result: Response = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(result.deleted_count)
    }

    async fn update_retention_policy(&self, policy: RetentionPolicy) -> sequins_core::error::Result<()> {
        let url = format!("{}/api/retention/policy", self.management_base_url);
        self.client
            .put(&url)
            .json(&policy)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        Ok(())
    }

    async fn get_retention_policy(&self) -> sequins_core::error::Result<RetentionPolicy> {
        let url = format!("{}/api/retention/policy", self.management_base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let policy = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(policy)
    }

    async fn run_maintenance(&self) -> sequins_core::error::Result<MaintenanceStats> {
        let url = format!("{}/api/maintenance", self.management_base_url);
        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let stats = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(stats)
    }

    async fn get_storage_stats(&self) -> sequins_core::error::Result<StorageStats> {
        let url = format!("{}/api/storage/stats", self.management_base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("HTTP request failed: {}", e)))?;

        let stats = response
            .json()
            .await
            .map_err(|e| sequins_core::error::Error::Other(format!("Failed to parse response: {}", e)))?;

        Ok(stats)
    }
}
