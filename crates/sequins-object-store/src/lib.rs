//! Shared object-store construction for Sequins.
//!
//! One place that turns a storage URI plus [`ObjectStoreConfig`] into an
//! [`ObjectStore`]. The cold tier uses it for telemetry partitions; other
//! subsystems that need their own bucket or prefix use the same entry point so
//! credential handling, endpoint overrides, and S3-compatible quirks stay
//! consistent rather than being reimplemented per subsystem.
//!
//! Deliberately dependency-light — `object_store` and nothing from the Arrow,
//! DataFusion, or Vortex stack — so a caller that only needs a store does not
//! pull in the columnar engine.
//!
//! # Conditional writes
//!
//! Callers that need compare-and-swap semantics (`PutMode::Create`,
//! `PutMode::Update`) get them from the returned store without extra
//! configuration: `AmazonS3` defaults to `S3ConditionalPut::ETagMatch`, which
//! works against AWS S3, MinIO, and R2. See [`supports_conditional_put`].

use std::sync::Arc;

use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

/// Errors raised while constructing a store.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The URI was malformed, named an unsupported scheme, or the provider
    /// rejected the resulting configuration.
    #[error("{0}")]
    Config(String),

    /// Creating the backing directory for a `file://` store failed.
    #[error("Failed to create storage directory: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Connection options for cloud object stores. All fields are optional; unset
/// values fall back to the object-store provider's defaults (including the
/// standard credential chain — instance profile, IRSA / workload identity).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ObjectStoreConfig {
    /// Region (e.g. `us-east-1`). Unset uses the provider default.
    pub region: Option<String>,

    /// Custom endpoint URL for S3-compatible stores (MinIO, Ceph RGW, a gateway).
    pub endpoint: Option<String>,

    /// Allow plain-HTTP endpoints. Required for a local MinIO served over http
    /// (object stores reject non-https endpoints otherwise).
    #[serde(default)]
    pub allow_http: bool,

    /// Force path-style addressing (`endpoint/bucket`) instead of virtual-hosted
    /// (`bucket.endpoint`). Most S3-compatibles (MinIO) need path-style.
    pub virtual_hosted_style: Option<bool>,

    /// Static access key id. Prefer the credential chain (IRSA / instance
    /// profile) in production; set this only for local/dev (e.g. MinIO).
    pub access_key_id: Option<String>,

    /// Static secret access key (paired with `access_key_id`).
    pub secret_access_key: Option<String>,
}

/// Build an object store for `uri`.
///
/// Supports:
/// - Local filesystem: `file:///path` or `/path`
/// - AWS S3 (and S3-compatibles): `s3://bucket/path`
/// - Google Cloud Storage: `gs://bucket/path`
/// - Azure Blob Storage: `az://container/path` or `azure://container/path`
///
/// Cloud credentials default to the provider's standard chain — instance
/// profile, IRSA / workload identity — so a properly-configured pod needs no
/// static credentials in config. `config` supplies connection settings only.
///
/// For a `file://` URI the target directory is created if it does not exist.
pub fn build(uri: &str, config: &ObjectStoreConfig) -> Result<Arc<dyn ObjectStore>> {
    if uri.starts_with("file://") || uri.starts_with('/') {
        return build_local(uri);
    }
    if uri.starts_with("s3://") {
        return build_s3(uri, config);
    }
    if uri.starts_with("gs://") {
        return build_gcs(uri);
    }
    if uri.starts_with("az://") || uri.starts_with("azure://") {
        return build_azure(uri, config);
    }

    Err(Error::Config(format!(
        "Unsupported object store URI: {}. Supported: file://, s3://, gs://, az://",
        uri
    )))
}

/// Whether stores built for `uri` support conditional update
/// (`PutMode::Update`, i.e. compare-and-swap on an existing object).
///
/// All three cloud backends do: S3 via `If-Match` on the ETag, GCS via
/// `x-goog-if-generation-match`, Azure via its own precondition header.
/// **`LocalFileSystem` does not** — `object_store` 0.13 returns
/// `Error::NotImplemented` for `PutMode::Update`, though it does support
/// `PutMode::Create`.
///
/// A subsystem whose correctness rests on CAS — advancing a generation pointer,
/// for instance — must check this at configuration time and refuse a `file://`
/// URI, rather than discovering the gap at the first contended write.
///
/// Preserve **both** `e_tag` and `version` from the [`object_store::PutResult`]
/// when constructing the next `UpdateVersion`: S3 matches on the ETag while GCS
/// requires `version` and fails with `MissingVersion` without it.
pub fn supports_conditional_put(uri: &str) -> bool {
    uri.starts_with("s3://")
        || uri.starts_with("gs://")
        || uri.starts_with("az://")
        || uri.starts_with("azure://")
}

fn build_local(uri: &str) -> Result<Arc<dyn ObjectStore>> {
    use object_store::local::LocalFileSystem;

    let path = uri.strip_prefix("file://").unwrap_or(uri);
    std::fs::create_dir_all(path)?;

    // No prefix — callers address objects with full paths.
    Ok(Arc::new(LocalFileSystem::new()))
}

fn build_s3(uri: &str, config: &ObjectStoreConfig) -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;

    let bucket = bucket_from(uri, "S3")?;

    // Base on `from_env` so the default AWS credential chain (instance profile,
    // IRSA / web-identity, the credentials injected by the platform) is picked
    // up with no configuration. Connection settings come from `config`, not the
    // environment.
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }
    if let Some(vhost) = config.virtual_hosted_style {
        builder = builder.with_virtual_hosted_style_request(vhost);
    }
    if let (Some(key), Some(secret)) = (&config.access_key_id, &config.secret_access_key) {
        builder = builder
            .with_access_key_id(key)
            .with_secret_access_key(secret);
    }

    let store = builder
        .build()
        .map_err(|e| Error::Config(format!("Failed to create S3 store: {}", e)))?;

    Ok(Arc::new(store))
}

fn build_gcs(uri: &str) -> Result<Arc<dyn ObjectStore>> {
    use object_store::gcp::GoogleCloudStorageBuilder;

    let bucket = bucket_from(uri, "GCS")?;

    // `from_env` picks up GOOGLE_APPLICATION_CREDENTIALS and the default
    // workload-identity credential chain; the bucket comes from the URI.
    let store = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(|e| Error::Config(format!("Failed to create GCS store: {}", e)))?;

    Ok(Arc::new(store))
}

fn build_azure(uri: &str, config: &ObjectStoreConfig) -> Result<Arc<dyn ObjectStore>> {
    use object_store::azure::MicrosoftAzureBuilder;

    let stripped = uri
        .strip_prefix("az://")
        .or_else(|| uri.strip_prefix("azure://"))
        .expect("caller checked the scheme");

    let url = url::Url::parse(&format!("https://{}", stripped))
        .map_err(|e| Error::Config(format!("Invalid Azure URI '{}': {}", uri, e)))?;
    let container = url
        .host_str()
        .ok_or_else(|| Error::Config(format!("Azure URI missing container name: {}", uri)))?;

    // `from_env` picks up AZURE_STORAGE_* and the default credential chain
    // (managed / workload identity); the container comes from the URI.
    let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(container);
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }
    let store = builder
        .build()
        .map_err(|e| Error::Config(format!("Failed to create Azure store: {}", e)))?;

    Ok(Arc::new(store))
}

/// Extract the bucket (URI host) from a `scheme://bucket/path` URI.
fn bucket_from<'a>(uri: &'a str, label: &str) -> Result<&'a str> {
    let rest = uri
        .split_once("://")
        .map(|(_, rest)| rest)
        .ok_or_else(|| Error::Config(format!("Invalid {} URI '{}'", label, uri)))?;

    let bucket = rest.split('/').next().unwrap_or("");
    if bucket.is_empty() {
        return Err(Error::Config(format!(
            "{} URI missing bucket name: {}",
            label, uri
        )));
    }
    Ok(bucket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_scheme_is_rejected() {
        let err = build("ftp://host/path", &ObjectStoreConfig::default()).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert!(err.to_string().contains("Unsupported object store URI"));
    }

    #[test]
    fn bucket_is_parsed_from_uri() {
        assert_eq!(
            bucket_from("s3://my-bucket/a/b", "S3").unwrap(),
            "my-bucket"
        );
        assert_eq!(bucket_from("s3://my-bucket", "S3").unwrap(), "my-bucket");
        assert!(bucket_from("s3:///a/b", "S3").is_err());
        assert!(bucket_from("my-bucket/a", "S3").is_err());
    }

    #[test]
    fn local_uri_creates_the_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("nested/created");
        let uri = format!("file://{}", target.display());

        build(&uri, &ObjectStoreConfig::default()).unwrap();

        assert!(
            target.is_dir(),
            "build() should create the target directory"
        );
    }

    #[test]
    fn conditional_put_support_is_declared_per_scheme() {
        assert!(supports_conditional_put("s3://bucket/x"));
        assert!(supports_conditional_put("gs://bucket/x"));
        assert!(supports_conditional_put("az://container/x"));
        assert!(supports_conditional_put("azure://container/x"));
        // LocalFileSystem returns NotImplemented for PutMode::Update.
        assert!(!supports_conditional_put("file:///tmp/x"));
        assert!(!supports_conditional_put("/tmp/x"));
    }

    /// Pins the local backend's actual conditional-write behaviour: `Create`
    /// works, `Update` does not. This is why [`supports_conditional_put`]
    /// excludes `file://`. If `object_store` ever implements local `Update`,
    /// this test fails and both it and the capability check should be updated.
    #[tokio::test]
    async fn local_supports_create_but_not_update() {
        use object_store::{path::Path, Error as OsError, PutMode, PutOptions, UpdateVersion};

        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", tmp.path().display());
        let store = build(&uri, &ObjectStoreConfig::default()).unwrap();
        let path = Path::from(format!("{}/current.json", tmp.path().display()));

        let create = || PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };

        let first = store
            .put_opts(&path, bytes::Bytes::from_static(b"gen-1").into(), create())
            .await
            .expect("first Create should succeed");

        let conflict = store
            .put_opts(&path, bytes::Bytes::from_static(b"gen-1'").into(), create())
            .await;
        assert!(
            matches!(conflict, Err(OsError::AlreadyExists { .. })),
            "second Create should conflict, got {conflict:?}"
        );

        let update = store
            .put_opts(
                &path,
                bytes::Bytes::from_static(b"gen-2").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: first.e_tag.clone(),
                        version: first.version.clone(),
                    }),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(update, Err(OsError::NotImplemented { .. })),
            "local Update is expected to be unimplemented, got {update:?}"
        );
    }
}
