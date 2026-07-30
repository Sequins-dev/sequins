//! Conditional-write smoke test against a real S3-compatible endpoint.
//!
//! Skipped unless `SEQUINS_TEST_S3_URI` is set, so an ordinary `cargo test` on a
//! machine with no object store still passes. Point it at AWS S3 or a local
//! MinIO:
//!
//! ```text
//! docker run -d --rm -p 19000:9000 \
//!   -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//!   minio/minio:latest server /data
//! docker run --rm --entrypoint sh minio/mc:latest -c \
//!   "mc alias set t http://host.docker.internal:19000 minioadmin minioadmin \
//!    && mc mb --ignore-existing t/sequins-test"
//!
//! SEQUINS_TEST_S3_URI=s3://sequins-test \
//! SEQUINS_TEST_S3_ENDPOINT=http://127.0.0.1:19000 \
//! SEQUINS_TEST_S3_ACCESS_KEY_ID=minioadmin \
//! SEQUINS_TEST_S3_SECRET_ACCESS_KEY=minioadmin \
//!   cargo test -p sequins-object-store --test conditional_put_s3
//! ```
//!
//! This is the compare-and-swap behaviour that a generation pointer depends on,
//! so it is worth exercising against a real endpoint rather than trusting that
//! `S3ConditionalPut::ETagMatch` is wired up.

use object_store::{
    path::Path, Error as OsError, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use sequins_object_store::{build, ObjectStoreConfig};

/// Read the endpoint config from the environment, or `None` to skip.
fn config_from_env() -> Option<(String, ObjectStoreConfig)> {
    let uri = std::env::var("SEQUINS_TEST_S3_URI").ok()?;
    let config = ObjectStoreConfig {
        region: std::env::var("SEQUINS_TEST_S3_REGION")
            .ok()
            .or_else(|| Some("us-east-1".to_string())),
        endpoint: std::env::var("SEQUINS_TEST_S3_ENDPOINT").ok(),
        // A local MinIO is served over plain HTTP and needs path-style addressing.
        allow_http: true,
        virtual_hosted_style: Some(false),
        access_key_id: std::env::var("SEQUINS_TEST_S3_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("SEQUINS_TEST_S3_SECRET_ACCESS_KEY").ok(),
    };
    Some((uri, config))
}

/// A distinct key per run so repeat runs against a persistent bucket don't
/// collide on the `Create` assertion.
fn unique_key(label: &str) -> Path {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    Path::from(format!("conditional-put-test/{label}-{nanos}.json"))
}

#[tokio::test]
async fn create_conflicts_and_update_enforces_the_etag() {
    let Some((uri, config)) = config_from_env() else {
        eprintln!("SEQUINS_TEST_S3_URI unset — skipping S3 conditional-put test");
        return;
    };

    assert!(
        sequins_object_store::supports_conditional_put(&uri),
        "{uri} should be declared CAS-capable"
    );

    let store = build(&uri, &config).expect("store should build");
    let path = unique_key("generation");

    let create = || PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };

    // First Create wins and yields the version we must present to update.
    let first = store
        .put_opts(&path, bytes::Bytes::from_static(b"gen-1").into(), create())
        .await
        .expect("first Create should succeed");

    // A racing writer that assumes the object is absent must lose.
    let conflict = store
        .put_opts(&path, bytes::Bytes::from_static(b"gen-1'").into(), create())
        .await;
    assert!(
        matches!(conflict, Err(OsError::AlreadyExists { .. })),
        "second Create should conflict, got {conflict:?}"
    );

    // A writer holding a stale version must lose rather than clobber.
    let rejected = store
        .put_opts(
            &path,
            bytes::Bytes::from_static(b"gen-2-from-stale").into(),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some("\"0000000000000000000000000000dead\"".to_string()),
                    version: None,
                }),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(rejected, Err(OsError::Precondition { .. })),
        "Update with a stale ETag should be rejected, got {rejected:?}"
    );

    // The holder of the current version advances the generation.
    // Carry both e_tag and version: S3 matches the ETag, GCS needs the version.
    let second = store
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
        .await
        .expect("Update with the current ETag should succeed");

    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(&bytes[..], b"gen-2", "the winning write should be visible");

    // The superseded version must not be reusable — otherwise a lagging writer
    // could resurrect an old generation.
    let replayed = store
        .put_opts(
            &path,
            bytes::Bytes::from_static(b"gen-3-replay").into(),
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
        matches!(replayed, Err(OsError::Precondition { .. })),
        "replaying the superseded version should be rejected, got {replayed:?}"
    );

    assert_ne!(
        first.e_tag, second.e_tag,
        "each generation should get a distinct ETag"
    );

    store.delete(&path).await.ok();
}
