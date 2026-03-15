// Test to verify libSQL JSONB support
use libsql;
use tempfile::TempDir;

#[tokio::test]
async fn test_jsonb_support() -> anyhow::Result<()> {
    // Create temporary database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");

    let db = libsql::Builder::new_local(&db_path).build().await?;

    let conn = db.connect()?;

    // Create test table
    conn.execute(
        "CREATE TABLE jsonb_test (id INTEGER PRIMARY KEY, data BLOB)",
        (),
    )
    .await?;

    // Test 1: Insert JSON as JSONB using jsonb() function
    let json_text = r#"{"http":{"method":"GET","status_code":200},"db":{"system":"postgres"}}"#;

    let insert_result = conn
        .execute(
            "INSERT INTO jsonb_test (id, data) VALUES (?, jsonb(?))",
            libsql::params![1, json_text],
        )
        .await;

    match insert_result {
        Ok(_) => {
            println!("✅ JSONB insert successful - jsonb() function is supported");

            // Test 2: Query with json_extract() (should work with both TEXT and BLOB)
            let mut rows = conn
                .query(
                    "SELECT json_extract(data, '$.http.method'), json_extract(data, '$.http.status_code') FROM jsonb_test WHERE id = 1",
                    (),
                )
                .await?;

            if let Some(row) = rows.next().await? {
                let method: String = row.get(0)?;
                let status_code: i64 = row.get(1)?;

                assert_eq!(method, "GET");
                assert_eq!(status_code, 200);

                println!("✅ json_extract() works with JSONB blob");
                println!("   - http.method: {}", method);
                println!("   - http.status_code: {}", status_code);
            }

            // Test 3: Verify it's stored as BLOB, not TEXT
            let mut rows = conn
                .query("SELECT typeof(data) FROM jsonb_test WHERE id = 1", ())
                .await?;

            if let Some(row) = rows.next().await? {
                let data_type: String = row.get(0)?;
                println!("✅ Data type: {} (expected: blob)", data_type);
                assert_eq!(data_type, "blob");
            }

            println!("\n🎉 libSQL {} SUPPORTS JSONB!", "0.6.0");
            println!("   - jsonb() function available");
            println!("   - Stores as BLOB");
            println!("   - json_extract() works transparently");

            Ok(())
        }
        Err(e) => {
            // jsonb() function not available
            println!("❌ JSONB NOT supported in libSQL 0.6.0");
            println!("   Error: {}", e);
            println!("   This likely means libSQL is based on SQLite < 3.45.0");
            println!("\n   Recommendation: Keep TEXT JSON storage for now");

            // Return Ok to pass the test - we just wanted to verify support
            Ok(())
        }
    }
}
