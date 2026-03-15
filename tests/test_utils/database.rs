/// Test utilities for creating isolated test databases
use anyhow::Result;
use libsql::{Builder, Connection, Database};
use tempfile::TempDir;

/// Test database wrapper that provides isolation and automatic cleanup
pub struct TestDatabase {
    pub conn: Connection,
    #[allow(dead_code)]
    pub db: Database,
    #[allow(dead_code)]
    temp_dir: Option<TempDir>,
}

impl TestDatabase {
    /// Create a new in-memory database (fast, isolated)
    pub async fn new_in_memory() -> Result<Self> {
        // Use a file-based temporary database instead of named in-memory database
        // Named in-memory databases (:memory:name) create temporary files that may
        // not be cleaned up properly, littering the filesystem.
        // File-based temp databases are guaranteed to be cleaned up by TempDir.
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir
            .path()
            .join(format!("test-{}.db", uuid::Uuid::new_v4()));

        let db = Builder::new_local(&db_path).build().await?;
        let conn = db.connect()?;

        Ok(Self {
            conn,
            db,
            temp_dir: Some(temp_dir),
        })
    }

    /// Create a new file-based database in a temporary directory
    /// Use this for tests that need to persist data across connections
    pub async fn new_file_based() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test.db");

        let db = Builder::new_local(db_path).build().await?;
        let conn = db.connect()?;

        Ok(Self {
            conn,
            db,
            temp_dir: Some(temp_dir),
        })
    }

    /// Create a new in-memory database with schema initialized
    pub async fn with_schema() -> Result<Self> {
        let test_db = Self::new_in_memory().await?;
        test_db.initialize_schema().await?;
        Ok(test_db)
    }

    /// Initialize the Sequins database schema
    async fn initialize_schema(&self) -> Result<()> {
        // Import the schema initialization from storage
        // We'll execute the same DDL that TursoStorage uses
        self.execute_ddl().await?;
        Ok(())
    }

    /// Execute the complete DDL schema
    async fn execute_ddl(&self) -> Result<()> {
        // Spans table
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS spans (
                    span_id TEXT PRIMARY KEY NOT NULL,
                    trace_id TEXT NOT NULL,
                    parent_span_id TEXT,
                    name TEXT NOT NULL,
                    kind INTEGER NOT NULL,
                    start_time INTEGER NOT NULL,
                    end_time INTEGER NOT NULL,
                    status_code INTEGER NOT NULL,
                    status_message TEXT,
                    attributes BLOB,
                    events BLOB,
                    links BLOB,
                    service_name TEXT,
                    -- Generated columns for common semantic conventions
                    http_method TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"http.method\"')) VIRTUAL,
                    http_status_code INTEGER GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"http.status_code\"')) VIRTUAL,
                    rpc_method TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"rpc.method\"')) VIRTUAL,
                    rpc_system TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"rpc.system\"')) VIRTUAL,
                    db_system TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"db.system\"')) VIRTUAL,
                    db_operation TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"db.operation\"')) VIRTUAL,
                    messaging_system TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"messaging.system\"')) VIRTUAL,
                    messaging_operation TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.\"messaging.operation\"')) VIRTUAL
                )",
                (),
            )
            .await?;

        // Indexes for spans
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON spans(trace_id)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_spans_start_time ON spans(start_time DESC)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_spans_service_name ON spans(service_name)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_spans_http_method ON spans(http_method)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_spans_http_status_code ON spans(http_status_code)",
                (),
            )
            .await?;

        // Logs table
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS logs (
                    log_id TEXT PRIMARY KEY NOT NULL,
                    timestamp INTEGER NOT NULL,
                    observed_timestamp INTEGER NOT NULL,
                    trace_id TEXT,
                    span_id TEXT,
                    severity_number INTEGER NOT NULL,
                    severity_text TEXT,
                    body TEXT NOT NULL,
                    attributes BLOB,
                    service_name TEXT
                )",
                (),
            )
            .await?;

        // Indexes for logs
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp DESC)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs(trace_id)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_logs_severity_number ON logs(severity_number)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_logs_service_name ON logs(service_name)",
                (),
            )
            .await?;

        // FTS5 for log body search
        self.conn
            .execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
                    log_id UNINDEXED,
                    body,
                    content=logs,
                    content_rowid=rowid
                )",
                (),
            )
            .await?;

        // Triggers for FTS5 sync
        self.conn
            .execute(
                "CREATE TRIGGER IF NOT EXISTS logs_fts_insert AFTER INSERT ON logs BEGIN
                    INSERT INTO logs_fts(rowid, log_id, body) VALUES (new.rowid, new.log_id, new.body);
                END",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE TRIGGER IF NOT EXISTS logs_fts_delete AFTER DELETE ON logs BEGIN
                    DELETE FROM logs_fts WHERE rowid = old.rowid;
                END",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE TRIGGER IF NOT EXISTS logs_fts_update AFTER UPDATE ON logs BEGIN
                    DELETE FROM logs_fts WHERE rowid = old.rowid;
                    INSERT INTO logs_fts(rowid, log_id, body) VALUES (new.rowid, new.log_id, new.body);
                END",
                (),
            )
            .await?;

        // Metrics table
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS metrics (
                    metric_id TEXT PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    unit TEXT,
                    type INTEGER NOT NULL,
                    temporality INTEGER NOT NULL,
                    is_monotonic INTEGER,
                    attributes BLOB,
                    service_name TEXT
                )",
                (),
            )
            .await?;

        // Indexes for metrics
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_service_name ON metrics(service_name)",
                (),
            )
            .await?;

        // Metric data points table
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS metric_data_points (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_id TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    start_timestamp INTEGER,
                    value_type INTEGER NOT NULL,
                    int_value INTEGER,
                    double_value REAL,
                    histogram_data BLOB,
                    exponential_histogram_data BLOB,
                    summary_data BLOB,
                    attributes BLOB,
                    exemplars BLOB,
                    flags INTEGER,
                    FOREIGN KEY (metric_id) REFERENCES metrics(metric_id) ON DELETE CASCADE
                )",
                (),
            )
            .await?;

        // Indexes for metric data points
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_metric_data_points_metric_id ON metric_data_points(metric_id)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_metric_data_points_timestamp ON metric_data_points(timestamp DESC)",
                (),
            )
            .await?;

        // Profiles table
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS profiles (
                    profile_id TEXT PRIMARY KEY NOT NULL,
                    timestamp INTEGER NOT NULL,
                    duration INTEGER NOT NULL,
                    profile_type TEXT NOT NULL,
                    sample_rate INTEGER,
                    samples BLOB NOT NULL,
                    attributes BLOB,
                    service_name TEXT
                )",
                (),
            )
            .await?;

        // Indexes for profiles
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_profiles_timestamp ON profiles(timestamp DESC)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_profiles_service_name ON profiles(service_name)",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_profiles_type ON profiles(profile_type)",
                (),
            )
            .await?;

        Ok(())
    }

    /// Assert that a table exists in the database
    pub async fn assert_table_exists(&self, table_name: &str) -> Result<bool> {
        let mut rows = self
            .conn
            .query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                [table_name],
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Assert that an index exists in the database
    pub async fn assert_index_exists(&self, index_name: &str) -> Result<bool> {
        let mut rows = self
            .conn
            .query(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                [index_name],
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Assert that a trigger exists in the database
    pub async fn assert_trigger_exists(&self, trigger_name: &str) -> Result<bool> {
        let mut rows = self
            .conn
            .query(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name=?",
                [trigger_name],
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Get count of rows in a table
    pub async fn count_rows(&self, table_name: &str) -> Result<i64> {
        let query = format!("SELECT COUNT(*) FROM {}", table_name);
        let mut rows = self.conn.query(&query, ()).await?;

        if let Some(row) = rows.next().await? {
            Ok(row.get::<i64>(0)?)
        } else {
            Ok(0)
        }
    }

    /// Get table schema information
    pub async fn get_table_info(&self, table_name: &str) -> Result<Vec<(String, String)>> {
        let query = format!("PRAGMA table_info({})", table_name);
        let mut rows = self.conn.query(&query, ()).await?;

        let mut columns = Vec::new();
        while let Some(row) = rows.next().await? {
            let name: String = row.get(1)?;
            let col_type: String = row.get(2)?;
            columns.push((name, col_type));
        }

        Ok(columns)
    }

    /// Execute a raw SQL query (for test setup)
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.conn.execute(sql, ()).await?;
        Ok(())
    }

    /// Close the database connection
    #[allow(dead_code)]
    pub async fn close(self) -> Result<()> {
        // Connection will be closed when dropped
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_in_memory_database() {
        let db = TestDatabase::new_in_memory().await.unwrap();
        assert!(db.conn.is_autocommit());
        assert!(db.temp_dir.is_some()); // Now uses temp file instead of :memory:
    }

    #[tokio::test]
    async fn test_create_file_based_database() {
        let db = TestDatabase::new_file_based().await.unwrap();
        assert!(db.temp_dir.is_some());
        assert!(db.conn.is_autocommit());
    }

    #[tokio::test]
    async fn test_with_schema_creates_tables() {
        let db = TestDatabase::with_schema().await.unwrap();

        // Check that all tables exist
        assert!(db.assert_table_exists("spans").await.unwrap());
        assert!(db.assert_table_exists("logs").await.unwrap());
        assert!(db.assert_table_exists("logs_fts").await.unwrap());
        assert!(db.assert_table_exists("metrics").await.unwrap());
        assert!(db.assert_table_exists("metric_data_points").await.unwrap());
        assert!(db.assert_table_exists("profiles").await.unwrap());
    }

    #[tokio::test]
    async fn test_with_schema_creates_indexes() {
        let db = TestDatabase::with_schema().await.unwrap();

        // Check that key indexes exist
        assert!(db.assert_index_exists("idx_spans_trace_id").await.unwrap());
        assert!(db
            .assert_index_exists("idx_spans_start_time")
            .await
            .unwrap());
        assert!(db.assert_index_exists("idx_logs_timestamp").await.unwrap());
        assert!(db.assert_index_exists("idx_metrics_name").await.unwrap());
    }

    #[tokio::test]
    async fn test_with_schema_creates_triggers() {
        let db = TestDatabase::with_schema().await.unwrap();

        // Check FTS5 triggers
        assert!(db.assert_trigger_exists("logs_fts_insert").await.unwrap());
        assert!(db.assert_trigger_exists("logs_fts_delete").await.unwrap());
        assert!(db.assert_trigger_exists("logs_fts_update").await.unwrap());
    }

    #[tokio::test]
    async fn test_count_rows_empty_table() {
        let db = TestDatabase::with_schema().await.unwrap();
        let count = db.count_rows("spans").await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_get_table_info() {
        let db = TestDatabase::with_schema().await.unwrap();
        let columns = db.get_table_info("spans").await.unwrap();

        assert!(!columns.is_empty());
        assert!(columns.iter().any(|(name, _)| name == "span_id"));
        assert!(columns.iter().any(|(name, _)| name == "trace_id"));
        assert!(columns.iter().any(|(name, _)| name == "name"));
    }

    #[tokio::test]
    async fn test_multiple_databases_isolated() {
        let db1 = TestDatabase::with_schema().await.unwrap();
        let db2 = TestDatabase::with_schema().await.unwrap();

        // Insert into db1
        db1.execute("INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name) VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1')")
            .await
            .unwrap();

        // Check counts
        assert_eq!(db1.count_rows("spans").await.unwrap(), 1);
        assert_eq!(db2.count_rows("spans").await.unwrap(), 0);
    }
}
