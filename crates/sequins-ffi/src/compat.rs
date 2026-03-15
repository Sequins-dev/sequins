//! Compatibility stub types for old query API
//!
//! These types are no longer used in the current SeQL-based system,
//! but are kept as stubs to allow old FFI type definitions to compile.

#![allow(dead_code)]

use sequins_types::models::{
    LogEntry, Metric, MetricDataPoint, Profile, ProfileSample, Span, Timestamp,
};
use std::collections::HashMap;

/// Stub - not used
#[derive(Debug, Clone)]
pub struct QueryCursor {
    pub query_timestamp: Timestamp,
}

impl QueryCursor {
    pub fn to_opaque(&self) -> String {
        String::new()
    }
}

/// Stub - not used
#[derive(Debug, Clone)]
pub struct LogQueryResult {
    pub logs: Vec<LogEntry>,
    pub cursor: QueryCursor,
}

/// Stub - not used
#[derive(Debug, Clone)]
pub struct MetricQueryResult {
    pub metrics: Vec<Metric>,
    pub cursor: QueryCursor,
}

/// Stub - not used
#[derive(Debug, Clone)]
pub struct MetricDataPointQueryResult {
    pub data_points: Vec<MetricDataPoint>,
    pub cursor: QueryCursor,
}

/// Stub - not used
#[derive(Debug, Clone)]
pub struct ProfileQueryResult {
    pub profiles: Vec<Profile>,
    pub cursor: QueryCursor,
}

/// Stub - not used
#[derive(Debug, Clone)]
pub struct ProfileSampleQueryResult {
    pub samples: Vec<ProfileSample>,
    pub cursor: QueryCursor,
}

/// Stub - not used
#[derive(Debug, Clone)]
pub struct SpanQueryResult {
    pub spans: Vec<Span>,
    pub cursor: QueryCursor,
}

/// Stub - not used
#[derive(Debug, Clone)]
pub struct Service {
    pub name: String,
    pub span_count: usize,
    pub log_count: usize,
    pub resource_attributes: HashMap<String, Vec<String>>,
}
