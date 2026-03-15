//! Additional profile model tests
//!
//! Tests edge cases not covered in unit tests:
//! - Profile serialization/deserialization
//! - Large profile data handling
//! - ProfileType edge cases
//! - Profile with trace correlation

use sequins::models::{
    profiles::{Profile, ProfileId, ProfileType},
    Timestamp, TraceId,
};

// ============================================================================
// ProfileType Additional Tests
// ============================================================================

#[test]
fn test_profile_type_all_variants() {
    let types = vec![
        ProfileType::Cpu,
        ProfileType::Memory,
        ProfileType::Goroutine,
        ProfileType::Other,
    ];
    assert_eq!(types.len(), 4);
}

#[test]
fn test_profile_type_equality() {
    assert_eq!(ProfileType::Cpu, ProfileType::Cpu);
    assert_ne!(ProfileType::Cpu, ProfileType::Memory);
    assert_ne!(ProfileType::Memory, ProfileType::Goroutine);
}

#[test]
fn test_profile_type_hash() {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(ProfileType::Cpu);
    set.insert(ProfileType::Memory);
    set.insert(ProfileType::Cpu); // Duplicate
    assert_eq!(set.len(), 2);
}

// ============================================================================
// Profile Serialization Tests
// ============================================================================

#[test]
fn test_profile_serialization() {
    let profile = Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1000),
        service_name: "test-service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "cpu".to_string(),
        sample_unit: "nanoseconds".to_string(),
        data: vec![0x1f, 0x8b, 0x08, 0x00], // Gzip magic bytes
        trace_id: None,
    };

    let json = serde_json::to_string(&profile).unwrap();
    let deserialized: Profile = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.id, profile.id);
    assert_eq!(deserialized.timestamp, profile.timestamp);
    assert_eq!(deserialized.service_name, profile.service_name);
    assert_eq!(deserialized.profile_type, profile.profile_type);
    assert_eq!(deserialized.data, profile.data);
}

#[test]
fn test_profile_type_serialization() {
    // ProfileType uses #[serde(rename_all = "lowercase")]
    let cpu = ProfileType::Cpu;
    let json = serde_json::to_string(&cpu).unwrap();
    assert_eq!(json, "\"cpu\"");

    let memory = ProfileType::Memory;
    let json = serde_json::to_string(&memory).unwrap();
    assert_eq!(json, "\"memory\"");
}

// ============================================================================
// Profile with Trace Context Tests
// ============================================================================

#[test]
fn test_profile_with_trace_context() {
    let trace_id = TraceId::from_bytes([1; 16]);
    let profile = Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1000),
        service_name: "service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "cpu".to_string(),
        sample_unit: "nanoseconds".to_string(),
        data: vec![1, 2, 3],
        trace_id: Some(trace_id),
    };

    assert!(profile.has_trace_context());
    assert_eq!(profile.trace_id, Some(trace_id));
}

#[test]
fn test_profile_without_trace_context() {
    let profile = Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1000),
        service_name: "service".to_string(),
        profile_type: ProfileType::Memory,
        sample_type: "heap".to_string(),
        sample_unit: "bytes".to_string(),
        data: vec![1, 2, 3],
        trace_id: None,
    };

    assert!(!profile.has_trace_context());
    assert_eq!(profile.trace_id, None);
}

// ============================================================================
// Profile Data Size Tests
// ============================================================================

#[test]
fn test_profile_empty_data() {
    let profile = Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1000),
        service_name: "service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "cpu".to_string(),
        sample_unit: "nanoseconds".to_string(),
        data: vec![],
        trace_id: None,
    };

    assert_eq!(profile.data_size(), 0);
}

#[test]
fn test_profile_large_data() {
    let large_data = vec![0u8; 1_000_000]; // 1MB
    let profile = Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1000),
        service_name: "service".to_string(),
        profile_type: ProfileType::Memory,
        sample_type: "heap".to_string(),
        sample_unit: "bytes".to_string(),
        data: large_data.clone(),
        trace_id: None,
    };

    assert_eq!(profile.data_size(), 1_000_000);
}

// ============================================================================
// Profile Clone Tests
// ============================================================================

#[test]
fn test_profile_clone() {
    let original = Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1000),
        service_name: "service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "cpu".to_string(),
        sample_unit: "nanoseconds".to_string(),
        data: vec![1, 2, 3, 4, 5],
        trace_id: Some(TraceId::from_bytes([1; 16])),
    };

    let cloned = original.clone();
    assert_eq!(cloned, original);
    assert_eq!(cloned.data, original.data);
}
