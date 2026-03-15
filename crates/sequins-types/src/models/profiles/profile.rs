use super::ProfileId;
use crate::models::{AttributeValue, Timestamp, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

/// Profile data from OTLP profiles
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    /// Unique identifier for this profile
    pub id: ProfileId,
    /// When this profile was captured
    pub timestamp: Timestamp,
    /// Type of profile (0=CPU, 1=Memory, 2=Goroutine, 3=Other)
    pub profile_type: u8,
    /// Type of samples (e.g., "cpu", "alloc_objects")
    pub sample_type: String,
    /// Unit for sample values (e.g., "nanoseconds", "bytes")
    pub sample_unit: String,
    /// Duration of the profiling session in nanoseconds
    pub duration_nanos: i64,
    /// Sampling period (how often samples were taken)
    pub period: i64,
    /// Type of period (e.g., "cpu", "space")
    pub period_type: String,
    /// Unit for period (e.g., "nanoseconds", "bytes")
    pub period_unit: String,
    /// Resource ID reference (FK to ResourceRegistry)
    pub resource_id: u32,
    /// Scope ID reference (FK to ScopeRegistry)
    pub scope_id: u32,
    /// Original format of the profile data (e.g., "pprof", "jfr")
    pub original_format: Option<String>,
    /// Additional attributes attached to this profile
    pub attributes: HashMap<String, AttributeValue>,
    /// Encoded profile data (raw bytes for re-parsing/forwarding)
    pub data: Vec<u8>,
    /// Associated trace ID (if available)
    pub trace_id: Option<TraceId>,
}

/// Profile type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum ProfileType {
    /// CPU profile
    Cpu = 0,
    /// Memory/heap profile
    Memory = 1,
    /// Goroutine profile (Go-specific)
    Goroutine = 2,
    /// Other profile type
    Other = 3,
}

impl From<u8> for ProfileType {
    fn from(value: u8) -> Self {
        match value {
            0 => ProfileType::Cpu,
            1 => ProfileType::Memory,
            2 => ProfileType::Goroutine,
            _ => ProfileType::Other,
        }
    }
}

impl From<ProfileType> for u8 {
    fn from(profile_type: ProfileType) -> Self {
        profile_type as u8
    }
}

impl ProfileType {
    /// Get display string
    pub fn as_str(&self) -> &'static str {
        match self {
            ProfileType::Cpu => "cpu",
            ProfileType::Memory => "memory",
            ProfileType::Goroutine => "goroutine",
            ProfileType::Other => "other",
        }
    }
}

impl FromStr for ProfileType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cpu" => Ok(ProfileType::Cpu),
            "memory" | "heap" | "allocs" => Ok(ProfileType::Memory),
            "goroutine" | "goroutines" | "threads" => Ok(ProfileType::Goroutine),
            _ => Ok(ProfileType::Other),
        }
    }
}

impl Profile {
    /// Get the profile type as enum
    pub fn get_profile_type(&self) -> ProfileType {
        ProfileType::from(self.profile_type)
    }

    /// Check if profile is linked to a trace
    pub fn has_trace_context(&self) -> bool {
        self.trace_id.is_some()
    }

    /// Get data size in bytes
    pub fn data_size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_type_from_str() {
        assert_eq!("cpu".parse::<ProfileType>(), Ok(ProfileType::Cpu));
        assert_eq!("CPU".parse::<ProfileType>(), Ok(ProfileType::Cpu));
        assert_eq!("memory".parse::<ProfileType>(), Ok(ProfileType::Memory));
        assert_eq!("heap".parse::<ProfileType>(), Ok(ProfileType::Memory));
        assert_eq!("allocs".parse::<ProfileType>(), Ok(ProfileType::Memory));
        assert_eq!(
            "goroutine".parse::<ProfileType>(),
            Ok(ProfileType::Goroutine)
        );
        assert_eq!("threads".parse::<ProfileType>(), Ok(ProfileType::Goroutine));
        assert_eq!("unknown".parse::<ProfileType>(), Ok(ProfileType::Other));
    }

    #[test]
    fn test_profile_type_as_str() {
        assert_eq!(ProfileType::Cpu.as_str(), "cpu");
        assert_eq!(ProfileType::Memory.as_str(), "memory");
        assert_eq!(ProfileType::Goroutine.as_str(), "goroutine");
        assert_eq!(ProfileType::Other.as_str(), "other");
    }

    fn create_test_profile(profile_type: ProfileType) -> Profile {
        Profile {
            id: ProfileId::new(),
            timestamp: Timestamp::now().unwrap(),
            profile_type: profile_type as u8,
            sample_type: "samples".to_string(),
            sample_unit: "count".to_string(),
            duration_nanos: 1_000_000_000,
            period: 10_000_000,
            period_type: "cpu".to_string(),
            period_unit: "nanoseconds".to_string(),
            resource_id: 0,
            scope_id: 0,
            original_format: Some("pprof".to_string()),
            attributes: HashMap::new(),
            data: vec![1, 2, 3, 4, 5],
            trace_id: None,
        }
    }

    #[test]
    fn test_profile_has_trace_context() {
        let mut profile = create_test_profile(ProfileType::Cpu);
        assert!(!profile.has_trace_context());

        profile.trace_id = Some(TraceId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        ]));
        assert!(profile.has_trace_context());
    }

    #[test]
    fn test_profile_data_size() {
        let profile = create_test_profile(ProfileType::Cpu);
        assert_eq!(profile.data_size(), 5);
    }
}
