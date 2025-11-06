use super::ProfileId;
use crate::models::{Timestamp, TraceId};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Profile data from pprof format
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    pub id: ProfileId,
    pub timestamp: Timestamp,
    pub service_name: String,
    pub profile_type: ProfileType,
    pub sample_type: String,
    pub sample_unit: String,
    pub data: Vec<u8>,
    pub trace_id: Option<TraceId>,
}

/// Profile type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProfileType {
    Cpu,
    Memory,
    Goroutine,
    Other,
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
            service_name: "test-service".to_string(),
            profile_type,
            sample_type: "samples".to_string(),
            sample_unit: "count".to_string(),
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
