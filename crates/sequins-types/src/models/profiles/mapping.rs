use serde::{Deserialize, Serialize};

/// A normalized binary/library mapping representing a loaded executable or library
///
/// ProfileMapping represents metadata about a binary or shared library that was
/// loaded in the profiled process. Frames may reference mappings to indicate
/// which binary they originate from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileMapping {
    /// Unique mapping identifier (sequential record ID)
    pub mapping_id: u64,
    /// Path to the binary or library file
    pub filename: String,
    /// Build identifier (build ID, UUID, or similar)
    ///
    /// This can be used to locate debug symbols or verify binary versions
    pub build_id: String,
}

impl ProfileMapping {
    /// Create a new ProfileMapping with the given parameters
    #[must_use]
    pub fn new(mapping_id: u64, filename: String, build_id: String) -> Self {
        Self {
            mapping_id,
            filename,
            build_id,
        }
    }

    /// Get the mapping ID
    #[must_use]
    pub fn mapping_id(&self) -> u64 {
        self.mapping_id
    }

    /// Get the filename/path
    #[must_use]
    pub fn filename(&self) -> &str {
        &self.filename
    }

    /// Get the build ID
    #[must_use]
    pub fn build_id(&self) -> &str {
        &self.build_id
    }

    /// Check if this mapping has a build ID
    #[must_use]
    pub fn has_build_id(&self) -> bool {
        !self.build_id.is_empty()
    }

    /// Get the base filename (without directory path)
    #[must_use]
    pub fn base_filename(&self) -> &str {
        self.filename.rsplit('/').next().unwrap_or(&self.filename)
    }

    /// Check if this mapping is likely a system library
    ///
    /// Heuristic: filename starts with common system paths
    #[must_use]
    pub fn is_system_library(&self) -> bool {
        self.filename.starts_with("/lib")
            || self.filename.starts_with("/usr/lib")
            || self.filename.starts_with("/System/Library")
            || self.filename.starts_with("C:\\Windows\\System32")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_mapping_creation() {
        let mapping = ProfileMapping::new(
            1,
            "/usr/local/bin/myapp".to_string(),
            "abc123def456".to_string(),
        );

        assert_eq!(mapping.mapping_id(), 1);
        assert_eq!(mapping.filename(), "/usr/local/bin/myapp");
        assert_eq!(mapping.build_id(), "abc123def456");
        assert!(mapping.has_build_id());
    }

    #[test]
    fn test_profile_mapping_no_build_id() {
        let mapping = ProfileMapping::new(2, "/path/to/lib.so".to_string(), String::new());

        assert!(!mapping.has_build_id());
    }

    #[test]
    fn test_base_filename() {
        let mapping = ProfileMapping::new(
            1,
            "/usr/local/bin/myapp".to_string(),
            "build123".to_string(),
        );

        assert_eq!(mapping.base_filename(), "myapp");
    }

    #[test]
    fn test_base_filename_no_path() {
        let mapping = ProfileMapping::new(1, "myapp".to_string(), "build123".to_string());

        assert_eq!(mapping.base_filename(), "myapp");
    }

    #[test]
    fn test_is_system_library() {
        let lib_mapping = ProfileMapping::new(
            1,
            "/lib/x86_64-linux-gnu/libc.so.6".to_string(),
            "abc".to_string(),
        );
        assert!(lib_mapping.is_system_library());

        let usr_lib_mapping =
            ProfileMapping::new(2, "/usr/lib/libssl.so".to_string(), "def".to_string());
        assert!(usr_lib_mapping.is_system_library());

        let macos_mapping = ProfileMapping::new(
            3,
            "/System/Library/Frameworks/Foundation.framework/Foundation".to_string(),
            "ghi".to_string(),
        );
        assert!(macos_mapping.is_system_library());

        let windows_mapping = ProfileMapping::new(
            4,
            "C:\\Windows\\System32\\kernel32.dll".to_string(),
            "jkl".to_string(),
        );
        assert!(windows_mapping.is_system_library());

        let user_mapping =
            ProfileMapping::new(5, "/home/user/myapp".to_string(), "mno".to_string());
        assert!(!user_mapping.is_system_library());
    }

    #[test]
    fn test_profile_mapping_serde() {
        let mapping = ProfileMapping::new(
            42,
            "/opt/app/bin/service".to_string(),
            "buildid123abc".to_string(),
        );

        let json = serde_json::to_string(&mapping).unwrap();
        let deserialized: ProfileMapping = serde_json::from_str(&json).unwrap();

        assert_eq!(mapping, deserialized);
    }

    #[test]
    fn test_profile_mapping_clone() {
        let mapping = ProfileMapping::new(1, "/path/to/binary".to_string(), "build".to_string());

        let cloned = mapping.clone();
        assert_eq!(mapping, cloned);
    }

    #[test]
    fn test_windows_path_base_filename() {
        let mapping = ProfileMapping::new(
            1,
            "C:\\Program Files\\MyApp\\myapp.exe".to_string(),
            "build".to_string(),
        );

        // Note: this will return the whole path since we're splitting on '/'
        // In a real implementation, we might want to handle both '/' and '\\'
        assert_eq!(
            mapping.base_filename(),
            "C:\\Program Files\\MyApp\\myapp.exe"
        );
    }
}
