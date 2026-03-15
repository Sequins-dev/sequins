use serde::{Deserialize, Serialize};

/// A normalized stack frame representing a single call site in a profile
///
/// ProfileFrame is a deduplicated, normalized representation of a stack frame
/// extracted from pprof data. Multiple samples may reference the same frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileFrame {
    /// Unique frame identifier (sequential record ID)
    pub frame_id: u64,
    /// Resolved function/method name (demangled if possible)
    pub function_name: String,
    /// System/mangled name (as it appears in binary)
    pub system_name: String,
    /// Source file path
    pub filename: String,
    /// Line number in source file
    pub line: i64,
    /// Column number in source file
    pub column: i64,
    /// Optional reference to binary mapping
    pub mapping_id: Option<u64>,
    /// True if this frame was inlined by the compiler
    pub inline: bool,
}

impl ProfileFrame {
    /// Create a new ProfileFrame with the given parameters
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        frame_id: u64,
        function_name: String,
        system_name: String,
        filename: String,
        line: i64,
        column: i64,
        mapping_id: Option<u64>,
        inline: bool,
    ) -> Self {
        Self {
            frame_id,
            function_name,
            system_name,
            filename,
            line,
            column,
            mapping_id,
            inline,
        }
    }

    /// Get the frame ID
    #[must_use]
    pub fn frame_id(&self) -> u64 {
        self.frame_id
    }

    /// Get the resolved function name
    #[must_use]
    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    /// Get the system/mangled name
    #[must_use]
    pub fn system_name(&self) -> &str {
        &self.system_name
    }

    /// Get the source filename
    #[must_use]
    pub fn filename(&self) -> &str {
        &self.filename
    }

    /// Get the line number
    #[must_use]
    pub fn line(&self) -> i64 {
        self.line
    }

    /// Get the column number
    #[must_use]
    pub fn column(&self) -> i64 {
        self.column
    }

    /// Get the mapping ID if present
    #[must_use]
    pub fn mapping_id(&self) -> Option<u64> {
        self.mapping_id
    }

    /// Check if this frame is inlined
    #[must_use]
    pub fn is_inline(&self) -> bool {
        self.inline
    }

    /// Check if this frame has source location information
    #[must_use]
    pub fn has_source_location(&self) -> bool {
        !self.filename.is_empty() && self.line > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_frame_creation() {
        let frame = ProfileFrame::new(
            1,
            "my_function".to_string(),
            "_ZN11my_function17h1234567890abcdefE".to_string(),
            "src/main.rs".to_string(),
            42,
            10,
            Some(100),
            false,
        );

        assert_eq!(frame.frame_id(), 1);
        assert_eq!(frame.function_name(), "my_function");
        assert_eq!(frame.system_name(), "_ZN11my_function17h1234567890abcdefE");
        assert_eq!(frame.filename(), "src/main.rs");
        assert_eq!(frame.line(), 42);
        assert_eq!(frame.column(), 10);
        assert_eq!(frame.mapping_id(), Some(100));
        assert!(!frame.is_inline());
    }

    #[test]
    fn test_profile_frame_inline() {
        let frame = ProfileFrame::new(
            2,
            "inlined_fn".to_string(),
            "inlined_fn".to_string(),
            "src/lib.rs".to_string(),
            100,
            5,
            None,
            true,
        );

        assert!(frame.is_inline());
        assert_eq!(frame.mapping_id(), None);
    }

    #[test]
    fn test_has_source_location() {
        let with_location = ProfileFrame::new(
            1,
            "func".to_string(),
            "func".to_string(),
            "file.rs".to_string(),
            10,
            0,
            None,
            false,
        );
        assert!(with_location.has_source_location());

        let no_filename = ProfileFrame::new(
            2,
            "func".to_string(),
            "func".to_string(),
            String::new(),
            10,
            0,
            None,
            false,
        );
        assert!(!no_filename.has_source_location());

        let no_line = ProfileFrame::new(
            3,
            "func".to_string(),
            "func".to_string(),
            "file.rs".to_string(),
            0,
            0,
            None,
            false,
        );
        assert!(!no_line.has_source_location());
    }

    #[test]
    fn test_profile_frame_serde() {
        let frame = ProfileFrame::new(
            42,
            "test_function".to_string(),
            "_Z13test_functionv".to_string(),
            "test.cpp".to_string(),
            123,
            45,
            Some(789),
            true,
        );

        let json = serde_json::to_string(&frame).unwrap();
        let deserialized: ProfileFrame = serde_json::from_str(&json).unwrap();

        assert_eq!(frame, deserialized);
    }

    #[test]
    fn test_profile_frame_clone() {
        let frame = ProfileFrame::new(
            1,
            "fn".to_string(),
            "fn".to_string(),
            "file.rs".to_string(),
            10,
            5,
            None,
            false,
        );

        let cloned = frame.clone();
        assert_eq!(frame, cloned);
    }
}
