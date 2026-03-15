use serde::{Deserialize, Serialize};

/// A single resolved stack frame with symbolic information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StackFrame {
    /// Function or method name
    pub function_name: String,
    /// Source file path (if available)
    pub file: Option<String>,
    /// Line number in source file (if available)
    pub line: Option<u32>,
    /// Module or library name (if available)
    pub module: Option<String>,
}
