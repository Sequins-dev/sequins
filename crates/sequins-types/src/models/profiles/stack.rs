use serde::{Deserialize, Serialize};

/// A normalized stack representing an ordered sequence of frames
///
/// ProfileStack is a deduplicated call stack composed of frame IDs.
/// The frames are ordered from bottom (earliest call) to top (most recent call).
/// Multiple samples may reference the same stack.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileStack {
    /// Unique stack identifier (sequential record ID)
    pub stack_id: u64,
    /// Ordered frame IDs from bottom (root) to top (leaf)
    ///
    /// Order: frame_ids[0] is the outermost/earliest call,
    /// frame_ids[len-1] is the innermost/most recent call
    pub frame_ids: Vec<u64>,
}

impl ProfileStack {
    /// Create a new ProfileStack with the given ID and frame IDs
    #[must_use]
    pub fn new(stack_id: u64, frame_ids: Vec<u64>) -> Self {
        Self {
            stack_id,
            frame_ids,
        }
    }

    /// Get the stack ID
    #[must_use]
    pub fn stack_id(&self) -> u64 {
        self.stack_id
    }

    /// Get all frame IDs in order (bottom to top)
    #[must_use]
    pub fn frame_ids(&self) -> &[u64] {
        &self.frame_ids
    }

    /// Get the depth (number of frames) in this stack
    #[must_use]
    pub fn depth(&self) -> usize {
        self.frame_ids.len()
    }

    /// Get the leaf (top-most, most recent) frame ID if present
    #[must_use]
    pub fn leaf_frame_id(&self) -> Option<u64> {
        self.frame_ids.last().copied()
    }

    /// Get the root (bottom-most, earliest) frame ID if present
    #[must_use]
    pub fn root_frame_id(&self) -> Option<u64> {
        self.frame_ids.first().copied()
    }

    /// Check if this stack contains the given frame ID
    #[must_use]
    pub fn contains_frame(&self, frame_id: u64) -> bool {
        self.frame_ids.contains(&frame_id)
    }

    /// Check if this stack is empty (has no frames)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frame_ids.is_empty()
    }

    /// Get a slice of frame IDs from bottom up to the specified depth
    ///
    /// If depth exceeds stack size, returns all frames
    #[must_use]
    pub fn frames_up_to_depth(&self, depth: usize) -> &[u64] {
        let end = depth.min(self.frame_ids.len());
        &self.frame_ids[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_stack_creation() {
        let stack = ProfileStack::new(1, vec![10, 20, 30, 40]);

        assert_eq!(stack.stack_id(), 1);
        assert_eq!(stack.frame_ids(), &[10, 20, 30, 40]);
        assert_eq!(stack.depth(), 4);
    }

    #[test]
    fn test_leaf_and_root_frames() {
        let stack = ProfileStack::new(1, vec![100, 200, 300]);

        assert_eq!(stack.root_frame_id(), Some(100));
        assert_eq!(stack.leaf_frame_id(), Some(300));
    }

    #[test]
    fn test_empty_stack() {
        let stack = ProfileStack::new(1, vec![]);

        assert!(stack.is_empty());
        assert_eq!(stack.depth(), 0);
        assert_eq!(stack.root_frame_id(), None);
        assert_eq!(stack.leaf_frame_id(), None);
    }

    #[test]
    fn test_single_frame_stack() {
        let stack = ProfileStack::new(1, vec![42]);

        assert!(!stack.is_empty());
        assert_eq!(stack.depth(), 1);
        assert_eq!(stack.root_frame_id(), Some(42));
        assert_eq!(stack.leaf_frame_id(), Some(42));
    }

    #[test]
    fn test_contains_frame() {
        let stack = ProfileStack::new(1, vec![10, 20, 30, 40]);

        assert!(stack.contains_frame(10));
        assert!(stack.contains_frame(20));
        assert!(stack.contains_frame(30));
        assert!(stack.contains_frame(40));
        assert!(!stack.contains_frame(50));
    }

    #[test]
    fn test_frames_up_to_depth() {
        let stack = ProfileStack::new(1, vec![1, 2, 3, 4, 5]);

        let empty: &[u64] = &[];
        assert_eq!(stack.frames_up_to_depth(0), empty);
        assert_eq!(stack.frames_up_to_depth(2), &[1u64, 2u64][..]);
        assert_eq!(
            stack.frames_up_to_depth(5),
            &[1u64, 2u64, 3u64, 4u64, 5u64][..]
        );
        assert_eq!(
            stack.frames_up_to_depth(10),
            &[1u64, 2u64, 3u64, 4u64, 5u64][..]
        );
    }

    #[test]
    fn test_profile_stack_serde() {
        let stack = ProfileStack::new(42, vec![100, 200, 300, 400]);

        let json = serde_json::to_string(&stack).unwrap();
        let deserialized: ProfileStack = serde_json::from_str(&json).unwrap();

        assert_eq!(stack, deserialized);
    }

    #[test]
    fn test_profile_stack_clone() {
        let stack = ProfileStack::new(1, vec![10, 20, 30]);

        let cloned = stack.clone();
        assert_eq!(stack, cloned);
    }

    #[test]
    fn test_stack_ordering() {
        // Verify that the order is preserved: bottom (root) to top (leaf)
        let stack = ProfileStack::new(1, vec![1, 2, 3]);

        let frames = stack.frame_ids();
        assert_eq!(frames[0], 1); // Root/bottom
        assert_eq!(frames[1], 2); // Middle
        assert_eq!(frames[2], 3); // Leaf/top
    }
}
