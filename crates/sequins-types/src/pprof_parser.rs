//! pprof binary format parser
//!
//! Parses pprof protobuf data and returns fully resolved [`ProfileSample`]s
//! with symbolic information (function name, file, line) inline.
//!
//! This is the query-path parser — it resolves symbols inline and does not
//! perform deduplication. The write-path in sequins-pro uses its own
//! SymbolTable/StackDictionary for efficient Vortex storage.

use crate::error::{Error, Result};
use crate::models::{ProfileId, ProfileSample, ProfileType, StackFrame, Timestamp, TraceId};
use std::collections::HashMap;

/// Parse pprof binary data and return fully resolved [`ProfileSample`]s.
///
/// Self-contained — resolves symbols inline from pprof's own string/function/location tables.
///
/// # Arguments
///
/// * `profile_id` - ID of the originating profile record
/// * `service_name` - Name of the service that produced the profile
/// * `profile_type` - High-level profile type (cpu, memory, etc.)
/// * `data` - Raw pprof protobuf bytes
/// * `timestamp` - When the profile was captured
/// * `trace_id` - Optional associated trace ID
///
/// # Errors
///
/// Returns an error if the pprof data cannot be decoded.
pub fn parse_pprof_to_samples(
    profile_id: &ProfileId,
    service_name: &str,
    profile_type: &ProfileType,
    data: &[u8],
    timestamp: Timestamp,
    trace_id: Option<&TraceId>,
) -> Result<Vec<ProfileSample>> {
    // Use pprof::protos::Message (prost 0.12 re-export) to avoid conflict with prost 0.14
    use pprof::protos::Message;

    if data.is_empty() {
        return Ok(Vec::new());
    }

    let profile = pprof::protos::Profile::decode(&mut &data[..])
        .map_err(|e| Error::Other(format!("Failed to parse pprof protobuf: {}", e)))?;

    // Helper to get string from string table
    let get_string = |index: i64| -> String {
        if index < 0 || index as usize >= profile.string_table.len() {
            String::new()
        } else {
            profile.string_table[index as usize].clone()
        }
    };

    // Step 1: Build mapping from Function ID → StackFrame
    let mut function_to_frame: HashMap<u64, StackFrame> = HashMap::new();

    for function in &profile.function {
        let function_name = get_string(function.name);
        if function_name.is_empty() {
            continue;
        }

        let file = if function.filename != 0 {
            let f = get_string(function.filename);
            if f.is_empty() {
                None
            } else {
                Some(f)
            }
        } else {
            None
        };

        let module = if function.system_name != 0 {
            let m = get_string(function.system_name);
            if m.is_empty() {
                None
            } else {
                Some(m)
            }
        } else {
            None
        };

        let frame = StackFrame {
            function_name,
            file,
            line: if function.start_line != 0 {
                Some(function.start_line as u32)
            } else {
                None
            },
            module,
        };

        function_to_frame.insert(function.id, frame);
    }

    // Step 2: Build mapping from Location ID → Vec<StackFrame>
    // A location may have multiple lines due to inlining.
    let mut location_to_frames: HashMap<u64, Vec<StackFrame>> = HashMap::new();

    for location in &profile.location {
        let mut frames = Vec::new();

        for line in &location.line {
            if let Some(frame) = function_to_frame.get(&line.function_id) {
                // Override the line number with the line from the location (more precise)
                let mut frame = frame.clone();
                if line.line != 0 {
                    frame.line = Some(line.line as u32);
                }
                frames.push(frame);
            } else {
                // Unknown function — use address as name
                frames.push(StackFrame {
                    function_name: format!("0x{:x}", location.address),
                    file: None,
                    line: if line.line != 0 {
                        Some(line.line as u32)
                    } else {
                        None
                    },
                    module: None,
                });
            }
        }

        // If no line info, create a frame from the address
        if frames.is_empty() && location.address != 0 {
            frames.push(StackFrame {
                function_name: format!("0x{:x}", location.address),
                file: None,
                line: None,
                module: None,
            });
        }

        location_to_frames.insert(location.id, frames);
    }

    // Step 3: Determine value type names
    let value_types: Vec<String> = profile
        .sample_type
        .iter()
        .map(|st| {
            let type_name = get_string(st.ty);
            let unit_name = get_string(st.unit);
            if unit_name.is_empty() {
                type_name
            } else {
                format!("{}/{}", type_name, unit_name)
            }
        })
        .collect();

    // Step 4: Process each sample into ProfileSamples
    let mut samples = Vec::new();

    for sample in &profile.sample {
        // Resolve location IDs to stack frames (leaf → root order preserved)
        let mut stack: Vec<StackFrame> = Vec::new();

        for &location_id in &sample.location_id {
            if let Some(frames) = location_to_frames.get(&location_id) {
                stack.extend(frames.iter().cloned());
            }
        }

        // Skip samples with empty stacks
        if stack.is_empty() {
            continue;
        }

        // Emit one ProfileSample per value type
        for (i, &value) in sample.value.iter().enumerate() {
            if value == 0 {
                continue;
            }

            let value_type_full = if i < value_types.len() {
                value_types[i].clone()
            } else {
                format!("value_{}", i)
            };

            // Split "type/unit" into separate fields
            let (value_type, value_unit) = if let Some(slash_pos) = value_type_full.find('/') {
                (
                    value_type_full[..slash_pos].to_string(),
                    value_type_full[slash_pos + 1..].to_string(),
                )
            } else {
                (value_type_full, String::new())
            };

            samples.push(ProfileSample {
                profile_id: *profile_id,
                timestamp,
                service_name: service_name.to_string(),
                profile_type: *profile_type,
                value_type,
                value_unit,
                value,
                stack: stack.clone(),
                stack_id: 0, // Will be assigned during normalization
                trace_id: trace_id.copied(),
                span_id: None, // Not available in pprof format
                attributes: std::collections::HashMap::new(),
                resource_id: 0,
                scope_id: 0,
            });
        }
    }

    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pprof::protos::Message;

    fn make_test_pprof() -> Vec<u8> {
        // Build a minimal valid pprof Profile using prost 0.12 types
        let profile = pprof::protos::Profile {
            string_table: vec![
                "".to_string(),            // 0: required empty
                "cpu".to_string(),         // 1: sample type name
                "nanoseconds".to_string(), // 2: sample type unit
                "my_function".to_string(), // 3: function name
                "main.rs".to_string(),     // 4: filename
            ],
            sample_type: vec![pprof::protos::ValueType { ty: 1, unit: 2 }],
            function: vec![pprof::protos::Function {
                id: 1,
                name: 3,
                system_name: 0,
                filename: 4,
                start_line: 10,
            }],
            location: vec![pprof::protos::Location {
                id: 1,
                mapping_id: 0,
                address: 0x1000,
                line: vec![pprof::protos::Line {
                    function_id: 1,
                    line: 42,
                }],
                is_folded: false,
            }],
            sample: vec![pprof::protos::Sample {
                location_id: vec![1],
                value: vec![100_000_000],
                ..Default::default()
            }],
            ..Default::default()
        };

        profile.encode_to_vec()
    }

    #[test]
    fn test_parse_basic_pprof() {
        let data = make_test_pprof();
        let profile_id = ProfileId::new();
        let timestamp = Timestamp::from_secs(1000);

        let samples = parse_pprof_to_samples(
            &profile_id,
            "test-service",
            &ProfileType::Cpu,
            &data,
            timestamp,
            None,
        )
        .unwrap();

        assert_eq!(samples.len(), 1);
        let s = &samples[0];
        assert_eq!(s.service_name, "test-service");
        assert_eq!(s.profile_type, ProfileType::Cpu);
        assert_eq!(s.value, 100_000_000);
        assert_eq!(s.value_type, "cpu"); // Value type parsing changed
        assert_eq!(s.stack.len(), 1);
        assert_eq!(s.stack[0].function_name, "my_function");
        assert_eq!(s.stack[0].file.as_deref(), Some("main.rs"));
        assert_eq!(s.stack[0].line, Some(42));
    }

    #[test]
    fn test_parse_empty_data() {
        let profile_id = ProfileId::new();
        let timestamp = Timestamp::from_secs(1000);

        let samples = parse_pprof_to_samples(
            &profile_id,
            "test-service",
            &ProfileType::Cpu,
            &[],
            timestamp,
            None,
        )
        .unwrap();

        assert!(samples.is_empty());
    }

    #[test]
    fn test_parse_with_trace_id() {
        let data = make_test_pprof();
        let profile_id = ProfileId::new();
        let timestamp = Timestamp::from_secs(1000);
        let trace_id = TraceId::from_bytes([1; 16]);

        let samples = parse_pprof_to_samples(
            &profile_id,
            "test-service",
            &ProfileType::Cpu,
            &data,
            timestamp,
            Some(&trace_id),
        )
        .unwrap();

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].trace_id, Some(trace_id));
    }
}
