//! Direct OTLP profile → Arrow RecordBatch conversion
//!
//! Produces five RecordBatches per batch of OTLP profiles:
//! - Profile metadata (`profile_schema`)
//! - Profile frames (`profile_frames_schema`) — deduplicated by content-addressed frame_id
//! - Profile stacks (`profile_stacks_schema`) — deduplicated by content-addressed stack_id
//! - Profile samples (`profile_samples_schema`)
//! - Profile mappings (`profile_mappings_schema`) — deduplicated by content-addressed mapping_id

use arrow::array::{
    ArrayRef, BooleanArray, Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::profiles::v1development::{
    Profile as OtlpProfile, ProfilesDictionary,
};
use prost::Message;
use sequins_types::arrow_schema::{
    profile_frames_schema, profile_mappings_schema, profile_samples_schema, profile_schema,
    profile_stacks_schema,
};
use sequins_types::models::{ProfileId, ProfileType};
use std::collections::HashMap;
use std::sync::Arc;

/// FNV-1a 64-bit hash for stable content-addressed IDs.
#[inline]
fn fnv1a_64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut hash = OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Output of `otlp_profiles_to_batches`.
pub struct ProfileBatches {
    pub profiles: RecordBatch,
    pub frames: RecordBatch,
    pub stacks: RecordBatch,
    pub samples: RecordBatch,
    pub mappings: RecordBatch,
}

/// Convert a batch of OTLP profiles to five Arrow `RecordBatch`es.
///
/// `items` contains `(OtlpProfile, resource_id, scope_id, service_name)` tuples.
/// `dictionary` is the shared `ProfilesDictionary` for all profiles in the request.
/// Frames, stacks, and mappings are deduplicated within the batch by content-addressed ID.
pub fn otlp_profiles_to_batches(
    items: &[(OtlpProfile, u32, u32, String)],
    dictionary: Option<&ProfilesDictionary>,
) -> Result<ProfileBatches, String> {
    // Profile metadata columns
    let n = items.len();
    let mut profile_ids: Vec<String> = Vec::with_capacity(n);
    let mut timestamps: Vec<i64> = Vec::with_capacity(n);
    let mut service_names: Vec<String> = Vec::with_capacity(n);
    let mut resource_ids: Vec<u32> = Vec::with_capacity(n);
    let mut scope_ids: Vec<u32> = Vec::with_capacity(n);
    let mut profile_types: Vec<String> = Vec::with_capacity(n);
    let mut sample_types: Vec<String> = Vec::with_capacity(n);
    let mut sample_units: Vec<String> = Vec::with_capacity(n);
    let mut trace_ids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut datas: Vec<Vec<u8>> = Vec::with_capacity(n);

    // Frames: deduplicated by frame_id across entire batch
    // frame_id → (function_name, system_name, filename, line, column, mapping_id, inline)
    let mut frame_map: HashMap<u64, FrameData> = HashMap::new();

    // Stacks: deduplicated by stack_id
    // stack_id → Vec<frame_id>
    let mut stack_map: HashMap<u64, Vec<u64>> = HashMap::new();

    // Mappings: deduplicated by mapping_id
    // mapping_id → (filename, build_id)
    let mut mapping_map: HashMap<u64, MappingData> = HashMap::new();

    // Samples (not deduplicated)
    let mut sample_profile_ids: Vec<String> = Vec::new();
    let mut sample_stack_ids: Vec<u64> = Vec::new();
    let mut sample_services: Vec<String> = Vec::new();
    let mut sample_timestamps: Vec<i64> = Vec::new();
    let mut sample_resource_ids: Vec<u32> = Vec::new();
    let mut sample_scope_ids: Vec<u32> = Vec::new();
    let mut sample_value_types: Vec<String> = Vec::new();
    let mut sample_values: Vec<i64> = Vec::new();

    for (otlp_profile, resource_id, scope_id, service_name) in items {
        let string_table: &[String] = dictionary.map(|d| d.string_table.as_slice()).unwrap_or(&[]);

        let get_str = |idx: i32| -> String {
            if idx <= 0 || idx as usize >= string_table.len() {
                String::new()
            } else {
                string_table[idx as usize].clone()
            }
        };

        // Profile ID
        let profile_id = if otlp_profile.profile_id.len() == 16 {
            let bytes: [u8; 16] = otlp_profile.profile_id[..16].try_into().unwrap();
            ProfileId::from_uuid(uuid::Uuid::from_bytes(bytes))
        } else {
            ProfileId::new()
        };
        let profile_id_hex = profile_id.to_hex();

        // Timestamp
        let ts_ns: i64 = if otlp_profile.time_unix_nano > 0 {
            otlp_profile.time_unix_nano as i64
        } else {
            sequins_types::models::Timestamp::now()
                .map(|t| t.as_nanos())
                .unwrap_or(0)
        };

        // Derive profile_type, sample_type, sample_unit
        let (value_type_str, value_unit_str) = if let Some(ref vt) = otlp_profile.sample_type {
            (get_str(vt.type_strindex), get_str(vt.unit_strindex))
        } else {
            ("unknown".to_string(), "unknown".to_string())
        };
        let profile_type = value_type_str
            .parse::<ProfileType>()
            .unwrap_or(ProfileType::Other);

        // Profile data
        let data = if !otlp_profile.original_payload.is_empty() {
            otlp_profile.original_payload.clone()
        } else {
            otlp_profile.encode_to_vec()
        };

        profile_ids.push(profile_id_hex.clone());
        timestamps.push(ts_ns);
        service_names.push(service_name.clone());
        resource_ids.push(*resource_id);
        scope_ids.push(*scope_id);
        profile_types.push(profile_type.as_str().to_string());
        sample_types.push(value_type_str.clone());
        sample_units.push(value_unit_str);
        trace_ids.push(None); // profile-level trace context not extracted from OTLP
        datas.push(data);

        // Process dictionary data (shared across all profiles in the request)
        if let Some(dict) = dictionary {
            // Build function_index → frame_id mapping
            let mut func_idx_to_frame_id: Vec<u64> = Vec::with_capacity(dict.function_table.len());

            for func in &dict.function_table {
                let name = get_str(func.name_strindex);
                let system_name = get_str(func.system_name_strindex);
                let filename = get_str(func.filename_strindex);
                let line = func.start_line;

                // Content-addressed frame_id
                let content = format!("{}\0{}\0{}", name, filename, line);
                let frame_id = fnv1a_64(content.as_bytes());

                func_idx_to_frame_id.push(frame_id);
                frame_map.entry(frame_id).or_insert(FrameData {
                    function_name: name,
                    system_name: if system_name.is_empty() {
                        None
                    } else {
                        Some(system_name)
                    },
                    filename: if filename.is_empty() {
                        None
                    } else {
                        Some(filename)
                    },
                    line: if line == 0 { None } else { Some(line) },
                    column: None,
                    mapping_id: None,
                    inline: false,
                });
            }

            // Build location_index → Vec<frame_id>
            let mut loc_idx_to_frame_ids: Vec<Vec<u64>> =
                Vec::with_capacity(dict.location_table.len());
            for location in &dict.location_table {
                let loc_frame_ids: Vec<u64> = location
                    .line
                    .iter()
                    .filter_map(|line| {
                        let fi = line.function_index as usize;
                        func_idx_to_frame_id.get(fi).copied()
                    })
                    .collect();
                loc_idx_to_frame_ids.push(loc_frame_ids);
            }

            // Build mappings from mapping_table
            for (i, m) in dict.mapping_table.iter().enumerate() {
                let filename = get_str(m.filename_strindex);
                let mapping_id = fnv1a_64(format!("{}{}", i, &filename).as_bytes());
                mapping_map.entry(mapping_id).or_insert(MappingData {
                    filename,
                    build_id: None,
                });
            }

            // Process samples
            for otlp_sample in &otlp_profile.sample {
                let stack_idx = otlp_sample.stack_index as usize;
                let stack_entry = dict.stack_table.get(stack_idx);

                let mut all_frame_ids: Vec<u64> = Vec::new();
                if let Some(stack) = stack_entry {
                    for &loc_idx in &stack.location_indices {
                        let li = loc_idx as usize;
                        if let Some(fids) = loc_idx_to_frame_ids.get(li) {
                            all_frame_ids.extend_from_slice(fids);
                        }
                    }
                }

                // Content-addressed stack_id
                let stack_id = if all_frame_ids.is_empty() {
                    0u64
                } else {
                    let mut stack_content = Vec::with_capacity(all_frame_ids.len() * 8);
                    for &fid in &all_frame_ids {
                        stack_content.extend_from_slice(&fid.to_le_bytes());
                    }
                    fnv1a_64(&stack_content)
                };

                if !all_frame_ids.is_empty() {
                    stack_map.entry(stack_id).or_insert(all_frame_ids);
                }

                let value = otlp_sample.values.first().copied().unwrap_or(0);

                sample_profile_ids.push(profile_id_hex.clone());
                sample_stack_ids.push(stack_id);
                sample_services.push(service_name.clone());
                sample_timestamps.push(ts_ns);
                sample_resource_ids.push(*resource_id);
                sample_scope_ids.push(*scope_id);
                sample_value_types.push(value_type_str.clone());
                sample_values.push(value);
            }
        }
    }

    // Build profile RecordBatch
    let profiles_batch = {
        let schema = profile_schema();
        if n == 0 {
            RecordBatch::new_empty(schema)
        } else {
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(StringViewArray::from(profile_ids)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
                Arc::new(StringViewArray::from(service_names)) as ArrayRef,
                Arc::new(UInt32Array::from(resource_ids)) as ArrayRef,
                Arc::new(UInt32Array::from(scope_ids)) as ArrayRef,
                Arc::new(StringViewArray::from(profile_types)) as ArrayRef,
                Arc::new(StringViewArray::from(sample_types)) as ArrayRef,
                Arc::new(StringViewArray::from(sample_units)) as ArrayRef,
                Arc::new(StringViewArray::from(trace_ids)) as ArrayRef,
                Arc::new(arrow::array::BinaryArray::from_iter_values(
                    datas.iter().map(|v| v.as_slice()),
                )) as ArrayRef,
            ];
            RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())?
        }
    };

    // Build frames RecordBatch
    let frames_batch = {
        let schema = profile_frames_schema();
        let nf = frame_map.len();
        if nf == 0 {
            RecordBatch::new_empty(schema)
        } else {
            let mut frame_ids: Vec<u64> = Vec::with_capacity(nf);
            let mut function_names: Vec<String> = Vec::with_capacity(nf);
            let mut system_names: Vec<Option<String>> = Vec::with_capacity(nf);
            let mut filenames: Vec<Option<String>> = Vec::with_capacity(nf);
            let mut lines: Vec<Option<i64>> = Vec::with_capacity(nf);
            let mut columns: Vec<Option<i64>> = Vec::with_capacity(nf);
            let mut mapping_ids: Vec<Option<u64>> = Vec::with_capacity(nf);
            let mut inlines: Vec<bool> = Vec::with_capacity(nf);

            for (frame_id, fd) in &frame_map {
                frame_ids.push(*frame_id);
                function_names.push(fd.function_name.clone());
                system_names.push(fd.system_name.clone());
                filenames.push(fd.filename.clone());
                lines.push(fd.line);
                columns.push(fd.column);
                mapping_ids.push(fd.mapping_id);
                inlines.push(fd.inline);
            }

            let arrays: Vec<ArrayRef> = vec![
                Arc::new(UInt64Array::from(frame_ids)) as ArrayRef,
                Arc::new(StringViewArray::from(function_names)) as ArrayRef,
                Arc::new(StringViewArray::from(system_names)) as ArrayRef,
                Arc::new(StringViewArray::from(filenames)) as ArrayRef,
                Arc::new(Int64Array::from(lines)) as ArrayRef,
                Arc::new(Int64Array::from(columns)) as ArrayRef,
                Arc::new(UInt64Array::from(mapping_ids)) as ArrayRef,
                Arc::new(BooleanArray::from(inlines)) as ArrayRef,
            ];
            RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())?
        }
    };

    // Build stacks RecordBatch — junction table: one row per (stack_id, frame_id) pair
    // `position` preserves leaf-first ordering from pprof/OTLP (position 0 = leaf frame)
    let stacks_batch = {
        let schema = profile_stacks_schema();
        if stack_map.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            let total_rows: usize = stack_map.values().map(|fids| fids.len()).sum();
            let mut stack_ids: Vec<u64> = Vec::with_capacity(total_rows);
            let mut frame_ids: Vec<u64> = Vec::with_capacity(total_rows);
            let mut positions: Vec<u32> = Vec::with_capacity(total_rows);

            for (stack_id, fids) in &stack_map {
                for (pos, &frame_id) in fids.iter().enumerate() {
                    stack_ids.push(*stack_id);
                    frame_ids.push(frame_id);
                    positions.push(pos as u32);
                }
            }

            let arrays: Vec<ArrayRef> = vec![
                Arc::new(UInt64Array::from(stack_ids)) as ArrayRef,
                Arc::new(UInt64Array::from(frame_ids)) as ArrayRef,
                Arc::new(UInt32Array::from(positions)) as ArrayRef,
            ];
            RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())?
        }
    };

    // Build samples RecordBatch
    let samples_batch = {
        let schema = profile_samples_schema();
        if sample_profile_ids.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(StringViewArray::from(sample_profile_ids)) as ArrayRef,
                Arc::new(UInt64Array::from(sample_stack_ids)) as ArrayRef,
                Arc::new(StringViewArray::from(sample_services)) as ArrayRef,
                Arc::new(Int64Array::from(sample_timestamps)) as ArrayRef,
                Arc::new(UInt32Array::from(sample_resource_ids)) as ArrayRef,
                Arc::new(UInt32Array::from(sample_scope_ids)) as ArrayRef,
                Arc::new(StringViewArray::from(sample_value_types)) as ArrayRef,
                Arc::new(Int64Array::from(sample_values)) as ArrayRef,
            ];
            RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())?
        }
    };

    // Build mappings RecordBatch
    let mappings_batch = {
        let schema = profile_mappings_schema();
        let nm = mapping_map.len();
        if nm == 0 {
            RecordBatch::new_empty(schema)
        } else {
            let mut mapping_ids: Vec<u64> = Vec::with_capacity(nm);
            let mut mapping_filenames: Vec<String> = Vec::with_capacity(nm);
            let mut build_ids: Vec<Option<String>> = Vec::with_capacity(nm);

            for (mapping_id, md) in &mapping_map {
                mapping_ids.push(*mapping_id);
                mapping_filenames.push(md.filename.clone());
                build_ids.push(md.build_id.clone());
            }

            let arrays: Vec<ArrayRef> = vec![
                Arc::new(UInt64Array::from(mapping_ids)) as ArrayRef,
                Arc::new(StringViewArray::from(mapping_filenames)) as ArrayRef,
                Arc::new(StringViewArray::from(build_ids)) as ArrayRef,
            ];
            RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())?
        }
    };

    Ok(ProfileBatches {
        profiles: profiles_batch,
        frames: frames_batch,
        stacks: stacks_batch,
        samples: samples_batch,
        mappings: mappings_batch,
    })
}

struct FrameData {
    function_name: String,
    system_name: Option<String>,
    filename: Option<String>,
    line: Option<i64>,
    column: Option<i64>,
    mapping_id: Option<u64>,
    inline: bool,
}

struct MappingData {
    filename: String,
    build_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::profiles::v1development::Profile as OtlpProfile;
    use sequins_types::arrow_schema::{
        profile_frames_schema, profile_mappings_schema, profile_samples_schema, profile_schema,
        profile_stacks_schema,
    };

    fn make_empty_profile(ts_ns: u64) -> OtlpProfile {
        OtlpProfile {
            profile_id: vec![7u8; 16],
            time_unix_nano: ts_ns,
            ..Default::default()
        }
    }

    #[test]
    fn test_otlp_profiles_to_batches_basic() {
        let profile = make_empty_profile(1_000_000_000);
        let items = vec![(profile, 1u32, 2u32, "my-service".to_string())];

        let batches = otlp_profiles_to_batches(&items, None).unwrap();

        // Profile metadata batch has 1 row
        assert_eq!(batches.profiles.num_rows(), 1);
        assert_eq!(batches.profiles.schema(), profile_schema());

        // service_name column (index 2)
        let services = batches
            .profiles
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(services.value(0), "my-service");

        // Frames/stacks/samples/mappings are empty (no dictionary provided)
        assert_eq!(batches.frames.num_rows(), 0);
        assert_eq!(batches.frames.schema(), profile_frames_schema());

        assert_eq!(batches.stacks.num_rows(), 0);
        assert_eq!(batches.stacks.schema(), profile_stacks_schema());

        assert_eq!(batches.samples.num_rows(), 0);
        assert_eq!(batches.samples.schema(), profile_samples_schema());

        assert_eq!(batches.mappings.num_rows(), 0);
        assert_eq!(batches.mappings.schema(), profile_mappings_schema());
    }

    #[test]
    fn test_otlp_profiles_to_batches_empty_input() {
        let batches = otlp_profiles_to_batches(&[], None).unwrap();

        // All batches should be empty with correct schemas
        assert_eq!(batches.profiles.num_rows(), 0);
        assert_eq!(batches.profiles.schema(), profile_schema());
        assert_eq!(batches.frames.num_rows(), 0);
        assert_eq!(batches.stacks.num_rows(), 0);
        assert_eq!(batches.samples.num_rows(), 0);
        assert_eq!(batches.mappings.num_rows(), 0);
    }
}
