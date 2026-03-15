use opentelemetry_proto::tonic::{
    collector::profiles::v1development::ExportProfilesServiceRequest,
    common::v1::{any_value::Value as AnyVal, AnyValue, InstrumentationScope, KeyValue as ProtoKV},
    profiles::v1development::{
        Function, Line, Location, Profile as OtlpProfile, ProfilesDictionary, ResourceProfiles,
        Sample, ScopeProfiles, Stack, ValueType,
    },
    resource::v1::Resource as ProtoResource,
};
use pprof::ProfilerGuardBuilder;
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{error, info, warn};

/// Intern a string into the string table, returning its index.
/// Index 0 is always the empty string.
fn intern(table: &mut Vec<String>, map: &mut HashMap<String, i32>, s: &str) -> i32 {
    if let Some(&idx) = map.get(s) {
        return idx;
    }
    let idx = table.len() as i32;
    table.push(s.to_string());
    map.insert(s.to_string(), idx);
    idx
}

fn convert_report_to_otlp(report: &pprof::Report) -> ExportProfilesServiceRequest {
    let mut string_table: Vec<String> = vec!["".to_string()]; // index 0 = empty
    let mut string_map: HashMap<String, i32> = HashMap::new();
    string_map.insert("".to_string(), 0);

    let cpu_idx = intern(&mut string_table, &mut string_map, "cpu");
    let ns_idx = intern(&mut string_table, &mut string_map, "nanoseconds");

    // Function table: index 0 = null entry
    let mut function_table: Vec<Function> = vec![Function::default()];
    // Key: (name, sys_name, filename, lineno)
    let mut fn_map: HashMap<(String, String, String, u32), i32> = HashMap::new();

    // Location table: index 0 = null entry
    let mut location_table: Vec<Location> = vec![Location::default()];
    // One location per unique function (use fn_idx as key)
    let mut loc_map: HashMap<i32, i32> = HashMap::new();

    // Stack table: index 0 = null entry
    let mut stack_table: Vec<Stack> = vec![Stack::default()];

    let frequency = report.timing.frequency;

    let mut samples: Vec<Sample> = Vec::new();

    for (frames, &count) in &report.data {
        if count <= 0 {
            continue;
        }

        // Build location indices for this stack (frames.frames is leaf-first)
        let mut location_indices: Vec<i32> = Vec::new();

        for sym_group in &frames.frames {
            // Each sym_group is a Vec<Symbol> for one frame (possibly inlined)
            let mut lines: Vec<Line> = Vec::new();

            for sym in sym_group {
                let name = sym.name();
                let sys_name = sym.sys_name().into_owned();
                let filename = sym.filename().into_owned();
                let lineno = sym.lineno();

                let fn_key = (name.clone(), sys_name.clone(), filename.clone(), lineno);

                let fn_idx = if let Some(&idx) = fn_map.get(&fn_key) {
                    idx
                } else {
                    let name_idx = intern(&mut string_table, &mut string_map, &name);
                    let sys_idx = intern(&mut string_table, &mut string_map, &sys_name);
                    let file_idx = intern(&mut string_table, &mut string_map, &filename);

                    let idx = function_table.len() as i32;
                    function_table.push(Function {
                        name_strindex: name_idx,
                        system_name_strindex: sys_idx,
                        filename_strindex: file_idx,
                        start_line: lineno as i64,
                    });
                    fn_map.insert(fn_key, idx);
                    idx
                };

                lines.push(Line {
                    function_index: fn_idx,
                    line: lineno as i64,
                    column: 0,
                });
            }

            if lines.is_empty() {
                continue;
            }

            let primary_fn_idx = lines[0].function_index;
            let loc_idx = if let Some(&idx) = loc_map.get(&primary_fn_idx) {
                idx
            } else {
                let idx = location_table.len() as i32;
                location_table.push(Location {
                    mapping_index: 0,
                    address: 0,
                    line: lines,
                    attribute_indices: vec![],
                });
                loc_map.insert(primary_fn_idx, idx);
                idx
            };

            location_indices.push(loc_idx);
        }

        if location_indices.is_empty() {
            continue;
        }

        let stack_idx = stack_table.len() as i32;
        stack_table.push(Stack { location_indices });

        let cpu_ns = count as i64 * 1_000_000_000 / frequency as i64;
        samples.push(Sample {
            stack_index: stack_idx,
            values: vec![cpu_ns],
            ..Default::default()
        });
    }

    let start_ns = report
        .timing
        .start_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let duration_ns = report.timing.duration.as_nanos() as u64;

    // 16-byte profile_id from timestamp
    let ts = start_ns.to_le_bytes();
    let ts2 = start_ns.wrapping_add(0xdeadbeef).to_le_bytes();
    let profile_id: Vec<u8> = ts.iter().chain(ts2.iter()).copied().collect();

    let period = if frequency > 0 {
        1_000_000_000i64 / frequency as i64
    } else {
        10_000_000
    };

    let profile = OtlpProfile {
        sample_type: Some(ValueType {
            type_strindex: cpu_idx,
            unit_strindex: ns_idx,
            aggregation_temporality: 0,
        }),
        sample: samples,
        time_unix_nano: start_ns,
        duration_nano: duration_ns,
        period_type: Some(ValueType {
            type_strindex: cpu_idx,
            unit_strindex: ns_idx,
            aggregation_temporality: 0,
        }),
        period,
        profile_id,
        ..Default::default()
    };

    let resource = ProtoResource {
        attributes: vec![ProtoKV {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(AnyVal::StringValue("rust-test-app".to_string())),
            }),
        }],
        ..Default::default()
    };

    ExportProfilesServiceRequest {
        resource_profiles: vec![ResourceProfiles {
            resource: Some(resource),
            scope_profiles: vec![ScopeProfiles {
                scope: Some(InstrumentationScope {
                    name: "rust-test-app".to_string(),
                    ..Default::default()
                }),
                profiles: vec![profile],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
        dictionary: Some(ProfilesDictionary {
            mapping_table: vec![],
            location_table,
            function_table,
            link_table: vec![],
            string_table,
            stack_table,
            attribute_table: vec![],
        }),
    }
}

async fn export_report(report: &pprof::Report) {
    let request = convert_report_to_otlp(report);
    let bytes = request.encode_to_vec();

    let client = reqwest::Client::new();
    match client
        .post("http://localhost:4318/v1development/profiles")
        .header("Content-Type", "application/x-protobuf")
        .body(bytes)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                info!(samples = report.data.len(), "Profile exported successfully");
            } else {
                warn!(status = %resp.status(), "Profile export returned non-success status");
            }
        }
        Err(e) => error!(error = %e, "Failed to export profile"),
    }
}

pub async fn run(shutdown: Arc<AtomicBool>) {
    info!("Profiler started (10s capture interval, 99 Hz)");

    while !shutdown.load(Ordering::Relaxed) {
        let guard = match ProfilerGuardBuilder::default().frequency(99).build() {
            Ok(g) => g,
            Err(e) => {
                error!(error = %e, "Failed to start profiler guard");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        // Sleep 10s while traffic does real CPU work; check shutdown every 500ms
        for _ in 0..20 {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        match guard.report().build() {
            Ok(report) => {
                if report.data.is_empty() {
                    info!("No CPU samples captured this interval");
                } else {
                    export_report(&report).await;
                }
            }
            Err(e) => error!(error = %e, "Failed to build profiler report"),
        }
    }

    info!("Profiler shutting down");
}
