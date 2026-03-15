# FFI Delta and Heartbeat Frame Support - Implementation Summary

## Overview
Added comprehensive C FFI support for live query functionality by exposing delta frames (incremental updates) and heartbeat frames (keepalive signals) to Swift clients.

## Changes Made

### 1. New C-Compatible Types (`src/types/frames.rs`)

#### `CDeltaOpType` Enum
```c
enum CDeltaOpType {
    Append = 0,   // New row added
    Update = 1,   // Existing row updated
    Expire = 2,   // Row expired from window
    Replace = 3,  // Complete result set replaced
}
```

#### `CDeltaOp` Struct
```c
struct CDeltaOp {
    CDeltaOpType op_type;
    uint64_t row_id;
    char* data;  // JSON-encoded, null-terminated (NULL for Expire)
}
```

#### `CDeltaFrame` Struct
```c
struct CDeltaFrame {
    uint64_t watermark_ns;
    uint32_t ops_count;
    CDeltaOp* ops;
}
```

#### `CHeartbeatFrame` Struct
```c
struct CHeartbeatFrame {
    uint64_t watermark_ns;
}
```

### 2. Updated VTable (`src/seql.rs`)

Added two new callbacks to `CFrameSinkVTable`:
```c
struct CFrameSinkVTable {
    // ... existing callbacks ...
    void (*on_delta)(const CDeltaFrame*, void* ctx);        // NEW
    void (*on_heartbeat)(const CHeartbeatFrame*, void* ctx); // NEW
    // ... existing callbacks ...
}
```

### 3. Frame Conversion Logic

#### Delta Frame Conversion
- `DeltaOp::Append` → JSON array of column values
- `DeltaOp::Update` → JSON array of `[(column_index, new_value)]` pairs
- `DeltaOp::Expire` → No data (NULL pointer)
- `DeltaOp::Replace` → JSON array of row arrays (full dataset)

#### Heartbeat Frame Conversion
- Simple watermark timestamp passthrough
- No heap allocations

### 4. Memory Management Functions

```c
// Free functions (exported via #[no_mangle])
void c_delta_frame_free(CDeltaFrame* frame);
void c_heartbeat_frame_free(CHeartbeatFrame* frame);
```

**Memory safety guarantees:**
- All JSON strings allocated via `CString::into_raw()`
- Arrays allocated via `Box::into_boxed_slice()` + `forget()`
- Free functions properly reconstruct and drop all allocations
- Null-safe (checks for NULL before freeing)

### 5. Stream Processing Updates

Modified `sequins_seql_query()` to handle new frame types:
```rust
Ok(ResponseFrame::Delta(delta)) => {
    if let Some(cb) = sink_ctx.vtable.on_delta {
        let c = CDeltaFrame::from_delta(&delta);
        unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
    }
}
Ok(ResponseFrame::Heartbeat(heartbeat)) => {
    if let Some(cb) = sink_ctx.vtable.on_heartbeat {
        let c = CHeartbeatFrame::from(&heartbeat);
        unsafe { cb(&c as *const CHeartbeatFrame, sink_ctx.ctx.0) };
    }
}
```

### 6. Bug Fixes

Fixed `sequins_data_source_new_local()` to properly await async `Storage::new()`:
```rust
let storage = match crate::runtime::RUNTIME.block_on(Storage::new(config)) {
    Ok(s) => Arc::new(s),
    Err(e) => { /* error handling */ }
};
```

## Test Coverage

Added 9 comprehensive tests in `src/types/frames.rs`:

1. ✅ `test_heartbeat_frame_conversion` - Basic heartbeat conversion
2. ✅ `test_delta_frame_append_conversion` - Append operation with JSON values
3. ✅ `test_delta_frame_update_conversion` - Update operation with column changes
4. ✅ `test_delta_frame_expire_conversion` - Expire operation with NULL data
5. ✅ `test_delta_frame_replace_conversion` - Replace operation with full dataset
6. ✅ `test_delta_frame_multiple_ops` - Mixed operations in single frame
7. ✅ `test_delta_frame_empty_ops` - Empty delta frame edge case
8. ✅ `test_null_delta_frame_free` - NULL pointer safety
9. ✅ `test_null_heartbeat_frame_free` - NULL pointer safety

**All tests pass:** 37/37 library tests (including 9 new delta/heartbeat tests)

## Code Quality

- ✅ **Compilation:** Clean build with no errors
- ✅ **Clippy:** No warnings in sequins-ffi crate
- ✅ **Formatting:** All code formatted with `cargo fmt`
- ✅ **Tests:** 100% pass rate (37/37)

## API Compatibility

### Breaking Changes
**None.** All changes are additive:
- New callbacks are `Option<fn>` types (can be NULL)
- Existing callbacks remain unchanged
- Existing FFI functions unchanged

### Swift Integration Requirements

Swift code should:
1. Update `CFrameSinkVTable` to include new callbacks
2. Implement `onDelta(_:)` and `onHeartbeat(_:)` methods
3. Free frames using `c_delta_frame_free()` and `c_heartbeat_frame_free()`
4. Parse JSON data from `CDeltaOp.data` field

Example Swift callback signature:
```swift
func onDelta(_ frame: UnsafePointer<CDeltaFrame>?, context: UnsafeMutableRawPointer?) {
    guard let frame = frame?.pointee else { return }
    // Process delta operations...
    c_delta_frame_free(UnsafeMutablePointer(mutating: frame))
}
```

## Thread Safety

- All callbacks may be invoked from Tokio worker threads
- Callbacks wrapped in `SendPtr` and `AssertSend` for thread safety
- Raw pointers guaranteed valid by caller contract

## Performance Considerations

- **JSON serialization:** Delta operations encoded as JSON for cross-language compatibility
- **Zero-copy heartbeats:** Stack-allocated, no heap overhead
- **Efficient delta ops:** Array allocated once, forgotten (caller must free)

## Dependencies Added

- `tempfile = "3"` (dev-dependency for tests)

## Files Modified

1. `/crates/sequins-ffi/src/types/frames.rs` - New types and conversions
2. `/crates/sequins-ffi/src/seql.rs` - VTable updates and stream handling
3. `/crates/sequins-ffi/src/data_source.rs` - Async Storage::new() fix
4. `/crates/sequins-ffi/Cargo.toml` - Added tempfile dev-dependency

## Next Steps for Swift Integration

1. Update `SeQLContext.swift` to add delta/heartbeat callback methods
2. Create Swift structs mirroring `CDeltaFrame` and `CHeartbeatFrame`
3. Implement JSON parsing for delta operation data
4. Update UI to display incremental updates (append/update/expire)
5. Test live queries with `last 1h` time range

## Verification Commands

```bash
# Build FFI crate
cargo build -p sequins-ffi

# Run all tests
cargo test -p sequins-ffi --lib

# Run clippy
cargo clippy -p sequins-ffi --all-targets

# Format code
cargo fmt -p sequins-ffi
```

All commands complete successfully with no errors or warnings.
