# Sequins LLDB Debugger

**Purpose:** Debug segfaults, deadlocks, and unresponsive behavior using LLDB.

**When to use:**
- Application crashes with segfault or panic
- Application hangs or becomes unresponsive
- Deadlock suspected
- Need to inspect memory, threads, or stack traces

**Invocation:** `sequins-lldb-debugger` when crashes or hangs occur

---

## What This Skill Does

Provides systematic debugging workflows for common Sequins failure modes:
1. **Segfaults** - Memory safety violations (rare in safe Rust, check FFI/unsafe)
2. **Panics** - Rust panics (unwrap, expect, assert failures)
3. **Deadlocks** - Threads waiting on each other
4. **Hangs** - Infinite loops or slow operations
5. **Memory issues** - Leaks or excessive usage

---

## Prerequisites

### Build with Debug Symbols

```bash
# Debug build (symbols included by default)
cargo build

# Or release build with debug info
cargo build --release
# And add to Cargo.toml:
[profile.release]
debug = true
```

### Install LLDB (macOS)

```bash
# LLDB comes with Xcode Command Line Tools
xcode-select --install

# Verify
lldb --version
```

---

## Debugging Workflow

### Scenario 1: Application Crashes (Segfault/Panic)

#### Step 1: Reproduce with LLDB

```bash
# Run under LLDB
lldb target/debug/sequins-app

# Or for daemon
lldb target/debug/sequins-daemon

# In LLDB prompt:
(lldb) run

# If it needs arguments:
(lldb) run --config ./config.toml
```

**Wait for crash...**

#### Step 2: Examine Crash Location

```lldb
# See where it crashed
(lldb) bt
(lldb) thread backtrace all

# See specific frame
(lldb) frame select 0
(lldb) frame variable

# See source code around crash
(lldb) source list
```

#### Step 3: Inspect Variables

```lldb
# Print variable values
(lldb) print variable_name
(lldb) p variable_name

# Print with type info
(lldb) p/x pointer_value  # Hex format
(lldb) p/t value          # Binary format

# Inspect struct fields
(lldb) p my_struct
(lldb) p my_struct.field_name
```

#### Step 4: Check for Common Issues

**Null pointer dereference:**
```lldb
(lldb) p pointer_name
# If output is "0x0" or NULL, that's the issue
```

**Out of bounds access:**
```lldb
(lldb) p array_name
(lldb) p index
# Check if index >= array.len()
```

**Invalid memory:**
```lldb
(lldb) memory read --size 8 --format x --count 4 address
# Check if memory looks corrupt
```

---

### Scenario 2: Application Hangs

#### Step 1: Attach to Running Process

```bash
# Find PID
ps aux | grep sequins

# Attach LLDB
lldb -p <PID>

# Or
lldb
(lldb) attach --pid <PID>
```

#### Step 2: Check All Threads

```lldb
# List all threads
(lldb) thread list

# See all backtraces
(lldb) thread backtrace all

# Example output:
# thread #1: tid = 0x1234, main thread, stopped
# thread #2: tid = 0x5678, tokio-runtime-worker, blocked
# thread #3: tid = 0x9abc, tokio-runtime-worker, blocked
```

#### Step 3: Identify Blocking Threads

Look for threads in these states:
- `park` / `park_timeout` - Waiting on condition
- `pthread_cond_wait` - Waiting on condition variable
- `futex` - Waiting on mutex/lock
- `select` / `poll` / `epoll_wait` - Waiting on IO

```lldb
# Switch to suspicious thread
(lldb) thread select 2

# See backtrace
(lldb) bt

# See local variables
(lldb) frame variable
```

---

### Scenario 3: Deadlock Detection

#### Step 1: Look for Mutex Patterns

```lldb
# Get all backtraces
(lldb) thread backtrace all

# Look for multiple threads blocked on locks:
# Thread 1: pthread_mutex_lock at 0x...
#   frame #3: Storage::flush
# Thread 2: pthread_mutex_lock at 0x...
#   frame #3: Storage::query_traces
```

#### Step 2: Identify Lock Ordering

```lldb
# For each blocked thread, identify:
# 1. Which lock it's waiting on
# 2. Which locks it currently holds

# Switch to thread
(lldb) thread select 1
(lldb) bt

# Look for lock acquisition in backtrace
# std::sync::Mutex::lock
# std::sync::RwLock::write
# parking_lot::Mutex::lock
```

#### Step 3: Check for Circular Wait

Classic deadlock pattern:
- Thread A holds Lock X, waits for Lock Y
- Thread B holds Lock Y, waits for Lock X

Solution: Enforce lock ordering in code.

---

### Scenario 4: Async Runtime Issues

Sequins uses Tokio extensively. Look for:

#### Blocking in Async Context

```lldb
# Thread backtrace showing:
# tokio::runtime::blocking::pool::Spawned::run
#   some_sync_operation()  # <-- Blocking call in async context!
```

**Issue:** Blocking operation in async task

**Fix:** Use `tokio::task::spawn_blocking`

#### Task Not Progressing

```lldb
# Check if task is scheduled
(lldb) thread backtrace all

# Look for:
# tokio::runtime::park::Park::park
# tokio::runtime::scheduler::...
```

**Issue:** Task may be:
- Waiting on channel that never sends
- Waiting on future that never completes
- Deadlocked with another task

---

### Scenario 5: Papaya Lock-Free Issues

Papaya uses epoch-based reclamation, not locks. But issues can still occur:

#### Infinite Loop in Epoch Reclamation

```lldb
# Backtrace showing:
# papaya::HashMap::pin
# papaya::epoch::...
#   <-- Stuck in epoch advancement
```

**Possible causes:**
- Guard not dropped (held across await)
- Too many concurrent operations
- Bug in Papaya itself

#### Check Guard Lifetimes

```lldb
# In frame with Papaya code
(lldb) frame variable

# Look for guards
# guard: papaya::Guard
# Check if it should have been dropped already
```

---

## Common Debugging Commands

### Navigation

```lldb
# List threads
thread list

# Select thread
thread select <num>

# Backtrace current thread
bt

# Backtrace all threads
thread backtrace all

# Select stack frame
frame select <num>

# Move up/down frames
up
down
```

### Inspection

```lldb
# Print variable
print variable_name
p variable_name

# Print with format
p/x value     # Hex
p/t value     # Binary
p/d value     # Decimal

# Frame variables
frame variable
fr v

# Print type
type lookup TypeName
```

### Execution Control

```lldb
# Continue execution
continue
c

# Step over
next
n

# Step into
step
s

# Step out
finish

# Run until line
thread until <line>
```

### Breakpoints

```lldb
# Set breakpoint on function
breakpoint set --name function_name
b function_name

# Set breakpoint on file:line
breakpoint set --file main.rs --line 42
b main.rs:42

# List breakpoints
breakpoint list
br l

# Delete breakpoint
breakpoint delete <num>
```

---

## Sequins-Specific Debugging

### Check Hot Tier State

```lldb
# When stopped in Storage method
(lldb) p self.hot

# Check Papaya map size
(lldb) p self.hot.traces.len()

# Try to access specific key (careful!)
(lldb) call self.hot.traces.get(&some_trace_id)
```

### Check Cold Tier State

```lldb
# DataFusion context
(lldb) p self.cold

# Check Parquet files
(lldb) p self.cold.object_store
```

### Check Tokio Runtime

```lldb
# Find tokio worker threads
(lldb) thread list

# Check runtime state
(lldb) thread backtrace all | grep tokio
```

---

## Debugging Recipes

### Recipe 1: Find Why Thread is Blocked

```lldb
# 1. Attach to process
lldb -p <pid>

# 2. List threads
(lldb) thread list

# 3. For each thread, check backtrace
(lldb) thread select 1
(lldb) bt

# 4. Look for blocking calls:
#    - mutex_lock
#    - cond_wait
#    - recv (channel receive)
#    - park
#    - io operation

# 5. Identify what it's waiting for
(lldb) frame variable
```

### Recipe 2: Debug Panic

```lldb
# Panic will stop at panic handler
(lldb) run

# When it panics:
(lldb) bt

# Find user code in backtrace (ignore std::panicking frames)
# Look for:
#   frame #5: sequins_storage::tiered_storage::query_traces
#   frame #6: tokio::runtime::...

# Select user frame
(lldb) frame select 5

# See panic location
(lldb) source list

# See variables at panic
(lldb) frame variable
```

### Recipe 3: Memory Leak Investigation

```lldb
# Run with memory tracking
# (Requires additional tools like Instruments on macOS)

# Or check allocations in LLDB
(lldb) memory history <address>
```

Consider using:
```bash
# Valgrind (if on Linux)
valgrind --leak-check=full ./target/debug/sequins-app

# macOS Instruments
# Use Allocations or Leaks template
```

---

## Post-Mortem: Core Dumps

### Generate Core Dump

```bash
# macOS: Enable core dumps
ulimit -c unlimited

# Run program
./target/debug/sequins-app

# If it crashes, core dump created at /cores/core.<pid>
```

### Debug Core Dump

```lldb
lldb target/debug/sequins-app --core /cores/core.12345

# Now you can inspect crash state
(lldb) bt
(lldb) thread backtrace all
(lldb) frame variable
```

---

## Integration with Logging

Before debugging, check logs:

```bash
# Run with verbose logging
RUST_LOG=debug cargo run

# Or trace level
RUST_LOG=trace cargo run

# Specific module
RUST_LOG=sequins_storage=trace cargo run
```

Logs often reveal the issue without needing LLDB.

---

## When to Use LLDB vs Other Tools

| Issue | Best Tool | Why |
|-------|-----------|-----|
| Segfault | LLDB | Need exact crash location |
| Panic | Logs + LLDB | Logs show panic message, LLDB shows state |
| Deadlock | LLDB | Need to see all thread states |
| Hang | LLDB | Need to identify blocking operation |
| Memory leak | Instruments/Valgrind | Better memory tracking |
| Logic bug | Debugger + tests | Step through execution |
| Performance | `perf` / FlameGraph | Profiling, not debugging |

---

## Rust-Specific Tips

### Unwinding Panics

By default, Rust panics unwind. To get better backtraces:

```toml
# In Cargo.toml
[profile.dev]
panic = "abort"

[profile.release]
panic = "abort"
```

Now panics abort immediately, easier to debug.

### Pretty Printers

LLDB has limited Rust support. For better formatting:

```bash
# Use rust-lldb wrapper
rust-lldb target/debug/sequins-app
```

Or add Rust LLDB scripts (complex, see Rust docs).

---

## Success Criteria

You've successfully debugged when:

- ✅ Identified exact line where crash occurs
- ✅ Understood why it crashed (null pointer, assertion, etc.)
- ✅ Identified which threads are blocked and why
- ✅ Found root cause of deadlock (lock ordering issue)
- ✅ Determined what task/operation is hanging
- ✅ Can reproduce issue reliably
- ✅ Have fix and verification test

---

## Prevention

After debugging, prevent future issues:

1. **Add tests** - Reproduce the bug in a test
2. **Add assertions** - Catch issues early
3. **Improve logging** - Log more context around failure point
4. **Use debug_assert** - Runtime checks in debug builds
5. **Document unsafe** - If using unsafe, document invariants

---

**Remember:** Most crashes in Sequins will be:
1. Panics from unwrap/expect (use `?` instead)
2. Deadlocks from lock ordering issues (enforce ordering)
3. Hangs from blocking in async (use spawn_blocking)
4. Papaya guard lifetime issues (drop before await)

LLDB helps identify the symptom, but understanding the architecture helps find the root cause.
