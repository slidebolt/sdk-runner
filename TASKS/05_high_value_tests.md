# High-Value Test Strategy

## Overview

This document identifies high-value tests that should exist for the SDK Runner, assuming the application is 100% working as designed. High-value tests protect critical functionality, prevent regressions in complex areas, and verify integration between components.

**Current Test Coverage**: ~1,250 lines across 9 test files
**Target**: Comprehensive coverage of critical paths, concurrency, and integration

---

## Critical Test Categories (High Priority)

### 1. Plugin Lifecycle Integration Tests

**Why Critical**: The plugin lifecycle is the foundation. Failures here break everything.

**Existing Coverage**: ❌ None - only unit tests for individual methods

**Required Tests**:

```go
// TestRunner_PluginLifecycle_Success
// Verifies: OnInitialize → OnReady → WaitReady → [operations] → OnShutdown
// - State properly passed through lifecycle
// - Manifest correctly initialized
// - Graceful shutdown cleanup

// TestRunner_PluginLifecycle_WaitReadyTimeout
// Verifies: WaitReady timeout handling doesn't block forever
// - Plugin that never becomes ready is handled gracefully

// TestRunner_PluginLifecycle_InitializeFailure
// Verifies: Plugin initialization errors are propagated
// - Runner doesn't proceed if OnInitialize fails

// TestRunner_PluginLifecycle_HealthCheckFailure
// Verifies: Health check responses are properly returned
// - Plugin errors in health check are handled
```

**Test Location**: `runner_lifecycle_test.go`
**Estimated Lines**: 300-400

---

### 2. Device/Entity CRUD End-to-End Tests

**Why Critical**: Core functionality. Breaks user workflows if broken.

**Existing Coverage**: ⚠️ Partial - fallback tests exist, but not full flow

**Required Tests**:

```go
// TestDevice_Create_FullFlow
// Verifies: RPC → handler → plugin.OnDeviceCreate → persistence → response
// - Device persisted correctly
// - Response contains correct data
// - Entities automatically created if needed

// TestDevice_Create_WithFallback
// Verifies: When plugin rejects, synthetic device is created
// - Fallback device has correct fields populated

// TestDevice_Update_FullFlow
// Verifies: Merge with existing → plugin.OnDeviceUpdate → persistence
// - Source fields vs user fields properly reconciled
// - Labels merged correctly

// TestDevice_Delete_FullFlow
// Verifies: plugin.OnDeviceDelete called → entity cleanup → persistence removal
// - All entities for device removed
// - Raw data cleaned up

// TestDevice_List_FullFlow
// Verifies: plugin.OnDeviceDiscover → reconcile → persist → return
// - Discovery results merged with existing
// - Removed devices handled correctly

// TestEntity_Create_FullFlow
// Verifies: Entity creation with automatic device creation
// - Parent device created if missing
// - Entity fields properly set

// TestEntity_Update_FullFlow
// Verifies: Entity merge and update
// - Domain/actions preserved correctly
// - LocalName not overwritten

// TestEntity_List_FullFlow
// Verifies: plugin.OnEntityDiscover → reconcile → persist
// - Per-device entity discovery
// - DeviceID association maintained
```

**Test Location**: `runner_crud_test.go`
**Estimated Lines**: 400-500

---

### 3. Command Processing Integration Tests

**Why Critical**: Commands are the primary user interaction. Failures prevent device control.

**Existing Coverage**: ⚠️ Minimal - command helpers tested but not full flow

**Required Tests**:

```go
// TestCommand_Create_FullFlow
// Verifies: RPC → createCommand → plugin.OnCommand → Lua handler → status update
// - Command ID generated correctly
// - Status properly tracked
// - Entity updated with LastCommandID
// - Status published to NATS

// TestCommand_Create_EntityNotFound
// Verifies: Proper error when entity doesn't exist
// - Command fails fast with clear error

// TestCommand_Create_PluginError
// Verifies: Command failure handling when plugin.OnCommand errors
// - Status updated to Failed
// - Error message captured
// - Entity sync status set to Failed

// TestCommand_Status_Tracking
// Verifies: Status transitions through lifecycle
// - Pending → Succeeded (on success)
// - Pending → Failed (on error)

// TestCommand_Status_Lookup
// Verifies: Command status can be retrieved by ID
// - getCommandStatus returns correct status

// TestCommand_Concurrent_SameEntity
// Verifies: Concurrent commands to same entity are safe
// - No race conditions
// - Statuses tracked independently

// TestCommand_LuaDispatch
// Verifies: Commands dispatched to Lua handlers correctly
// - Handler selector matching
// - Payload passed correctly to Lua
```

**Test Location**: `runner_command_test.go`
**Estimated Lines**: 350-450

---

### 4. Event Processing Integration Tests

**Why Critical**: Events drive state updates. Failures break synchronization.

**Existing Coverage**: ⚠️ Partial - Lua dispatch tested but not full flow

**Required Tests**:

```go
// TestEvent_Ingest_FullFlow
// Verifies: RPC → processInboundEvent → plugin.OnEvent → state update → publish
// - Entity state updated correctly
// - SyncStatus transitions properly
// - Event published to NATS

// TestEvent_Ingest_MissingType
// Verifies: Rejection of events without type field
// - Proper error returned

// TestEvent_Ingest_EntityNotFound
// Verifies: Handling when entity doesn't exist
// - Error response

// TestEvent_Ingest_PluginError
// Verifies: Event processing when plugin.OnEvent errors
// - Raw payload applied anyway (as per current implementation)
// - Warning logged

// TestEvent_Correlation_MatchesCommand
// Verifies: Event with correlation ID updates command status
// - Command status changed to Succeeded
// - Command.LastUpdatedAt updated

// TestEvent_LuaDispatch
// Verifies: Events dispatched to all matching Lua handlers
// - Handler selector matching
// - Multiple handlers can receive same event

// TestEvent_ExternalEntityEvent
// Verifies: Handling events from other plugins
// - Events from other plugin IDs processed
// - Own events ignored (to prevent loops)

// TestEvent_ConcurrentProcessing
// Verifies: Concurrent event processing is safe
// - No race conditions on entity updates
```

**Test Location**: `runner_event_test.go`
**Estimated Lines**: 300-400

---

### 5. NATS Messaging Integration Tests

**Why Critical**: NATS is the transport layer. Failures isolate the plugin.

**Existing Coverage**: ❌ None - all NATS code untested

**Required Tests**:

```go
// TestNATS_ConnectionRetry
// Verifies: Connection retries on failure
// - Exponential backoff or fixed retry
// - Eventually connects

// TestNATS_Reconnection_RegistrationReplay
// Verifies: Registration resent after reconnect
// - Registry state republished

// TestNATS_RPC_RequestResponse
// Verifies: Full RPC request/response cycle
// - Request properly routed
// - Response returned to caller

// TestNATS_RPC_MethodNotFound
// Verifies: Proper error for unknown methods
// - -32601 error code returned

// TestNATS_RPC_ParseError
// Verifies: Proper error for malformed requests
// - -32700 error code returned

// TestNATS_Registration_Heartbeat
// Verifies: Registration heartbeat sent periodically
// - 1-second interval

// TestNATS_SearchPlugin_Broadcast
// Verifies: Plugin search responds correctly
// - Local plugin info returned

// TestNATS_SearchDevice_Query
// Verifies: Device search with various query patterns
// - Pattern matching
// - Label filtering
// - Limit honored

// TestNATS_SearchEntity_Query
// Verifies: Entity search functionality
// - Domain filtering
// - DeviceID filtering
```

**Test Location**: `runner_nats_test.go`
**Estimated Lines**: 400-500
**Note**: Requires NATS test server (nats-server or embedded)

---

### 6. State Persistence & Recovery Tests

**Why Critical**: Data must survive restarts. Failures cause data loss.

**Existing Coverage**: ✅ Good - writeIfChanged, concurrent save, snapshot tests exist

**Gap Tests**:

```go
// TestPersistence_Restart_DataSurvives
// Verifies: Data persisted and reloaded after restart
// - Devices survive
// - Entities survive
// - Plugin state survives

// TestPersistence_CorruptedFile_Recovery
// Verifies: Graceful handling of corrupted state files
// - Corrupted files skipped
// - Other valid files loaded

// TestPersistence_ConcurrentReadWrite
// Verifies: Concurrent operations don't corrupt files
// - Read while writing
// - Multiple concurrent writes

// TestPersistence_SnapshotInterval
// Verifies: Snapshots taken at configured interval
// - Default 30s interval
// - Custom interval respected

// TestPersistence_HydrateFromSnapshot
// Verifies: Startup hydration from snapshot
// - Canonical state populated from snapshot
// - No data loss on restart

// TestPersistence_RawData_Separate
// Verifies: Raw data isolated from canonical state
// - Raw device data in separate location
// - Raw entity data in separate location
```

**Test Location**: `runner_persistence_test.go` (extend existing)
**Estimated Lines**: 200-300

---

### 7. Script Engine Integration Tests

**Why Critical**: Scripts extend functionality. Failures break automation.

**Existing Coverage**: ✅ Good - selectors, query parsing, dispatch tested

**Gap Tests**:

```go
// TestScript_LoadAndInit
// Verifies: Script loaded and OnInit called
// - Lua file loaded
// - OnInit invoked with Ctx

// TestScript_Reload_OnChange
// Verifies: Script reloaded when file changes
// - Mtime detection
// - Runtime invalidated and recreated

// TestScript_StatePersistence
// Verifies: Script state saved and restored
// - State written to .state.lua.json
// - State loaded on runtime creation

// TestScript_CommandHandler_Matching
// Verifies: Command handlers registered and invoked
// - OnCommand registers handler
// - Handler called for matching command

// TestScript_EventHandler_Matching
// Verifies: Event handlers registered and invoked
// - OnEvent registers handler
// - Handler called for matching event

// TestScript_CtxMethods
// Verifies: All Ctx methods work from Lua
// - GetState/SetState/DeleteState
// - FindEntities/FindDevices
// - SendCommand/SendBatchCommands
// - EmitEvent
// - Log methods

// TestScript_SendCommand_RPC
// Verifies: SendCommand from Lua calls actual RPC
// - Integration with callCreateCommand

// TestScript_CrossPlugin_Command
// Verifies: Commands to other plugins work
// - Cross-plugin RPC calls
```

**Test Location**: `script_engine_integration_test.go`
**Estimated Lines**: 400-500

---

### 8. Snapshot Integration Tests

**Why Critical**: Snapshots enable state recovery. Failures break restore.

**Existing Coverage**: ✅ Good - save, list, delete, restore tested

**Gap Tests**:

```go
// TestSnapshot_Restore_CommandDispatch
// Verifies: RestoreSnapshot dispatches commands correctly
// - StateToCommands conversion
// - Commands created for each payload

// TestSnapshot_SurvivesRestart
// Verifies: Snapshots persisted and survive restart
// - Snapshot in entity.Snapshots map
// - Map persisted to file

// TestSnapshot_Limits
// Verifies: Snapshot limits enforced (if applicable)
// - Max snapshots per entity

// TestSnapshot_Labels
// Verifies: Labels properly stored and retrieved
// - Custom labels on snapshots
```

**Test Location**: `snapshot_ops_test.go` (extend existing)
**Estimated Lines**: 100-150

---

### 9. Reconciliation Edge Case Tests

**Why Critical**: Reconciliation prevents data loss. Failures cause conflicts.

**Existing Coverage**: ✅ Good - basic reconcile tested

**Gap Tests**:

```go
// TestReconcile_ConflictingLabels
// Verifies: Label merge handles conflicts
// - User labels preserved
// - New discovered labels added

// TestReconcile_Device_RemovedFromDiscovery
// Verifies: Devices not in discovery are preserved
// - Not deleted when missing from discovery

// TestReconcile_Entity_DeviceIDMismatch
// Verifies: Entity deviceID reconciliation
// - DeviceID set correctly from context

// TestReconcile_EmptyFields
// Verifies: Empty field handling
// - Empty LocalName not overwriting existing
// - Empty Labels not clearing existing
```

**Test Location**: `reconcile_test.go` (extend existing)
**Estimated Lines**: 100-150

---

### 10. Error Handling & Resilience Tests

**Why Critical**: System must degrade gracefully. Failures cascade.

**Existing Coverage**: ❌ None - no error injection tests

**Required Tests**:

```go
// TestResilience_StorageFull
// Verifies: Behavior when disk is full
// - Errors returned, not panics

// TestResilience_NATSUnavailable
// Verifies: Behavior when NATS down
// - Local operations continue (in standalone mode)

// TestResilience_PluginPanic
// Verifies: Recovery from plugin method panics
// - Panic caught and logged
// - Runner continues operating

// TestResilience_LuaScriptError
// Verifies: Handling of Lua syntax/runtime errors
// - Script load errors don't crash runner
// - Runtime errors logged, not propagated

// TestResilience_InvalidJSON
// Verifies: Handling of malformed JSON in various places
// - RPC params
// - Event payloads
// - State files

// TestResilience_ConcurrentMapAccess
// Verifies: No race conditions in concurrent access
// - All maps protected by mutexes
// - go test -race passes
```

**Test Location**: `runner_resilience_test.go`
**Estimated Lines**: 300-400

---

### 11. Distributed Operations Tests

**Why Critical**: Multi-plugin operations must coordinate correctly.

**Existing Coverage**: ⚠️ Partial - findEntities tested but not cross-plugin RPC

**Required Tests**:

```go
// TestDistributed_SearchDevices_CrossPlugin
// Verifies: Device search aggregates from multiple plugins
// - Broadcast to all registered plugins
// - Results aggregated correctly

// TestDistributed_SearchEntities_CrossPlugin
// Verifies: Entity search works across plugins
// - Multi-plugin coordination

// TestDistributed_CallRPC_Timeout
// Verifies: RPC calls timeout correctly
// - Slow plugins don't block forever

// TestDistributed_CallRPC_PluginNotFound
// Verifies: Error when calling unregistered plugin
// - Proper error returned

// TestDistributed_ListDevices_CrossPlugin
// Verifies: List devices from other plugin
// - callListDevices works

// TestDistributed_CreateCommand_CrossPlugin
// Verifies: Create command on other plugin
// - callCreateCommand works
```

**Test Location**: `runner_distributed_test.go`
**Estimated Lines**: 250-350

---

### 12. Bootstrap & Discovery Tests

**Why Critical**: Startup must hydrate topology correctly. Failures cause missing devices.

**Existing Coverage**: ⚠️ Partial - bootstrap topology tested but edge cases missing

**Gap Tests**:

```go
// TestBootstrap_EmptyState
// Verifies: Clean startup with no prior state
// - Initializes correctly
// - Discovers devices

// TestBootstrap_WithExistingState
// Verifies: Startup with persisted state
// - State loaded before discovery
// - Discovery reconciles with existing

// TestBootstrap_DiscoveryFailure
// Verifies: Handling when discovery fails
// - Error propagated
// - Runner doesn't start

// TestBootstrap_EntityHydrate_Failure
// Verifies: Handling when entity hydration fails
// - Device created but entities fail
```

**Test Location**: `runner_bootstrap_test.go`
**Estimated Lines**: 150-200

---

### 13. Configuration & Environment Tests

**Why Critical**: Configuration affects all operations. Failures prevent startup.

**Existing Coverage**: ⚠️ Partial - logger tests exist, env tests missing

**Required Tests**:

```go
// TestConfig_RequiredEnv_Missing
// Verifies: Error when required env vars missing
// - PLUGIN_DATA_DIR required
// - NATS_URL required for normal mode

// TestConfig_OptionalEnv_Defaults
// Verifies: Default values for optional config
// - Log level defaults
// - Snapshot interval defaults

// TestConfig_LogLevelEnv_VariousFormats
// Verifies: Log level parsing from env
// - trace, debug, info, warn, error

// TestConfig_DataDir_Creation
// Verifies: Data directory created if missing
// - MkdirAll called
```

**Test Location**: `runner_config_test.go`
**Estimated Lines**: 100-150

---

### 14. Performance & Load Tests

**Why Critical**: System must handle load. Failures cause timeouts.

**Existing Coverage**: ❌ None

**Required Tests**:

```go
// TestPerformance_CommandThroughput
// Verifies: System handles command load
// - Benchmark: 1000 commands/second

// TestPerformance_EventThroughput
// Verifies: System handles event load
// - Benchmark: 1000 events/second

// TestPerformance_MemoryGrowth
// Verifies: No memory leaks over time
// - Run for extended period
// - Memory usage stable

// TestPerformance_Search_LargeDataset
// Verifies: Search performance with many devices/entities
// - 10,000 devices
// - Response time < 100ms
```

**Test Location**: `runner_bench_test.go`
**Estimated Lines**: 150-200

---

## Test Summary

| Category | Existing | Needed | Priority | Est. Lines |
|----------|----------|--------|----------|------------|
| Plugin Lifecycle | ❌ | 3 tests | High | 350 |
| CRUD End-to-End | ⚠️ | 8 tests | High | 450 |
| Command Processing | ⚠️ | 7 tests | High | 400 |
| Event Processing | ⚠️ | 7 tests | High | 350 |
| NATS Messaging | ❌ | 8 tests | Critical | 450 |
| Persistence | ✅ | 6 tests | High | 250 |
| Script Engine | ✅ | 8 tests | Medium | 450 |
| Snapshots | ✅ | 4 tests | Low | 150 |
| Reconciliation | ✅ | 4 tests | Low | 150 |
| Error/Resilience | ❌ | 6 tests | High | 350 |
| Distributed | ⚠️ | 6 tests | Medium | 300 |
| Bootstrap | ⚠️ | 4 tests | Medium | 175 |
| Configuration | ⚠️ | 4 tests | Low | 125 |
| Performance | ❌ | 4 tests | Low | 175 |
| **Total** | ~1,250 | **87 tests** | | **~4,200** |

---

## Testing Infrastructure Requirements

### Required Test Utilities

```go
// mockPlugin.go
// Full mock implementation of Plugin interface

type mockPlugin struct {
    // Configurable responses for each method
    initializeFunc func(Config, types.Storage) (types.Manifest, types.Storage)
    onDeviceCreateFunc func(types.Device) (types.Device, error)
    // ... etc
}

// natsTestServer.go
// Embedded NATS server for integration tests

type natsTestServer struct {
    server *testNatsServer.Server
    conn   *nats.Conn
}

// testRunner.go
// Helper to create Runner with test configuration

func newTestRunner(t *testing.T, plugin Plugin) (*Runner, string) {
    // Create temp dir
    // Set env vars
    // Create runner
    // Return cleanup function
}
```

### Test Isolation Requirements

1. **Temp directories**: Each test uses unique temp dir (avoid conflicts)
2. **NATS isolation**: Each test gets fresh NATS connection/subjects
3. **Time control**: Mock time for deterministic snapshot/heartbeat tests
4. **State cleanup**: Automatic cleanup after each test

---

## Implementation Priority

### Phase 1: Critical (Weeks 1-2)
1. NATS Messaging Tests
2. Plugin Lifecycle Tests
3. Command Processing Tests

### Phase 2: High Priority (Weeks 3-4)
4. Event Processing Tests
5. Error/Resilience Tests
6. CRUD End-to-End Tests

### Phase 3: Medium Priority (Weeks 5-6)
7. Persistence Tests (extend existing)
8. Distributed Operations Tests
9. Bootstrap Tests

### Phase 4: Nice to Have (Weeks 7-8)
10. Script Engine Integration Tests
11. Performance Tests
12. Configuration Tests

---

## Success Metrics

- **Coverage**: 80%+ code coverage
- **Critical Paths**: 100% of critical paths tested
- **Race Detection**: `go test -race` passes
- **Reliability**: No flaky tests (run 100 times, 100% pass)
- **Speed**: Test suite completes in < 30 seconds

---

## Risk Areas Identified

1. **NATS Connection**: Untested reconnection logic
2. **Concurrent Access**: Many mutexes, limited concurrency tests
3. **File I/O**: Error paths in file operations mostly untested
4. **Lua Integration**: Complex Lua↔Go boundary, limited test coverage
5. **Error Recovery**: Panic recovery and graceful degradation untested

Focus testing efforts on these areas first.
