# Feature Specification: Performance Optimization

**Feature Branch**: `001-performance-optimization`  
**Created**: 2026-02-04  
**Status**: Draft  
**Input**: User description: "Optimize parsing and transformation performance with zero-allocation patterns, schema caching, and efficient string operations while maintaining backward compatibility"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reduced Memory Allocations in Hot Paths (Priority: P1)

Data pipeline engineers processing millions of events per hour experience reduced memory pressure and garbage collection pauses when using the SDK's core transformation functions (`ParseEvent`, `ToMap`, `ToJson`, `shredContexts`).

**Why this priority**: Memory allocations directly impact throughput and latency in high-volume pipelines. This is the most impactful optimization affecting all SDK users.

**Independent Test**: Run benchmark tests on existing event payloads and verify memory allocations reduced by 30-40% while producing identical outputs. Measure with `go test -bench=. -benchmem`.

**Acceptance Scenarios**:

1. **Given** a typical enriched event with 130 fields, **When** calling `ParseEvent` and `ToMap`, **Then** total allocations reduced from ~15 to <10 allocations with identical JSON output
2. **Given** an event with 5 contexts, **When** calling `shredContexts`, **Then** allocations reduced by pre-allocating maps with capacity hints, output matches exactly
3. **Given** any existing test event, **When** running optimized code, **Then** all existing unit tests pass without modification

---

### User Story 2 - Schema Parsing Performance via Caching (Priority: P2)

Pipeline engineers processing batches of similar events (common in production) benefit from cached schema parsing, reducing CPU overhead when the same schema URIs appear repeatedly across events.

**Why this priority**: Schema parsing happens for every context in every event. Caching eliminates redundant regex operations and string processing for repeated schemas.

**Independent Test**: Run benchmarks on events with repeated schema URIs (realistic production scenario) and verify 50-70% performance improvement in schema processing. Verify cache is thread-safe.

**Acceptance Scenarios**:

1. **Given** 1000 events with the same context schemas, **When** processing all events, **Then** schema parsing operations reduced from 1000 to 1 per unique schema
2. **Given** concurrent goroutines processing events, **When** accessing schema cache, **Then** no race conditions detected with `go test -race`
3. **Given** any schema URI, **When** first parsed, **Then** result cached for subsequent calls with identical output

---

### User Story 3 - Optimized String Operations (Priority: P3)

Engineers benefit from faster string manipulation operations (`insertUnderscores`, field name processing) through use of `strings.Builder` instead of repeated string concatenation.

**Why this priority**: String operations occur frequently but are less impactful than memory allocations. Still worthwhile for cumulative performance gains.

**Independent Test**: Run string operation benchmarks and verify 15-20% performance improvement with identical outputs.

**Acceptance Scenarios**:

1. **Given** a camelCase string like "ThisStringIsCamelCase", **When** calling `insertUnderscores`, **Then** returns "This_String_Is_Camel_Case" with fewer allocations
2. **Given** any existing string test case, **When** running optimized code, **Then** output matches exactly with existing tests
3. **Given** empty or single-character strings, **When** processing, **Then** edge cases handled correctly

---

### Edge Cases

- What happens when schema cache grows very large (10,000+ unique schemas)? Memory usage should remain bounded and reasonable.
- How does system handle concurrent access to schema cache? Must be thread-safe with no race conditions.
- What happens with malformed schema URIs? Existing error handling must be preserved.
- How are empty strings, nil values, and boundary conditions handled? All existing test cases must continue passing.
- What happens with events containing invalid UTF-8 or special characters? Existing behavior preserved.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST maintain 100% backward compatibility - all existing tests pass without modification
- **FR-002**: System MUST produce identical outputs for identical inputs across all public APIs
- **FR-003**: System MUST reduce memory allocations by 30-40% in core transformation paths
- **FR-004**: System MUST pre-allocate maps and slices with capacity hints when size is predictable
- **FR-005**: System MUST use `strings.Builder` for all string concatenation operations
- **FR-006**: System MUST cache schema parsing results in a thread-safe cache
- **FR-007**: System MUST use `sync.RWMutex` for schema cache access (read-heavy workload)
- **FR-008**: System MUST preserve all existing error messages and error handling behavior
- **FR-009**: System MUST maintain or improve test coverage (≥85%)
- **FR-010**: System MUST include benchmark tests tracking allocations with `b.ReportAllocs()`
- **FR-011**: System MUST document performance improvements in godoc comments
- **FR-012**: System MUST pass `go vet` and `golint` with zero warnings
- **FR-013**: System MUST not introduce race conditions (verified with `go test -race`)

### Key Entities

- **Schema Cache**: Thread-safe map storing parsed schema results, keyed by prefix+URI, protected by RWMutex for concurrent access
- **Pre-allocated Buffers**: Maps and slices created with capacity hints based on typical event sizes (e.g., 70-item map for event fields, 8-item map for contexts)
- **String Builder**: Reusable `strings.Builder` instances for efficient string operations with pre-grown capacity

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Memory allocations in `ParseEvent` + `ToMap` reduced from ~15 to <10 allocations per event (measured with benchmarks)
- **SC-002**: Schema parsing for repeated URIs shows 50-70% performance improvement (operations per second measured with benchmarks)
- **SC-003**: String operations with `strings.Builder` show 15-20% performance improvement over current implementation
- **SC-004**: Overall event processing throughput improves by 20-35% for typical production workloads (batch of similar events)
- **SC-005**: All 100% of existing unit tests pass without modification (backward compatibility proven)
- **SC-006**: Test coverage remains ≥85% for all modified packages
- **SC-007**: Zero race conditions detected when running `go test -race ./...`
- **SC-008**: Benchmark regression tests show no performance degradation in any code path

## Assumptions *(optional)*

- Typical production workloads process batches of similar events (same schemas repeated)
- Events average 130 fields with 3-5 contexts
- Concurrent goroutines may process events in parallel
- Memory efficiency is more important than peak throughput (garbage collection pressure is the main concern)
- Schema cache memory usage is acceptable (<10MB for typical production loads with <1000 unique schemas)

## Dependencies & Constraints *(optional)*

### Dependencies

- Existing `github.com/json-iterator/go` dependency (no changes needed)
- Existing `github.com/stretchr/testify` dependency (no changes needed)
- Standard library `strings.Builder`, `sync.RWMutex` (already available)

### Constraints

- **Backward Compatibility**: No breaking changes to public APIs - this is a patch release (v0.4.1)
- **Output Preservation**: All outputs must match byte-for-byte with current implementation
- **Test Stability**: All existing tests must pass without modification
- **Performance**: No performance regressions in any code path (baseline must be established first)
- **Thread Safety**: All optimizations must be safe for concurrent use

## Out of Scope *(optional)*

- Changing public API signatures or return types
- Adding new public functions or methods
- Modifying parsing logic or validation rules
- Changing error messages or error types
- Adding new dependencies
- Algorithmic changes (e.g., different parsing approach)
- Breaking changes requiring major version bump
