# Feature Specification: Bounded Schema Cache with LRU Eviction

**Feature Branch**: `002-bounded-schema-cache`  
**Created**: 2026-02-04  
**Status**: Draft  
**Input**: User description: "Implement bounded schema cache with LRU eviction to comply with Constitution Principle VII and prevent unbounded memory growth in long-running services"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Bounded Memory Consumption (Priority: P1)

As an **operations engineer** running the SDK in a long-running service processing millions of events, I need the schema cache to have a **predictable maximum memory footprint** so that I can capacity plan accurately and prevent OOM crashes in production.

**Why this priority**: This is the critical production stability issue. Unbounded cache growth in v0.4.1 can cause production outages. This must be fixed before any other enhancements.

**Independent Test**: Configure cache with max size of 1000 entries, process 10,000 unique schema URIs, verify memory usage never exceeds ~200KB and oldest entries are evicted.

**Acceptance Scenarios**:

1. **Given** cache configured with maxSize=1000, **When** 1000 unique schemas are cached, **Then** cache size equals 1000 entries and memory usage is ~200KB
2. **Given** cache is full (1000 entries), **When** 1001st unique schema is processed, **Then** least-recently-used entry is evicted and cache size remains 1000
3. **Given** cache contains 1000 entries, **When** an existing schema is accessed, **Then** that entry moves to front (most-recently-used) and no eviction occurs
4. **Given** long-running service processes 100,000 events over 24 hours, **When** monitoring memory usage, **Then** cache memory remains bounded at ~200KB regardless of unique schema count

---

### User Story 2 - Cache Observability (Priority: P2)

As a **platform engineer** monitoring production systems, I need visibility into cache performance metrics (hit rate, size, evictions) so that I can tune cache size and diagnose performance issues.

**Why this priority**: After establishing bounded memory (P1), operators need observability to optimize cache configuration and validate performance benefits.

**Independent Test**: Call GetCacheStats() API, verify accurate reporting of hits, misses, evictions, current size, and hit rate percentage.

**Acceptance Scenarios**:

1. **Given** cache has processed 100 lookups (80 hits, 20 misses), **When** GetCacheStats() is called, **Then** returns hits=80, misses=20, hitRate=80%, currentSize=[actual count], evictions=[actual count]
2. **Given** cache is full and eviction occurs, **When** monitoring metrics, **Then** eviction counter increments
3. **Given** application starts fresh, **When** GetCacheStats() is called immediately, **Then** returns all zeros (hits=0, misses=0, size=0, evictions=0)

---

### User Story 3 - Configurable Cache Behavior (Priority: P3)

As a **developer** integrating the SDK, I need to configure cache size or disable caching entirely so that I can optimize for my specific workload (high schema diversity vs. low diversity).

**Why this priority**: After core functionality (P1) and observability (P2), providing configuration flexibility allows users to tune for their specific scenarios.

**Independent Test**: Set maxCacheSize=500 via configuration, verify cache evicts at 500 entries. Set maxCacheSize=0, verify caching is disabled and all lookups result in cache misses.

**Acceptance Scenarios**:

1. **Given** user sets SetSchemaCacheConfig(maxSize=500), **When** 501st schema is processed, **Then** eviction occurs and size remains 500
2. **Given** user sets SetSchemaCacheConfig(maxSize=0), **When** any schema is processed, **Then** no caching occurs, all lookups miss, memory usage is constant
3. **Given** default configuration (no explicit setting), **When** cache is used, **Then** defaults to maxSize=1000

---

### Edge Cases

- **Concurrent Access**: What happens when 100 goroutines simultaneously access/update the cache? Must be thread-safe with no race conditions.
- **Cache Full + High Churn**: How does system handle scenario where all 1000 entries are unique on every request? Must maintain O(1) lookup performance.
- **Zero or Negative maxSize**: How does configuration validation work? Must reject invalid sizes or treat as "disabled".
- **Memory Pressure**: What happens if individual schema URIs are extremely long (>1KB)? Actual memory usage scales with key/value size, documentation must reflect this.
- **Repeated Access Pattern**: What happens if same 10 schemas accessed repeatedly while cache holds 1000 entries? Must maintain those 10 at front, others eligible for eviction.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Cache MUST enforce maximum size limit preventing unbounded growth (default: 1000 entries)
- **FR-002**: Cache MUST implement LRU (Least Recently Used) eviction policy when limit is reached
- **FR-003**: Cache MUST maintain thread-safety using RWMutex for concurrent read/write access
- **FR-004**: Cache MUST provide GetCacheStats() API returning: hits, misses, evictions, currentSize, hitRate
- **FR-005**: Cache MUST provide SetSchemaCacheConfig(maxSize int) API for runtime configuration
- **FR-006**: Cache MUST provide ClearSchemaCache() API for manual cache clearing (testing, maintenance)
- **FR-007**: Cache lookups MUST remain O(1) average complexity even when full
- **FR-008**: Cache MUST support disabling by setting maxSize=0 (fallback to v0.4.0 behavior)
- **FR-009**: System MUST maintain 100% backward compatibility - existing code continues to work without changes
- **FR-010**: Cache behavior MUST be documented in godoc comments with memory characteristics and configuration examples

### Key Entities

- **LRUCache**: Cache structure with map for O(1) lookup and doubly-linked list for O(1) LRU tracking
  - Attributes: maxSize (int), currentSize (int), cache (map), lruList (doubly-linked list), mutex (RWMutex)
  - Relationships: Contains cacheEntry elements in both map and list

- **CacheEntry**: Individual cached schema transformation
  - Attributes: key (string), value (string), listElement (pointer to list node)
  - Relationships: Stored in both cache map and LRU list

- **CacheStats**: Performance metrics snapshot
  - Attributes: hits (int64), misses (int64), evictions (int64), currentSize (int64), hitRate (float64)
  - Relationships: Read-only snapshot of cache performance counters

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Cache memory usage MUST NOT exceed (maxSize * 250 bytes) under any workload, ensuring predictable resource consumption
- **SC-002**: Cache hit rate MUST exceed 95% for workloads with <1000 unique schemas (typical production scenario)
- **SC-003**: Cache lookup performance MUST remain <50ns for hits and <1µs for misses (matching v0.4.1 performance)
- **SC-004**: System MUST handle 10,000 concurrent goroutines accessing cache with zero race conditions (verified with -race flag)
- **SC-005**: Configuration API MUST allow runtime changes without application restart
- **SC-006**: All 21 existing unit tests MUST pass without modification, proving 100% backward compatibility
- **SC-007**: Test coverage MUST remain ≥92% (current level)
- **SC-008**: Documentation MUST clearly state memory characteristics, configuration options, and trade-offs

### Assumptions

- Schema URI length averages 60 characters, maximum realistic length 200 characters
- Typical production workloads have <1000 unique schemas (90th percentile use case)
- High-cardinality scenarios (>10,000 schemas) are rare but must be supported without crashes
- Default maxSize=1000 provides good balance: ~200KB memory, >95% hit rate for most users
- Users requiring higher limits can configure explicitly (e.g., maxSize=5000 for ~1MB cache)

### Out of Scope

- **TTL-based expiration**: This spec focuses on size-based LRU eviction only. Time-based expiration could be added in future if needed.
- **Persistent cache**: Cache is in-memory only, cleared on application restart. Disk persistence not required for this use case.
- **Distributed cache**: Each process has independent cache. Shared cache across processes out of scope.
- **Compression**: Schema strings stored uncompressed. Compression adds complexity without significant benefit for small strings.
- **Cache warming**: Pre-populating cache with known schemas at startup. Users can implement externally if needed.

### Dependencies

- **v0.4.1 baseline**: This feature builds on the schema cache introduced in v0.4.1
- **Constitution v1.1.0**: Compliance with new Principle VII (Memory Management & Resource Bounds)
- **Go standard library**: `container/list` for doubly-linked list, `sync` for RWMutex
- **Backward compatibility**: Must work as drop-in replacement for v0.4.1 schema cache

### Non-Functional Requirements

- **Performance**: Cache operations MUST NOT introduce >5% overhead vs. v0.4.1 unbounded cache
- **Memory**: Default configuration MUST use <250KB for cache structure + entries
- **Thread-Safety**: All cache operations MUST be safe for concurrent access from unlimited goroutines
- **Testability**: Cache behavior MUST be fully testable with deterministic eviction order
- **Maintainability**: Implementation MUST use standard Go patterns (container/list) for long-term maintainability

### Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| LRU overhead degrades performance vs unbounded cache | Medium | Benchmark thoroughly; accept <5% overhead for memory safety |
| Users with >1000 schemas see cache thrashing | Low | Document configuration; provide tuning guidance; make maxSize configurable |
| Default maxSize too low for some users | Low | Conservative default (1000); clear docs on tuning; easy configuration API |
| Lock contention under extreme concurrency | Low | Use RWMutex (read-optimized); benchmark with -race flag; typical workloads unaffected |

### Version & Release

- **Target Version**: v0.4.2 (PATCH release - bug fix for unbounded growth issue)
- **Release Type**: Patch (fixes production stability issue without breaking changes)
- **Backward Compatibility**: 100% - existing code works without modification
- **Migration Path**: Automatic - users upgrade with `go get -u`, no code changes required
- **Rollback Strategy**: Users can downgrade to v0.4.1 if issues arise (accepts unbounded risk)

### Constitution Compliance

This specification aligns with all seven Constitution principles:

- ✅ **Principle I (Performance-First)**: Maintains v0.4.1 performance with <5% overhead
- ✅ **Principle II (Zero-Allocation)**: LRU structure reuses list nodes, minimizes allocations
- ✅ **Principle III (Test Coverage)**: Maintains ≥92% coverage with new LRU tests
- ✅ **Principle IV (API Stability)**: Patch release, 100% backward compatible
- ✅ **Principle V (Error Transparency)**: Configuration errors clearly documented
- ✅ **Principle VI (Documentation)**: Godoc comments document memory behavior, configuration
- ✅ **Principle VII (Memory Management)**: PRIMARY DRIVER - implements bounded cache per principle requirements

### Related Documents

- Constitution v1.1.0: [.specify/memory/constitution.md](../../.specify/memory/constitution.md) - Principle VII
- v0.4.1 Performance Report: [specs/001-performance-optimization/performance-report.md](../001-performance-optimization/performance-report.md)
- v0.4.1 Implementation: [specs/001-performance-optimization/IMPLEMENTATION-SUMMARY.md](../001-performance-optimization/IMPLEMENTATION-SUMMARY.md)

### User Story 1 - [Brief Title] (Priority: P1)

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently - e.g., "Can be fully tested by [specific action] and delivers [specific value]"]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]
2. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

### User Story 2 - [Brief Title] (Priority: P2)

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

### User Story 3 - [Brief Title] (Priority: P3)

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

[Add more user stories as needed, each with an assigned priority]

### Edge Cases

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right edge cases.
-->

- What happens when [boundary condition]?
- How does system handle [error scenario]?

## Requirements *(mandatory)*

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right functional requirements.
-->

### Functional Requirements

- **FR-001**: System MUST [specific capability, e.g., "allow users to create accounts"]
- **FR-002**: System MUST [specific capability, e.g., "validate email addresses"]  
- **FR-003**: Users MUST be able to [key interaction, e.g., "reset their password"]
- **FR-004**: System MUST [data requirement, e.g., "persist user preferences"]
- **FR-005**: System MUST [behavior, e.g., "log all security events"]

*Example of marking unclear requirements:*

- **FR-006**: System MUST authenticate users via [NEEDS CLARIFICATION: auth method not specified - email/password, SSO, OAuth?]
- **FR-007**: System MUST retain user data for [NEEDS CLARIFICATION: retention period not specified]

### Key Entities *(include if feature involves data)*

- **[Entity 1]**: [What it represents, key attributes without implementation]
- **[Entity 2]**: [What it represents, relationships to other entities]

## Success Criteria *(mandatory)*

<!--
  ACTION REQUIRED: Define measurable success criteria.
  These must be technology-agnostic and measurable.
-->

### Measurable Outcomes

- **SC-001**: [Measurable metric, e.g., "Users can complete account creation in under 2 minutes"]
- **SC-002**: [Measurable metric, e.g., "System handles 1000 concurrent users without degradation"]
- **SC-003**: [User satisfaction metric, e.g., "90% of users successfully complete primary task on first attempt"]
- **SC-004**: [Business metric, e.g., "Reduce support tickets related to [X] by 50%"]
