# Implementation Plan: Bounded Schema Cache with LRU Eviction

**Branch**: `002-bounded-schema-cache` | **Date**: 2026-02-04 | **Spec**: [spec.md](spec.md)  
**Input**: Feature specification from `/specs/002-bounded-schema-cache/spec.md`

## Summary

Replace unbounded schema cache (v0.4.1) with LRU-bounded cache to prevent memory leaks in long-running services. Default 1000-entry limit (~200KB memory) with configurable size. Maintains v0.4.1 performance (<5% overhead) while guaranteeing bounded memory growth per Constitution Principle VII.

**Technical Approach**: 
- Dual data structure: map[string]*list.Element for O(1) lookup + doubly-linked list for O(1) LRU tracking
- Go stdlib `container/list` for proven LRU implementation
- RWMutex for thread-safe concurrent access (read-optimized)
- Atomic counters for metrics (hits, misses, evictions)
- Configuration API for runtime tuning
- Zero breaking changes - drop-in replacement for v0.4.1

## Technical Context

**Language/Version**: Go 1.25

**Primary Dependencies**: 
- `container/list` (stdlib - doubly-linked list for LRU tracking)
- `sync` (stdlib - RWMutex for thread-safety, atomic for counters)
- `github.com/json-iterator/go` v1.1.12 (existing)
- `github.com/stretchr/testify` v1.11.1 (existing)

**Storage**: In-memory cache only (no persistence)

**Testing**: 
- `go test` with `-race` flag for concurrency verification
- `testify/assert` and `testify/require` for assertions
- Benchmark tests with `b.ReportAllocs()` for performance regression detection
- Deterministic eviction testing with known schema sequences

**Target Platform**: Cross-platform (Linux, macOS, Windows)

**Project Type**: Go library (single package `analytics/`)

**Performance Goals**: 
- Cache hit: <50ns (maintain v0.4.1 performance)
- Cache miss: <1µs (LRU overhead acceptable)
- Eviction: <100ns (constant time with doubly-linked list)
- Overall overhead: <5% vs v0.4.1 unbounded cache
- Memory: maxSize * 250 bytes maximum (default: 1000 * 250 = 250KB)

**Constraints**: 
- 100% backward compatibility - patch release (v0.4.2)
- Zero breaking API changes
- Test coverage ≥92% (current level)
- All 21 existing tests pass without modification
- Zero race conditions (verified with `-race` flag)
- No performance regressions >5%

**Scale/Scope**: 
- Embedded in production pipelines processing millions of events/hour globally
- Long-running services (hours to months without restart)
- High-cardinality workloads (up to 10,000+ unique schemas)
- Multi-tenant platforms with unpredictable schema diversity

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Performance-First Design** (Principle I):
- [x] Benchmark tests planned for cache operations (hit, miss, eviction)
- [x] Performance baseline from v0.4.1: 38ns hit, ~800ns miss
- [x] No regressions: <5% overhead acceptable for memory safety

**Zero-Allocation Optimization** (Principle II):
- [x] LRU structure reuses list.Element nodes (no allocation per access)
- [x] Pre-allocated map with estimated capacity at initialization
- [x] Eviction reuses existing nodes (removes from map, updates list pointers)

**Test Coverage & Correctness** (Principle III - NON-NEGOTIABLE):
- [x] Unit tests cover: cache fill, eviction, LRU ordering, concurrency, configuration
- [x] Benchmark tests for: cache hit, cache miss, eviction overhead, full cache churn
- [x] Test coverage target: ≥92% (maintain current level)
- [x] Concurrent access test with 10,000 goroutines
- [x] Deterministic eviction order verification

**API Stability & Backward Compatibility** (Principle IV):
- [x] Semantic versioning: PATCH release v0.4.2 (bug fix for unbounded growth)
- [x] Zero breaking changes: existing fixSchema() function signature unchanged
- [x] New APIs are additions only: GetCacheStats(), SetSchemaCacheConfig(), ClearSchemaCache()
- [x] Migration: automatic - no user code changes required

**Error Transparency** (Principle V):
- [x] Configuration validation: SetSchemaCacheConfig returns error for invalid sizes
- [x] Error messages include context: "invalid cache size: %d (must be >=0)"
- [x] All error paths tested in configuration unit tests

**Documentation as Contract** (Principle VI):
- [x] Godoc comments for all exports: lruCache, CacheStats, configuration funcs
- [x] Memory characteristics documented: "Default 1000 entries (~200KB), configurable via SetSchemaCacheConfig"
- [x] Examples in godoc: configuration patterns, monitoring with GetCacheStats()
- [x] README updated: performance section with cache behavior, memory usage, tuning guidance

**Memory Management & Resource Bounds** (Principle VII - PRIMARY DRIVER):
- [x] Explicit size limit enforced: maxSize parameter prevents unbounded growth
- [x] LRU eviction policy: oldest entries removed when limit reached
- [x] Configurable bounds: users can tune maxSize based on workload
- [x] Memory footprint documented: "maxSize * ~250 bytes per entry"
- [x] Disableable: maxSize=0 disables caching entirely

**✅ Constitution Check PASSED**: All principles satisfied

## Project Structure

### Documentation (this feature)

```
specs/002-bounded-schema-cache/
├── spec.md              # Feature specification (completed)
├── plan.md              # This file (Phase 0 output)
├── research.md          # Phase 0: LRU patterns, container/list usage, benchmarking strategy
├── data-model.md        # Phase 1: lruCache, cacheEntry, CacheStats structures
├── quickstart.md        # Phase 1: Configuration examples, monitoring patterns
├── contracts/           # Phase 1: API contracts
│   └── cache-api.md     # Public API: GetCacheStats(), SetSchemaCacheConfig(), ClearSchemaCache()
├── checklists/
│   └── requirements.md  # Requirements validation (completed)
└── tasks.md             # Phase 2: Detailed task breakdown (created by /speckit.tasks)
```

### Source Code (repository root)

```
analytics/               # Main package (existing)
├── shred.go             # MODIFIED: Replace unbounded cache with lruCache
├── shred_test.go        # MODIFIED: Add LRU eviction tests, concurrent access tests
├── cache.go             # NEW: lruCache implementation (LRU data structure + methods)
├── cache_test.go        # NEW: Unit tests for LRU eviction logic
└── cache_bench_test.go  # NEW: Benchmarks for cache operations (hit/miss/eviction)

README.md                # MODIFIED: Update performance section with cache behavior
CHANGELOG                # MODIFIED: Add v0.4.2 entry
VERSION                  # MODIFIED: Update to 0.4.2
```

**Structure Decision**: 
- **analytics/cache.go**: New file for LRU cache implementation (separates concern from schema logic)
- **analytics/shred.go**: Modified to use lruCache instead of map
- Single package design maintains simplicity (no internal/ packages needed)
- Test files colocated with implementation per Go conventions

## Complexity Tracking

**No violations** - Constitution Check passed completely. This is a straightforward refactor:
- Using proven stdlib `container/list` (not reinventing LRU)
- Single package, single responsibility (caching)
- No architectural complexity added
- Backward compatible drop-in replacement

---

## Phase 0: Research & Analysis

**Goal**: Resolve technical unknowns and establish implementation approach

### Research Tasks

1. **LRU Implementation Patterns in Go**
   - Study `container/list` API and best practices
   - Review existing LRU implementations for patterns (golang-lru, groupcache)
   - Determine optimal list.Element usage (value vs pointer)
   - **Output**: Proven pattern for map + list.Element combination

2. **Thread-Safety Strategy**
   - Analyze RWMutex vs Mutex for read-heavy workload
   - Research atomic counter patterns for metrics
   - Study lock granularity (single lock vs read/write separation)
   - **Output**: Thread-safety approach with minimal lock contention

3. **Performance Benchmarking Strategy**
   - Identify v0.4.1 baseline benchmarks to extend
   - Design benchmark scenarios: hit-only, miss-only, eviction, churn
   - Research memory profiling techniques (`go test -memprofile`)
   - **Output**: Comprehensive benchmark suite design

4. **Configuration API Design**
   - Research Go stdlib configuration patterns (e.g., http.Transport)
   - Study runtime reconfiguration approaches (safe concurrent updates)
   - Evaluate configuration validation patterns
   - **Output**: API design for SetSchemaCacheConfig()

### Unknowns to Resolve

- **NEEDS CLARIFICATION**: Does list.Element.Value store pointer or value? → Research shows pointer is standard
- **NEEDS CLARIFICATION**: Best practice for map cleanup after eviction? → Delete from map, let GC handle list.Element
- **NEEDS CLARIFICATION**: How to benchmark LRU overhead accurately? → Separate hit/miss/eviction benchmarks
- **NEEDS CLARIFICATION**: Should configuration be global or per-cache-instance? → Global package-level config (matches v0.4.1 pattern)

### Research Output: `research.md`

Document findings on:
1. Proven LRU implementation pattern using container/list
2. Thread-safety approach (RWMutex + atomic counters)
3. Benchmark suite design
4. Configuration API design
5. Performance expectations and trade-offs

---

## Phase 1: Design & Contracts

**Goal**: Define data structures, APIs, and integration approach

### Data Model: `data-model.md`

**lruCache struct**:
```go
type lruCache struct {
    maxSize   int
    cache     map[string]*list.Element  // O(1) lookup
    lruList   *list.List                 // LRU tracking
    mu        sync.RWMutex               // Thread-safety
    hits      atomic.Int64               // Metrics
    misses    atomic.Int64
    evictions atomic.Int64
}
```

**cacheEntry struct** (stored in list.Element.Value):
```go
type cacheEntry struct {
    key   string
    value string
}
```

**CacheStats struct** (exported):
```go
type CacheStats struct {
    Hits      int64   // Cache hits
    Misses    int64   // Cache misses
    Evictions int64   // Evictions due to size limit
    Size      int64   // Current cache size
    HitRate   float64 // Calculated: Hits / (Hits + Misses)
}
```

### API Contracts: `contracts/cache-api.md`

**Public APIs**:

1. **GetCacheStats() CacheStats**
   - Returns snapshot of cache performance metrics
   - Thread-safe, non-blocking
   - Example:
   ```go
   stats := analytics.GetCacheStats()
   fmt.Printf("Cache: %d entries, %.1f%% hit rate, %d evictions\n",
       stats.Size, stats.HitRate*100, stats.Evictions)
   ```

2. **SetSchemaCacheConfig(maxSize int) error**
   - Configures maximum cache size
   - maxSize=0 disables caching
   - Returns error if maxSize < 0
   - Thread-safe runtime reconfiguration
   - Example:
   ```go
   // High-cardinality workload
   analytics.SetSchemaCacheConfig(5000)
   ```

3. **ClearSchemaCache()**
   - Clears all cached entries
   - Resets metrics to zero
   - Thread-safe
   - Example:
   ```go
   // Maintenance or testing
   analytics.ClearSchemaCache()
   ```

**Internal APIs** (unexported):
- `(c *lruCache) get(key string) (string, bool)` - O(1) lookup, updates LRU
- `(c *lruCache) put(key, value string)` - O(1) insert, triggers eviction if needed
- `(c *lruCache) evict()` - O(1) remove oldest entry

### Integration Points

**Modified: analytics/shred.go**:
```go
// Replace package-level variables
var schemaCache = newLRUCache(1000) // Default maxSize

func fixSchema(prefix string, schemaUri string) (string, error) {
    cacheKey := prefix + ":" + schemaUri
    
    // Try cache (now using LRU get)
    if cached, ok := schemaCache.get(cacheKey); ok {
        return cached, nil
    }
    
    // Cache miss - compute result
    parts, err := extractSchema(schemaUri)
    if err != nil {
        return "", fmt.Errorf("error parsing schema path: %w", err)
    }
    
    vendor := strings.ReplaceAll(parts.Vendor, ".", "_")
    name := insertUnderscores(parts.Name)
    result := strings.ToLower(strings.Join([]string{prefix, vendor, name, parts.Model}, "_"))
    
    // Store in cache (now using LRU put)
    schemaCache.put(cacheKey, result)
    
    return result, nil
}
```

### Quickstart: `quickstart.md`

**Basic Usage** (unchanged - backward compatible):
```go
import "github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

// Existing code works without changes
event, _ := analytics.ParseEvent(tsvEvent)
mapped, _ := event.ToMap()
```

**Monitoring Cache Performance**:
```go
import "github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

// Monitor cache effectiveness
stats := analytics.GetCacheStats()
log.Printf("Schema cache: %d entries, %.1f%% hit rate, %d evictions",
    stats.Size, stats.HitRate*100, stats.Evictions)

// Alert if hit rate drops below threshold
if stats.HitRate < 0.90 && stats.Size >= 1000 {
    log.Warn("Cache hit rate low - consider increasing maxSize")
}
```

**Configuring Cache Size**:
```go
import "github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

// Increase cache for high-cardinality workloads
if err := analytics.SetSchemaCacheConfig(5000); err != nil {
    log.Fatal(err)
}

// Disable cache for testing/debugging
analytics.SetSchemaCacheConfig(0)
```

**Maintenance Operations**:
```go
// Clear cache (e.g., after schema deployments)
analytics.ClearSchemaCache()
```

---

## Phase 2: Gates & Validation

### Constitution Re-Check (Post-Design)

**Performance-First Design** (Principle I):
- ✅ Benchmark tests designed: hit, miss, eviction, churn scenarios
- ✅ Baseline from v0.4.1: 38ns hit, needs matching performance
- ✅ Overhead budget: <5% acceptable

**Zero-Allocation Optimization** (Principle II):
- ✅ list.Element reuse via list.MoveToFront (no new allocations)
- ✅ Map pre-allocated with estimated capacity
- ✅ Eviction updates pointers only (no allocations)

**Test Coverage & Correctness** (Principle III):
- ✅ Unit tests planned: 10+ tests covering eviction, concurrency, config
- ✅ Benchmark tests: 5 benchmarks for hit/miss/eviction
- ✅ Coverage target: ≥92%

**API Stability & Backward Compatibility** (Principle IV):
- ✅ Zero breaking changes confirmed
- ✅ fixSchema() signature unchanged
- ✅ New APIs are additions only

**Error Transparency** (Principle V):
- ✅ Configuration errors detailed: "invalid cache size: %d (must be >=0)"
- ✅ All error paths tested

**Documentation as Contract** (Principle VI):
- ✅ Godoc planned for all exports
- ✅ Examples in quickstart.md
- ✅ README update planned

**Memory Management & Resource Bounds** (Principle VII):
- ✅ Explicit maxSize enforcement designed
- ✅ LRU eviction policy defined
- ✅ Memory characteristics documented

### Design Validation

**✅ All gates passed** - Ready for implementation (Phase 3: `/speckit.tasks`)

### Risk Assessment

| Risk | Mitigation | Status |
|------|------------|--------|
| LRU overhead > 5% | Benchmark early, optimize list operations | Mitigated: container/list proven efficient |
| Lock contention | RWMutex for read-optimization | Mitigated: Read-heavy workload benefits |
| Configuration race conditions | Atomic config updates | Mitigated: Design includes safe reconfiguration |
| Eviction correctness | Deterministic testing with known sequences | Mitigated: Test plan includes eviction verification |

---

## Next Steps

**Phase 2 Complete** - Ready for `/speckit.tasks` to generate implementation tasks

This plan provides:
1. ✅ Clear technical approach (LRU with container/list)
2. ✅ Constitution compliance verified
3. ✅ Data structures defined
4. ✅ API contracts specified
5. ✅ Integration points identified
6. ✅ Testing strategy established
7. ✅ All unknowns resolved

**Recommendation**: Proceed to task breakdown with confidence. Implementation is straightforward refactor with proven patterns.
