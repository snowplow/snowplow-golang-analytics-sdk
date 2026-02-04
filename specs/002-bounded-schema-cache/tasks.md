# Tasks: Bounded Schema Cache with LRU Eviction

**Input**: Design documents from `/specs/002-bounded-schema-cache/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cache-api.md, quickstart.md

**Feature**: Replace unbounded schema cache (v0.4.1) with LRU-bounded cache to prevent memory leaks in long-running services per Constitution Principle VII.

**Tests**: Test tasks included per Constitution Principle III (Test Coverage & Correctness - NON-NEGOTIABLE).

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

---

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project structure and baseline establishment

- [X] T001 Establish v0.4.1 performance baseline by running existing benchmarks in analytics/shred_test.go
- [X] T002 Document baseline metrics: cache hit latency (~38ns), memory usage (unbounded), test coverage (≥92%)
- [X] T003 [P] Create analytics/cache.go file structure with package declaration and imports (container/list, sync, sync/atomic)
- [X] T004 [P] Create analytics/cache_test.go file structure with test package and imports
- [X] T005 [P] Create analytics/cache_bench_test.go file structure for benchmark tests

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core data structures and initialization - MUST complete before user story implementation

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T006 Define lruCache struct in analytics/cache.go with maxSize, cache map, lruList, RWMutex, atomic counters
- [X] T007 Define cacheEntry struct in analytics/cache.go with key and value string fields
- [X] T008 Define CacheStats struct in analytics/cache.go with Size, Hits, Misses, Evictions, HitRate fields
- [X] T009 [P] Implement newLRUCache(maxSize int) constructor in analytics/cache.go
- [X] T010 [P] Add godoc comments for all exported types (CacheStats) and internal types (lruCache, cacheEntry)
- [X] T011 Create package-level cache singleton: var schemaCache = newLRUCache(1000) in analytics/cache.go

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Bounded Memory Consumption (Priority: P1) 🎯 MVP

**Goal**: Implement LRU cache with configurable size limit that prevents unbounded memory growth

**Independent Test**: Configure cache with maxSize=1000, process 10,000 unique schemas, verify memory ≤~200KB and oldest entries evicted

### Tests for User Story 1 (REQUIRED per Constitution Principle III) ✅

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation (RED-GREEN-REFACTOR)**

- [X] T012 [P] [US1] Unit test: TestLRUCacheBasicOperations in analytics/cache_test.go
  - Test: Create cache, add entry, retrieve entry (happy path)
  - Test: Retrieve non-existent key returns false (edge case)
  - Test: Empty cache operations (edge case)
- [X] T013 [P] [US1] Unit test: TestLRUCacheEviction in analytics/cache_test.go
  - Test: Fill cache to maxSize, add one more, verify oldest evicted
  - Test: Verify eviction counter increments correctly
  - Test: Verify cache size never exceeds maxSize
- [X] T014 [P] [US1] Unit test: TestLRUCacheLRUOrdering in analytics/cache_test.go
  - Test: Add 3 entries, access middle entry, add new entry, verify first entry evicted (not middle)
  - Test: Repeated access of same entry keeps it at front
  - Test: LRU ordering maintained after multiple operations
- [X] T015 [P] [US1] Unit test: TestLRUCacheThreadSafety in analytics/cache_test.go
  - Test: 100 goroutines simultaneously reading/writing cache with -race flag
  - Test: No data races detected
  - Test: Final cache state is consistent
- [X] T016 [P] [US1] Unit test: TestLRUCacheMemoryBounds in analytics/cache_test.go
  - Test: maxSize=1000 with 10,000 unique schemas never exceeds ~250KB
  - Test: Document memory per entry (~250 bytes)
  - Use runtime.MemStats to measure actual memory
- [X] T017 [P] [US1] Benchmark: BenchmarkLRUCacheHit in analytics/cache_bench_test.go
  - Measure cache hit latency (target: <50ns)
  - Report allocations with b.ReportAllocs() (target: 0 allocs)
- [X] T018 [P] [US1] Benchmark: BenchmarkLRUCacheMiss in analytics/cache_bench_test.go
  - Measure cache miss latency (target: <100ns)
  - Report allocations (target: 0 allocs)
- [X] T019 [P] [US1] Benchmark: BenchmarkLRUEviction in analytics/cache_bench_test.go
  - Measure eviction overhead when cache full (target: <200ns)
  - Report allocations (target: 0-1 allocs)
- [X] T020 [P] [US1] Benchmark: BenchmarkLRUCacheChurn in analytics/cache_bench_test.go
  - Measure realistic workload: 2000 schemas, 95%+ hit rate
  - Compare against v0.4.1 baseline (<5% overhead acceptable)
- [X] T021 [P] [US1] Benchmark: BenchmarkLRUConcurrent in analytics/cache_bench_test.go
  - Measure concurrent access performance with b.RunParallel()
  - Verify RWMutex scaling with multiple cores

### Implementation for User Story 1

- [X] T022 [P] [US1] Implement lruCache.get(key string) (string, bool) method in analytics/cache.go
  - Acquire write lock (mu.Lock) for thread-safety
  - Check cache map for key existence
  - If found: MoveToFront() in lruList, increment hits counter, return value
  - If not found: Increment misses counter, return false
- [X] T023 [P] [US1] Implement lruCache.put(key, value string) method in analytics/cache.go
  - Acquire write lock (mu.Lock) for thread-safety
  - Check if key already exists in cache map
  - If exists: Update value, MoveToFront() in lruList
  - If not exists and cache full (len(cache) >= maxSize): Evict LRU entry
  - Add new cacheEntry to lruList front and cache map
- [X] T024 [US1] Implement lruCache.evictLRU() method in analytics/cache.go
  - Get oldest entry: lruList.Back()
  - Remove from lruList: lruList.Remove(oldest)
  - Delete from cache map using entry.key
  - Increment evictions counter
- [X] T025 [US1] Add godoc comments to all lruCache methods explaining behavior and thread-safety guarantees
- [X] T026 [US1] Verify tests pass: go test -v -race analytics/cache_test.go
  - All unit tests GREEN
  - Zero race conditions detected
- [X] T027 [US1] Run benchmarks: go test -bench=BenchmarkLRU -benchmem analytics/cache_bench_test.go
  - Cache hit: <50ns/op ✓
  - Cache miss: <100ns/op ✓
  - Eviction: <200ns/op ✓
  - Overall overhead: <5% vs v0.4.1 baseline ✓
- [X] T028 [US1] Verify test coverage: go test -cover analytics/cache.go analytics/cache_test.go
  - Target: ≥85% coverage for cache.go
  - Document coverage percentage in commit message
- [X] T029 [US1] Integration: Replace global map cache in analytics/shred.go with schemaCache.get/put calls
  - Locate fixSchema() function in analytics/shred.go
  - Replace unbounded map lookups with schemaCache.get(cacheKey)
  - Replace map writes with schemaCache.put(cacheKey, result)
  - Maintain backward compatibility - no function signature changes
- [X] T030 [US1] Run all existing tests: go test -v -race ./analytics/...
  - All 21+ existing tests pass without modification ✓
  - Zero race conditions ✓
  - Test coverage ≥92% maintained ✓

**Checkpoint**: At this point, User Story 1 (bounded memory) is fully functional and independently testable

---

## Phase 4: User Story 2 - Cache Observability (Priority: P2)

**Goal**: Provide visibility into cache performance via GetCacheStats() API for monitoring and tuning

**Independent Test**: Call GetCacheStats(), verify accurate reporting of hits, misses, evictions, size, and hit rate

### Tests for User Story 2 (REQUIRED per Constitution Principle III) ✅

- [X] T031 [P] [US2] Unit test: TestGetCacheStatsEmpty in analytics/cache_test.go
  - Test: Fresh cache returns all zeros (hits=0, misses=0, size=0, evictions=0, hitRate=0.0)
- [X] T032 [P] [US2] Unit test: TestGetCacheStatsHits in analytics/cache_test.go
  - Test: Add entry, access it 5 times, verify hits=5, misses=0, hitRate=1.0
- [X] T033 [P] [US2] Unit test: TestGetCacheStatsMisses in analytics/cache_test.go
  - Test: Access non-existent keys 10 times, verify misses=10, hits=0, hitRate=0.0
- [X] T034 [P] [US2] Unit test: TestGetCacheStatsEvictions in analytics/cache_test.go
  - Test: Fill cache to limit, add 5 more entries, verify evictions=5
- [X] T035 [P] [US2] Unit test: TestGetCacheStatsHitRate in analytics/cache_test.go
  - Test: 80 hits + 20 misses = 80% hit rate (0.8)
  - Test: Hit rate calculation is accurate with floating point precision
- [X] T036 [P] [US2] Unit test: TestGetCacheStatsThreadSafe in analytics/cache_test.go
  - Test: 100 goroutines calling GetCacheStats() concurrently
  - Test: No panics, no race conditions with -race flag
- [X] T037 [P] [US2] Benchmark: BenchmarkGetCacheStats in analytics/cache_bench_test.go
  - Measure GetCacheStats() latency (target: <10ns - atomic reads only)
  - Report allocations (target: 0 allocs)

### Implementation for User Story 2

- [X] T038 [P] [US2] Implement GetCacheStats() function in analytics/cache.go
  - Read atomic counters: hits.Load(), misses.Load(), evictions.Load()
  - Acquire read lock (mu.RLock) to get cache size: len(cache)
  - Calculate hitRate: float64(hits) / float64(hits + misses) or 0.0 if no operations
  - Return CacheStats struct with all fields populated
- [X] T039 [US2] Add comprehensive godoc comment to GetCacheStats() with usage examples
  - Example: Monitoring cache performance
  - Example: Exporting metrics to Prometheus
  - Document thread-safety guarantees
  - Document snapshot consistency (eventual consistency acceptable)
- [X] T040 [US2] Verify tests pass: go test -v -race analytics/cache_test.go
  - All US2 tests GREEN ✓
  - Zero race conditions ✓
- [X] T041 [US2] Run benchmark: go test -bench=BenchmarkGetCacheStats -benchmem
  - Latency: <10ns/op ✓
  - Allocations: 0 allocs/op ✓
- [X] T042 [US2] Add monitoring example to analytics/cache.go godoc
  - Show periodic cache stats logging pattern
  - Show Prometheus metrics export pattern
  - Reference quickstart.md for detailed examples

**Checkpoint**: At this point, User Stories 1 AND 2 are both independently functional

---

## Phase 5: User Story 3 - Configurable Cache Behavior (Priority: P3)

**Goal**: Allow runtime configuration of cache size via SetSchemaCacheConfig() and manual clearing via ClearSchemaCache()

**Independent Test**: Set maxSize=500, verify eviction at 500 entries; set maxSize=0, verify caching disabled; call ClearSchemaCache(), verify all entries removed

### Tests for User Story 3 (REQUIRED per Constitution Principle III) ✅

- [X] T043 [P] [US3] Unit test: TestSetSchemaCacheConfigValid in analytics/cache_test.go
  - Test: SetSchemaCacheConfig(500) returns nil error
  - Test: SetSchemaCacheConfig(0) returns nil error (disables caching)
  - Test: SetSchemaCacheConfig(5000) returns nil error
- [X] T044 [P] [US3] Unit test: TestSetSchemaCacheConfigInvalid in analytics/cache_test.go
  - Test: SetSchemaCacheConfig(-1) returns error with message "maxSize must be non-negative, got: -1"
  - Test: SetSchemaCacheConfig(-100) returns error
  - Test: Error does not modify cache state
- [X] T045 [P] [US3] Unit test: TestSetSchemaCacheConfigShrink in analytics/cache_test.go
  - Test: Fill cache with 1000 entries, SetSchemaCacheConfig(500), verify size=500 and evictions=500
  - Test: Remaining entries are the 500 most recently used
- [X] T046 [P] [US3] Unit test: TestSetSchemaCacheConfigGrow in analytics/cache_test.go
  - Test: Cache with maxSize=100, SetSchemaCacheConfig(1000), verify cache can now hold 1000 entries
  - Test: No evictions occur during growth
- [X] T047 [P] [US3] Unit test: TestSetSchemaCacheConfigDisable in analytics/cache_test.go
  - Test: SetSchemaCacheConfig(0) disables caching
  - Test: All subsequent get() calls return false (cache miss)
  - Test: put() calls are no-ops
- [X] T048 [P] [US3] Unit test: TestSetSchemaCacheConfigConcurrent in analytics/cache_test.go
  - Test: Multiple goroutines calling SetSchemaCacheConfig() concurrently
  - Test: Last call wins, no panics, no race conditions with -race flag
- [X] T049 [P] [US3] Unit test: TestClearSchemaCacheBasic in analytics/cache_test.go
  - Test: Add 100 entries, call ClearSchemaCache(), verify size=0
  - Test: Verify all metrics reset (hits=0, misses=0, evictions=0)
  - Test: Verify maxSize preserved (not reset)
- [X] T050 [P] [US3] Unit test: TestClearSchemaCacheThreadSafe in analytics/cache_test.go
  - Test: Concurrent calls to ClearSchemaCache() with ongoing get/put operations
  - Test: No panics, no race conditions with -race flag
- [X] T051 [P] [US3] Benchmark: BenchmarkSetSchemaCacheConfig in analytics/cache_bench_test.go
  - Measure configuration change latency (target: <100ns for growth, O(n) for shrink)
- [X] T052 [P] [US3] Benchmark: BenchmarkClearSchemaCache in analytics/cache_bench_test.go
  - Measure clear operation latency (target: <50ns - O(1) operation)

### Implementation for User Story 3

- [X] T053 [P] [US3] Implement SetSchemaCacheConfig(maxSize int) error function in analytics/cache.go
  - Validate: maxSize >= 0, return error if negative with descriptive message
  - Acquire write lock (mu.Lock) for thread-safety
  - Update schemaCache.maxSize = maxSize
  - If maxSize < current size: Evict LRU entries until size <= maxSize (loop evictLRU)
  - If maxSize == 0: Clear cache (delete all entries, reset counters)
  - Return nil on success
- [X] T054 [P] [US3] Implement ClearSchemaCache() function in analytics/cache.go
  - Acquire write lock (mu.Lock) for thread-safety
  - Reset cache map: schemaCache.cache = make(map[string]*list.Element)
  - Reset lruList: schemaCache.lruList = list.New()
  - Reset atomic counters: hits.Store(0), misses.Store(0), evictions.Store(0)
  - Preserve maxSize (do not reset configuration)
- [X] T055 [US3] Add comprehensive godoc comments to SetSchemaCacheConfig() and ClearSchemaCache()
  - Document valid maxSize values (0 = disabled, >0 = limit)
  - Document error conditions and error messages
  - Document thread-safety guarantees
  - Document runtime reconfiguration behavior (eviction on shrink, growth on expand)
  - Document ClearSchemaCache() use cases (deployment, testing, maintenance)
  - Add code examples from quickstart.md
- [X] T056 [US3] Verify tests pass: go test -v -race analytics/cache_test.go
  - All US3 tests GREEN ✓
  - Zero race conditions ✓
- [X] T057 [US3] Run benchmarks: go test -bench="BenchmarkSetSchemaCache|BenchmarkClearSchemaCache" -benchmem
  - SetConfig (grow): <100ns/op ✓
  - SetConfig (shrink): O(evictions) acceptable ✓
  - Clear: <50ns/op ✓
- [X] T058 [US3] Add configuration examples to analytics/cache.go godoc
  - Show initialization pattern: SetSchemaCacheConfig(5000)
  - Show runtime adjustment pattern: monitor hit rate, adjust maxSize
  - Show disable pattern: SetSchemaCacheConfig(0) for testing
  - Show clear pattern: ClearSchemaCache() after schema deployment

**Checkpoint**: All user stories (US1, US2, US3) are now independently functional

---

## Phase 6: Integration & Validation

**Purpose**: Ensure complete system integration and backward compatibility

- [X] T059 Run full test suite: go test -v -race ./analytics/...
  - All 21+ existing tests pass ✓
  - All new LRU cache tests pass ✓
  - Zero race conditions with -race flag ✓
  - Test coverage ≥92% maintained ✓
- [X] T060 [P] Run all benchmarks: go test -bench=. -benchmem ./analytics/...
  - Compare against v0.4.1 baseline documented in T001-T002
  - Cache hit latency: <50ns (within 5% of v0.4.1's 38ns) ✓
  - Overall overhead: <5% ✓
  - Document results for performance report
- [X] T061 [P] Memory profiling: go test -bench=BenchmarkLRU -memprofile=mem.prof
  - Analyze with: go tool pprof -alloc_space mem.prof
  - Verify maxSize=1000 uses ~250KB memory ✓
  - Verify no memory leaks (flat profile, bounded allocations) ✓
- [X] T062 Integration test: Process 100,000 realistic events with real schemas
  - Use existing test events from analytics/shred_test.go
  - Monitor cache stats: verify hit rate >95% for typical workload
  - Verify memory usage remains bounded at ~250KB
  - Verify zero race conditions
- [X] T063 Stress test: 10,000 concurrent goroutines accessing cache
  - Run with -race flag
  - Verify zero race conditions ✓
  - Verify zero panics ✓
  - Verify cache consistency after test completion ✓
- [X] T064 [P] Verify go vet passes: go vet ./analytics/...
- [X] T065 [P] Verify gofmt formatting: gofmt -l ./analytics/
- [X] T066 Code review: Ensure code follows Go best practices
  - Error handling with wrapped errors (fmt.Errorf("%w", err))
  - Godoc comments for all exports
  - Thread-safety documented
  - No global mutable state (except package-level singleton)

---

## Phase 7: Documentation & Release

**Purpose**: Update documentation and prepare for v0.4.2 release

- [ ] T067 [P] Update README.md Performance section
  - Document bounded cache behavior (maxSize=1000 default, ~250KB memory)
  - Document configuration: SetSchemaCacheConfig(maxSize)
  - Document monitoring: GetCacheStats()
  - Document maintenance: ClearSchemaCache()
  - Document memory characteristics: maxSize × ~250 bytes
  - Document tuning guidelines (reference quickstart.md)
  - Add example code showing configuration patterns
- [ ] T068 [P] Update README.md API Reference section
  - Add GetCacheStats() function signature and description
  - Add SetSchemaCacheConfig(maxSize int) error signature and description
  - Add ClearSchemaCache() signature and description
  - Link to contracts/cache-api.md for detailed API contracts
- [ ] T069 [P] Update CHANGELOG for v0.4.2
  - Section: "### Fixed"
  - Entry: "Unbounded schema cache memory leak in long-running services (v0.4.1 regression)"
  - Section: "### Added"
  - Entry: "GetCacheStats() API for cache performance monitoring"
  - Entry: "SetSchemaCacheConfig(maxSize int) API for runtime cache configuration"
  - Entry: "ClearSchemaCache() API for manual cache invalidation"
  - Section: "### Changed"
  - Entry: "Schema cache now uses LRU eviction with default 1000-entry limit (~250KB memory)"
  - Section: "### Performance"
  - Entry: "Cache hit performance maintained at <50ns (within 5% of v0.4.1)"
  - Section: "### Migration"
  - Entry: "Fully backward compatible - no code changes required for existing users"
  - Entry: "Optional: Configure cache size with SetSchemaCacheConfig() if >1000 schemas needed"
- [ ] T070 [P] Update VERSION file to 0.4.2
- [ ] T071 [P] Add godoc examples to analytics/cache.go
  - Example: Basic usage (automatic caching, transparent to user)
  - Example: Monitoring with GetCacheStats()
  - Example: Configuration with SetSchemaCacheConfig()
  - Example: Clearing cache with ClearSchemaCache()
  - Example: Prometheus metrics export pattern
- [ ] T072 [P] Create release notes document: specs/002-bounded-schema-cache/RELEASE-NOTES.md
  - Summary: Fixes v0.4.1 unbounded cache memory leak
  - Background: Constitution Principle VII (Memory Management & Resource Bounds)
  - Changes: LRU cache with configurable bounds (default 1000 entries)
  - Migration: Automatic (100% backward compatible)
  - Configuration: Optional tuning via SetSchemaCacheConfig()
  - Performance: <5% overhead vs v0.4.1
  - Testing: ≥92% coverage maintained, zero race conditions
- [ ] T073 Final validation: Run complete test suite one more time
  - go test -v -race -cover ./analytics/...
  - All tests pass ✓
  - Coverage ≥92% ✓
  - Zero race conditions ✓
- [ ] T074 Tag release: git tag -a v0.4.2 -m "Bounded schema cache with LRU eviction"
- [ ] T075 Push release: git push origin v0.4.2

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup (Phase 1) - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational (Phase 2) - Core bounded memory feature
- **User Story 2 (Phase 4)**: Depends on Foundational (Phase 2) - Can start in parallel with US1 but needs US1 lruCache struct
- **User Story 3 (Phase 5)**: Depends on Foundational (Phase 2) - Can start in parallel with US1 but needs US1 lruCache methods
- **Integration (Phase 6)**: Depends on ALL user stories (US1, US2, US3) complete
- **Documentation (Phase 7)**: Depends on Integration (Phase 6) validation

### User Story Dependencies

- **User Story 1 (P1 - Bounded Memory)**: Foundational only - NO dependencies on other stories ✅
  - Implements core lruCache data structure and operations
  - Independently testable: Fill cache, verify eviction
  - **MVP STOP POINT**: Can ship with just US1 if needed
  
- **User Story 2 (P2 - Observability)**: Depends on US1 lruCache struct and atomic counters
  - Reads existing counters from US1 implementation
  - Independently testable: Call GetCacheStats(), verify metrics
  - Can be implemented after US1 or in parallel by different developer
  
- **User Story 3 (P3 - Configuration)**: Depends on US1 lruCache struct and eviction methods
  - Uses US1 evictLRU() method for shrinking
  - Independently testable: Configure size, verify behavior
  - Can be implemented after US1 or in parallel by different developer

### Within Each User Story

1. **Tests FIRST** (TDD - Red-Green-Refactor):
   - Write unit tests (expect FAIL)
   - Write benchmarks (baseline)
   
2. **Implementation**:
   - Implement core methods
   - Run tests (expect GREEN)
   
3. **Validation**:
   - Verify benchmarks meet targets
   - Verify coverage ≥85%
   
4. **Documentation**:
   - Add godoc comments
   - Update examples

### Parallel Opportunities

**Within Phase 1 (Setup)**:
- T003, T004, T005 can all run in parallel (different files)

**Within Phase 2 (Foundational)**:
- T006-T008 (struct definitions) must be sequential (same file)
- T009-T010 can run in parallel after T006-T008 (different concerns)

**Within Phase 3 (US1 Tests)**:
- T012-T021 can ALL run in parallel (independent test files/functions)

**Within Phase 3 (US1 Implementation)**:
- T022-T023 can run in parallel (different methods in cache.go)
- T024 depends on T023 (uses put() method)

**Within Phase 4 (US2)**:
- T031-T037 tests can ALL run in parallel
- T038 implementation can start after US1 T006 (needs CacheStats struct)
- T039-T042 can run in parallel

**Within Phase 5 (US3)**:
- T043-T052 tests can ALL run in parallel
- T053-T054 can run in parallel (independent functions)
- T055-T058 can run in parallel

**Within Phase 6 (Integration)**:
- T060, T061, T064, T065 can ALL run in parallel (independent validation)

**Within Phase 7 (Documentation)**:
- T067-T072 can ALL run in parallel (different files)

**Cross-Phase Parallelism**:
- Once Foundational (Phase 2) completes:
  - Developer A: US1 (T012-T030)
  - Developer B: US2 (T031-T042) - starts after US1 T006
  - Developer C: US3 (T043-T058) - starts after US1 T006
- All three can proceed mostly in parallel

---

## Parallel Example: After Foundational Phase

```bash
# Developer A: User Story 1 (Bounded Memory)
Task T012-T021: Write all US1 tests in parallel
Task T022-T024: Implement lruCache core methods
Task T025-T030: Validate and integrate

# Developer B: User Story 2 (Observability) - can start after US1 T006
Task T031-T037: Write all US2 tests in parallel
Task T038-T039: Implement GetCacheStats()
Task T040-T042: Validate

# Developer C: User Story 3 (Configuration) - can start after US1 T006
Task T043-T052: Write all US3 tests in parallel
Task T053-T054: Implement SetConfig and Clear
Task T055-T058: Validate
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. **Phase 1**: Setup (T001-T005) - Establish baseline and file structure
2. **Phase 2**: Foundational (T006-T011) - Core data structures **[GATE]**
3. **Phase 3**: User Story 1 (T012-T030) - Bounded memory with LRU eviction
4. **STOP and VALIDATE**: 
   - Run T059-T063 (integration tests)
   - Test independently: Fill cache with 10,000 schemas, verify ≤250KB memory
   - Performance: Verify <5% overhead vs v0.4.1
5. **MVP READY**: Can ship v0.4.2-beta with just bounded cache (US1)

### Incremental Delivery (Recommended)

1. **Foundation**: Setup + Foundational (T001-T011) → Foundation ready
2. **Release 1 - MVP**: Add US1 (T012-T030) → Test independently → Ship v0.4.2-beta
   - Delivers: Bounded memory, prevents OOM crashes
   - Value: Fixes critical production issue
3. **Release 2**: Add US2 (T031-T042) → Test independently → Ship v0.4.2-rc1
   - Delivers: Cache observability for monitoring
   - Value: Operators can tune and monitor performance
4. **Release 3**: Add US3 (T043-T058) → Test independently → Ship v0.4.2
   - Delivers: Runtime configuration and maintenance
   - Value: Complete feature set with flexibility
5. **Final**: Integration + Documentation (T059-T075) → Ship v0.4.2 final

### Parallel Team Strategy

With 3 developers after Foundational phase completes:

1. **All devs**: Complete Setup + Foundational together (T001-T011)
2. **After T011 checkpoint**:
   - **Dev A**: User Story 1 - Bounded Memory (T012-T030)
   - **Dev B**: User Story 2 - Observability (T031-T042)
   - **Dev C**: User Story 3 - Configuration (T043-T058)
3. **Merge point**: Integration testing (T059-T066)
4. **All devs**: Documentation in parallel (T067-T072)
5. **Lead dev**: Final validation and release (T073-T075)

**Timeline Estimate**:
- Sequential: ~12-15 hours total
- Parallel (3 devs): ~5-7 hours total

---

## Task Count Summary

- **Phase 1 (Setup)**: 5 tasks
- **Phase 2 (Foundational)**: 6 tasks [BLOCKING GATE]
- **Phase 3 (US1 - Bounded Memory)**: 19 tasks (10 tests + 9 implementation)
- **Phase 4 (US2 - Observability)**: 12 tasks (7 tests + 5 implementation)
- **Phase 5 (US3 - Configuration)**: 16 tasks (10 tests + 6 implementation)
- **Phase 6 (Integration)**: 8 tasks
- **Phase 7 (Documentation)**: 9 tasks

**Total**: 75 tasks

**Test Tasks**: 27 tests + 5 benchmarks = 32 test tasks (43% of total)
**Parallel Tasks**: 48 tasks marked [P] (64% parallelizable)
**User Story Tasks**: 47 tasks (Phase 3-5) organized by story for independent delivery

---

## Quality Gates

### After Phase 2 (Foundational)
- ✅ All data structures defined (lruCache, cacheEntry, CacheStats)
- ✅ Package singleton created and initialized
- ✅ Code compiles without errors

### After Phase 3 (US1)
- ✅ All US1 tests pass with -race flag
- ✅ Benchmarks meet targets (<50ns hit, <5% overhead)
- ✅ Memory bounded at ~250KB for 1000 entries
- ✅ Coverage ≥85% for cache.go
- **DECISION POINT**: Ship MVP or continue to US2?

### After Phase 4 (US2)
- ✅ All US2 tests pass
- ✅ GetCacheStats() returns accurate metrics
- ✅ Zero race conditions in concurrent stats calls
- **DECISION POINT**: Ship with observability or continue to US3?

### After Phase 5 (US3)
- ✅ All US3 tests pass
- ✅ Configuration API validates inputs correctly
- ✅ Runtime reconfiguration works (grow/shrink/disable)
- ✅ ClearSchemaCache() resets state correctly

### After Phase 6 (Integration)
- ✅ All 21+ existing tests pass (backward compatibility)
- ✅ Overall test coverage ≥92%
- ✅ Zero race conditions in full test suite
- ✅ Performance validation: <5% overhead vs v0.4.1
- ✅ Memory profiling shows bounded allocations

### Before Phase 7 (Release)
- ✅ All quality gates passed
- ✅ Code review complete
- ✅ Documentation complete
- ✅ CHANGELOG updated with semantic versioning

---

## Notes

- **[P] marker**: Tasks can run in parallel with others (different files or independent concerns)
- **[Story] marker**: Maps task to specific user story for traceability and independent testing
- **TDD approach**: Write tests FIRST (RED), implement (GREEN), then optimize (REFACTOR)
- **Constitution compliance**: Test coverage ≥92% maintained per Principle III (NON-NEGOTIABLE)
- **Commit strategy**: Commit after each logical group or checkpoint
- **Stop points**: Can validate and ship after any user story phase (incremental delivery)
- **Avoid**: Vague tasks, concurrent edits to same file, cross-story dependencies that break independence

---

**Tasks Status**: Ready for implementation  
**Generated**: 2026-02-04  
**Constitution**: v1.1.0 compliant  
**Feature Branch**: 002-bounded-schema-cache
