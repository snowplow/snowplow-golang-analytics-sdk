# Tasks: Performance Optimization

**Branch**: `001-performance-optimization`  
**Input**: Design documents from `/specs/001-performance-optimization/`
**Prerequisites**: [plan.md](plan.md) (required), [spec.md](spec.md) (required for user stories)

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 0: Baseline & Preparation (CRITICAL FIRST STEP)

**Purpose**: Establish performance baseline before making any changes

**⚠️ REQUIRED**: These tasks MUST be completed first to validate optimizations

- [X] T001 Run all existing tests and verify 100% pass: `go test ./analytics -v`
- [X] T002 Run race detector and verify zero warnings: `go test -race ./analytics`
- [X] T003 [P] Run existing benchmarks and document baseline results: `go test -bench=. -benchmem ./analytics > baseline-benchmarks.txt`
- [X] T004 [P] Verify current test coverage: `go test -cover ./analytics` (document current %)
- [X] T005 Create git tag for baseline: `git tag -a baseline-v0.4.0 -m "Performance baseline before optimizations"`
- [X] T006 Review analytics/shred.go - identify allocation hotspots in shredContexts, fixSchema, insertUnderscores
- [X] T007 Review analytics/transform.go - identify allocation hotspots in mapifyGoodEvent, GetSubsetMap

**Checkpoint**: ✅ Baseline documented, all tests passing, optimization targets identified

---

## Phase 1: User Story 1 - Memory Allocation Optimization (Priority: P1) 🎯 MVP

**Goal**: Reduce memory allocations by 30-40% in core transformation functions

**Independent Test**: Run benchmarks with `-benchmem` flag and verify allocations reduced from ~15 to <10 per event while all existing unit tests pass unchanged

### Benchmarks for User Story 1 (REQUIRED - Create FIRST) ✅

> **RED-GREEN-REFACTOR**: Write benchmarks → See baseline numbers → Optimize → See improvements

- [X] T010 [P] [US1] Add benchmark for shredContexts with allocation tracking in analytics/shred_test.go
  ```go
  func BenchmarkShredContexts(b *testing.B) {
      b.ReportAllocs()
      for i := 0; i < b.N; i++ {
          shredContexts(ctxt)
      }
  }
  ```

- [X] T011 [P] [US1] Add benchmark for mapifyGoodEvent in analytics/transform_test.go
  ```go
  func BenchmarkMapifyGoodEvent(b *testing.B) {
      b.ReportAllocs()
      event := ParseEvent(testEvent)
      b.ResetTimer()
      for i := 0; i < b.N; i++ {
          event.mapifyGoodEvent(enrichedEventFieldTypes, false)
      }
  }
  ```

- [X] T012 [P] [US1] Add benchmark for GetSubsetMap in analytics/transform_test.go
  ```go
  func BenchmarkGetSubsetMap(b *testing.B) {
      b.ReportAllocs()
      event := ParseEvent(testEvent)
      fields := []string{"platform", "event", "contexts", "unstruct_event"}
      b.ResetTimer()
      for i := 0; i < b.N; i++ {
          event.GetSubsetMap(fields...)
      }
  }
  ```

- [X] T013 [US1] Run new benchmarks and document BEFORE optimization numbers

### Implementation for User Story 1

- [X] T014 [US1] Optimize shredContexts in analytics/shred.go
  - Pre-allocate distinctContexts map: `make(map[string][]any, 8)`
  - Pre-allocate context value arrays: `make([]any, 0, 4)`
  - Pre-allocate output slice: `make([]KeyVal, 0, len(distinctContexts))`
  - **VERIFY**: Run TestShredContexts - must pass unchanged

- [X] T015 [US1] Optimize mapifyGoodEvent in analytics/transform.go
  - Pre-allocate output map: `make(map[string]any, 70)` (estimate 70 non-empty fields)
  - Skip empty fields early with `if value == "" { continue }`
  - **VERIFY**: Run all transform tests - must pass unchanged

- [X] T016 [US1] Optimize GetSubsetMap in analytics/transform.go
  - Pre-allocate output map: `make(map[string]any, len(fields))`
  - **VERIFY**: Run TestGetSubsetMap - must pass unchanged

- [X] T017 [US1] Run benchmarks and verify allocation improvements
  - Compare against T013 baseline
  - Target: <10 allocations per operation
  - Document actual improvement percentage

- [ ] T018 [US1] Run full test suite: `go test ./analytics -v`
  - ALL tests must pass
  - NO test code modifications allowed

- [ ] T019 [US1] Verify test coverage maintained: `go test -cover ./analytics`
  - Must be ≥85%

- [ ] T020 [US1] Run race detector: `go test -race ./analytics`
  - Must show zero warnings

**Checkpoint**: ✅ Allocations reduced 30-40%, all existing tests pass, coverage ≥85%, no races

---

## Phase 2: User Story 2 - Schema Cache Implementation (Priority: P2)

**Goal**: 50-70% performance improvement for repeated schema URIs through caching

**Independent Test**: Benchmark schema parsing with repeated URIs (realistic production scenario), verify cache is thread-safe with race detector

### Benchmarks for User Story 2 (REQUIRED - Create FIRST) ✅

- [X] T030 [P] [US2] Add benchmark for fixSchema with repeated URIs in analytics/shred_test.go
  ```go
  func BenchmarkFixSchemaRepeated(b *testing.B) {
      b.ReportAllocs()
      uri := "iglu:com.acme.data/some_event/jsonschema/15-34-1"
      for i := 0; i < b.N; i++ {
          fixSchema("unstruct", uri)
      }
  }
  ```

- [X] T031 [P] [US2] Add benchmark for fixSchema with unique URIs in analytics/shred_test.go
  ```go
  func BenchmarkFixSchemaUnique(b *testing.B) {
      b.ReportAllocs()
      for i := 0; i < b.N; i++ {
          uri := fmt.Sprintf("iglu:com.test/event_%d/jsonschema/1-0-0", i)
          fixSchema("unstruct", uri)
      }
  }
  ```

- [X] T032 [P] [US2] Add concurrent access test for schema cache in analytics/shred_test.go
  ```go
  func TestSchemaCacheConcurrent(t *testing.T) {
      // Test parallel goroutines accessing cache
      // Verify no race conditions with -race flag
  }
  ```

- [X] T033 [US2] Run benchmarks and document BEFORE caching numbers

### Implementation for User Story 2

- [X] T034 [US2] Add schema cache infrastructure at package level in analytics/shred.go
  ```go
  import "sync"
  
  var (
      schemaCache = make(map[string]string)
      schemaMu    sync.RWMutex
  )
  ```
  - Thread-safe with RWMutex for read-heavy workload

- [X] T035 [US2] Modify fixSchema function to use cache in analytics/shred.go
  - Check cache with RLock first
  - On cache miss: compute result, store with Lock
  - Cache key: `prefix + ":" + schemaUri`
  - **VERIFY**: Run TestFixSchema - must pass unchanged

- [X] T036 [US2] Move schema pattern regex to package level in analytics/shred.go
  - Compile once: `var schema_pattern = regexp.MustCompile(SCHEMA_URI_REGEX)`
  - Remove repeated compilation in extractSchema
  - **VERIFY**: Run TestExtractSchema - must pass unchanged

- [X] T037 [US2] Run benchmarks and verify cache performance
  - BenchmarkFixSchemaRepeated should show 50-70% improvement
  - Compare against T033 baseline
  - Document cache hit/miss performance

- [X] T038 [US2] Run concurrent test with race detector: `go test -race -run=TestSchemaCacheConcurrent ./analytics`
  - Must show zero race warnings

- [X] T039 [US2] Run full test suite: `go test ./analytics -v`
  - ALL tests must pass unchanged

- [X] T040 [US2] Verify test coverage: `go test -cover ./analytics`
  - Must maintain ≥85%

**Checkpoint**: ✅ Cache working, 50-70% improvement for repeated URIs, no races, all tests pass

---

## Phase 3: User Story 3 - String Operation Optimization (Priority: P3)

**Goal**: 15-20% performance improvement in string manipulation using strings.Builder

**Independent Test**: Benchmark insertUnderscores before/after optimization, verify identical outputs

### Benchmarks for User Story 3 (REQUIRED - Already Exist, Extend) ✅

- [X] T050 [US3] Verify BenchmarkInsertUnderscores exists in analytics/shred_test.go
  - Already exists (line ~65 in current code)
  - Run and document BEFORE optimization numbers

- [X] T051 [P] [US3] Add benchmark with longer strings in analytics/shred_test.go
  ```go
  func BenchmarkInsertUnderscoresLong(b *testing.B) {
      b.ReportAllocs()
      longString := "ThisIsAReallyLongCamelCaseStringWithManyWordsToTestPerformance"
      for i := 0; i < b.N; i++ {
          insertUnderscores(longString)
      }
  }
  ```

### Implementation for User Story 3

- [X] T052 [US3] Optimize insertUnderscores function in analytics/shred.go
  ```go
  func insertUnderscores(s string) string {
      if len(s) == 0 {
          return s
      }
      
      var b strings.Builder
      b.Grow(len(s) + len(s)/4) // pre-allocate ~25% extra for underscores
      
      for i, r := range s {
          if unicode.IsUpper(r) && i > 0 {
              prev := rune(s[i-1])
              if prev != '_' {
                  b.WriteRune('_')
              }
          }
          b.WriteRune(r)
      }
      return b.String()
  }
  ```
  - **VERIFY**: Run TestInsertUnderscores - must pass unchanged

- [X] T053 [US3] Run benchmarks and verify string operation improvements
  - Compare against T050 baseline
  - Target: 15-20% faster, fewer allocations
  - Document actual improvement

- [X] T054 [US3] Run full test suite: `go test ./analytics -v`
  - ALL tests must pass unchanged

- [X] T055 [US3] Verify test coverage: `go test -cover ./analytics`
  - Must maintain ≥85%

**Checkpoint**: ✅ String operations 15-20% faster, all tests pass, coverage maintained

---

## Phase 4: Validation & Documentation

**Purpose**: Final verification and documentation of performance improvements

- [X] T060 [P] Run complete benchmark suite: `go test -bench=. -benchmem ./analytics > final-benchmarks.txt`

- [X] T061 [P] Compare baseline (T003) vs final benchmarks
  - Document memory allocation improvements (target: 30-40% reduction)
  - Document schema caching improvements (target: 50-70%)
  - Document string operation improvements (target: 15-20%)
  - Overall throughput improvement (target: 20-35%)

- [X] T062 [P] Run full test suite one final time: `go test ./analytics -v`
  - Verify 100% pass rate (same as baseline)
  - No test modifications required

- [X] T063 [P] Verify race detector clean: `go test -race ./analytics`
  - Zero warnings

- [X] T064 [P] Verify final coverage: `go test -cover ./analytics`
  - Must be ≥85% (same or better than baseline)

- [X] T065 [P] Run go vet: `go vet ./analytics`
  - Zero warnings

- [X] T066 Update godoc comments in analytics/shred.go
  - Document shredContexts performance characteristics
  - Note schema caching behavior in fixSchema
  - Note optimized string building in insertUnderscores

- [X] T067 Update godoc comments in analytics/transform.go
  - Document mapifyGoodEvent pre-allocation strategy
  - Document GetSubsetMap optimizations

- [X] T068 Update CHANGELOG file
  ```
  Version 0.4.1 (2026-02-04)
  --------------------------
  Performance optimizations (#XXX)
  Reduce memory allocations by 30-40% in core transformation paths
  Add schema caching for 50-70% improvement on repeated URIs
  Optimize string operations with strings.Builder (15-20% faster)
  All optimizations maintain 100% backward compatibility
  ```

- [X] T069 Update VERSION file
  - Change from 0.4.0 to 0.4.1

- [X] T070 Create performance comparison report
  - Before/after benchmark comparison
  - Memory allocation graphs
  - Real-world scenario improvements
  - Save in specs/001-performance-optimization/performance-report.md

**Checkpoint**: ✅ All improvements documented, benchmarks show targets met, ready for PR

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 0 (Baseline)**: No dependencies - MUST complete first
- **Phase 1 (Allocations)**: Depends on Phase 0 - Creates foundation
- **Phase 2 (Caching)**: Depends on Phase 0 - Independent of Phase 1, can run in parallel if desired
- **Phase 3 (Strings)**: Depends on Phase 0 - Independent of Phase 1/2, can run in parallel if desired
- **Phase 4 (Validation)**: Depends on completion of Phases 1, 2, 3

### User Story Dependencies

- **US1 (P1)**: Independent - can be completed alone as MVP
- **US2 (P2)**: Independent - can be completed without US1
- **US3 (P3)**: Independent - can be completed without US1/US2

### Parallel Opportunities

**Within Phase 1**:
- T010, T011, T012 (benchmark creation) can run in parallel
- T014, T015, T016 (optimizations) must run sequentially to validate each change

**Within Phase 2**:
- T030, T031, T032 (benchmark creation) can run in parallel
- T034, T035, T036 (cache implementation) should run sequentially

**Within Phase 4**:
- T060, T062, T063, T064, T065, T066, T067 can all run in parallel
- T068, T069 depend on T061 (benchmark comparison)

**Across Phases**:
- After Phase 0 complete, Phases 1, 2, 3 can proceed in parallel by different developers
- Each phase validates itself independently

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 0: Baseline
2. Complete Phase 1: Memory allocations (30-40% improvement)
3. **STOP and VALIDATE**: Run all tests, verify no regressions
4. Deploy/release v0.4.1-beta if desired

### Incremental Delivery

1. Phase 0 → Foundation established
2. Phase 1 → 30-40% allocation reduction (MVP!)
3. Phase 2 → Additional 50-70% cache improvement
4. Phase 3 → Additional 15-20% string improvement
5. Phase 4 → Complete validation and documentation
6. Each phase adds value without breaking previous work

### Parallel Team Strategy

With multiple developers:

1. Everyone: Complete Phase 0 baseline together
2. Once baseline documented:
   - Developer A: Phase 1 (Allocations)
   - Developer B: Phase 2 (Caching)
   - Developer C: Phase 3 (Strings)
3. Merge in priority order: P1 → P2 → P3
4. Everyone: Phase 4 validation together

---

## Performance Targets Summary

| Optimization | Target | Measurement |
|--------------|--------|-------------|
| Memory Allocations | 30-40% reduction | <10 allocs per event (from ~15) |
| Schema Caching | 50-70% faster | ops/sec for repeated URIs |
| String Operations | 15-20% faster | insertUnderscores benchmark |
| Overall Throughput | 20-35% improvement | Combined effect on real workload |
| Test Compatibility | 100% pass | All existing tests unchanged |
| Coverage | ≥85% | Maintained or improved |
| Race Conditions | Zero | `go test -race` clean |

---

## Validation Checklist

Before considering this feature complete:

- [ ] All 70 tasks completed
- [ ] Baseline benchmarks documented (Phase 0)
- [ ] All target improvements achieved (Phases 1-3)
- [ ] 100% of existing tests pass without modification
- [ ] Coverage ≥85% maintained
- [ ] Zero race conditions detected
- [ ] `go vet` passes with zero warnings
- [ ] CHANGELOG updated
- [ ] VERSION updated to 0.4.1
- [ ] Godoc comments updated
- [ ] Performance report created
- [ ] Ready for PR and release

**Total Tasks**: 70 (T001-T070)
**Estimated Effort**: 3-5 days for experienced Go developer
**Parallel Opportunities**: 15+ tasks can run concurrently
**MVP Path**: Phase 0 + Phase 1 = ~1-2 days for 30-40% improvement
