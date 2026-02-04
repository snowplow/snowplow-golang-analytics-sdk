# Implementation Plan: Performance Optimization

**Branch**: `001-performance-optimization` | **Date**: 2026-02-04 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-performance-optimization/spec.md`

## Summary

Optimize the Snowplow Golang Analytics SDK to reduce memory allocations by 30-40%, improve schema parsing performance by 50-70% through caching, and accelerate string operations by 15-20% using `strings.Builder`. All optimizations must maintain 100% backward compatibility with identical outputs for identical inputs.

**Technical Approach**: Apply zero-allocation patterns (pre-allocated maps/slices with capacity hints), implement thread-safe schema cache using `sync.RWMutex`, and replace string concatenation with `strings.Builder`. All changes are internal optimizations with no public API modifications.

## Technical Context

**Language/Version**: Go 1.25 (current project version)
**Primary Dependencies**: 
- `github.com/json-iterator/go` v1.1.12 (existing - high-performance JSON)
- `github.com/stretchr/testify` v1.11.1 (existing - testing assertions)
**Storage**: N/A (in-memory event transformation)
**Testing**: `go test`, `testify/assert` for assertions, benchmark tests for performance
**Target Platform**: Cross-platform (Linux, macOS, Windows) - library consumed in data pipelines
**Project Type**: Go library (single package - analytics/)
**Performance Goals**: 
- Parsing: <100µs per enriched event (130 fields)
- Transformation: <50µs for ToMap/ToJson operations
- Memory: <10 allocations per transformation (down from ~15)
- Context shredding: <200µs for 5+ contexts
**Constraints**: 
- Zero breaking changes (patch release v0.4.1)
- Test coverage ≥85% maintained
- No performance regressions >10%
- All existing tests pass without modification
- Thread-safe for concurrent use
**Scale/Scope**: 
- Embedded in production pipelines processing millions of events/hour
- Global customer base requiring API stability
- Three optimization areas: allocations (P1), schema caching (P2), string ops (P3)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Performance-First Design**:
- [x] Benchmark tests planned for data transformation paths? (Principle I)
  - ✅ Benchmarks for ParseEvent, ToMap, ToJson, shredContexts, fixSchema, insertUnderscores
- [x] Performance baseline established for modified code paths?
  - ✅ Will establish baseline before any changes using existing benchmarks
- [x] No performance regressions without justification?
  - ✅ Success criteria requires no regressions, all optimizations tested

**Zero-Allocation Optimization**:
- [x] Pre-allocation strategy defined for maps/slices? (Principle II)
  - ✅ 70-item capacity for event maps, 8-item for context maps, 4-item for context arrays
- [x] String operations use `strings.Builder` or buffers?
  - ✅ insertUnderscores will use strings.Builder with pre-grown capacity
- [x] Memory allocation patterns analyzed?
  - ✅ Target <10 allocations per event (from current ~15)

**Test Coverage & Correctness**:
- [x] Unit tests cover normal, edge, and error cases? (Principle III - NON-NEGOTIABLE)
  - ✅ All existing tests must pass; new benchmarks track allocations
- [x] Benchmark tests planned for performance-critical paths?
  - ✅ Yes - with b.ReportAllocs() for allocation tracking
- [x] Test coverage target ≥85%?
  - ✅ Must maintain or improve existing coverage

**API Stability & Backward Compatibility**:
- [x] Semantic versioning impact assessed? (Principle IV)
  - ✅ Patch release v0.4.1 - no breaking changes, internal optimizations only
- [x] Breaking changes documented with migration path?
  - ✅ N/A - no breaking changes
- [x] Deprecation warnings planned if needed?
  - ✅ N/A - no API changes

**Error Transparency**:
- [x] Error messages include context (field names, values)? (Principle V)
  - ✅ All existing error handling preserved unchanged
- [x] All error paths tested?
  - ✅ Existing error tests must continue passing
- [x] No silent failures?
  - ✅ No changes to error handling behavior

**Documentation as Contract**:
- [x] Godoc comments planned for all exports? (Principle VI)
  - ✅ Update godocs to note performance improvements
- [x] Examples prepared for complex APIs?
  - ✅ No new APIs; existing examples remain valid
- [x] README updates identified?
  - ✅ CHANGELOG update for v0.4.1 performance improvements

## Project Structure

### Documentation (this feature)

```text
specs/001-performance-optimization/
├── spec.md              # Feature specification (created)
├── plan.md              # This file (implementation plan)
├── checklists/
│   └── requirements.md  # Specification quality checklist (created)
└── tasks.md             # Task breakdown (to be generated)
```

### Source Code (repository root)

```text
analytics/                          # Main package (existing)
├── shred.go                       # WILL MODIFY: shredContexts, fixSchema, insertUnderscores
├── shred_test.go                  # WILL EXTEND: add allocation benchmarks
├── transform.go                   # WILL MODIFY: mapifyGoodEvent, GetSubsetMap
├── transform_test.go              # WILL EXTEND: add allocation benchmarks
├── mappings.go                    # READ ONLY: field mappings (no changes)
└── vars_test.go                   # READ ONLY: test data (no changes)

go.mod                             # READ ONLY: no new dependencies
CHANGELOG                          # WILL UPDATE: add v0.4.1 entry
VERSION                            # WILL UPDATE: 0.4.0 → 0.4.1
README.md                          # READ ONLY: no API changes to document
```

**Structure Decision**: This is a Go library with a single package structure. All source code resides in `analytics/` with colocated tests. Modifications will be made to existing files only - no new files or packages needed. This keeps the change focused and minimal.

## Complexity Tracking

> **Not applicable** - All constitution checks passed. No violations to justify.

## Implementation Phases

### Phase 0: Baseline & Preparation (REQUIRED FIRST)

**Purpose**: Establish performance baseline and verify test suite

- Document current benchmark results (allocations, ops/sec)
- Run full test suite and verify 100% pass rate
- Run `go test -race` to verify no existing race conditions
- Review existing code to identify optimization targets
- Create backup branch for comparison

**Deliverables**: Baseline benchmark report, confirmed test stability

---

### Phase 1: User Story 1 - Memory Allocation Optimization (P1) 🎯 MVP

**Goal**: Reduce memory allocations by 30-40% in hot paths

**Files Modified**:
- `analytics/shred.go` - Pre-allocate maps in shredContexts
- `analytics/transform.go` - Pre-allocate maps in mapifyGoodEvent, GetSubsetMap
- `analytics/shred_test.go` - Add allocation benchmarks
- `analytics/transform_test.go` - Add allocation benchmarks

**Success Criteria**: Allocations <10 per event, all tests pass, coverage ≥85%

---

### Phase 2: User Story 2 - Schema Cache Implementation (P2)

**Goal**: 50-70% performance improvement for repeated schema URIs

**Files Modified**:
- `analytics/shred.go` - Add schema cache with sync.RWMutex, modify fixSchema to use cache
- `analytics/shred_test.go` - Add cache benchmarks, race condition tests

**Success Criteria**: Cache hit rate >90% for typical batches, no race conditions, all tests pass

---

### Phase 3: User Story 3 - String Operation Optimization (P3)

**Goal**: 15-20% performance improvement in string operations

**Files Modified**:
- `analytics/shred.go` - Optimize insertUnderscores with strings.Builder
- `analytics/shred_test.go` - Add string operation benchmarks

**Success Criteria**: Fewer allocations in string ops, all tests pass, 15-20% faster

---

### Phase 4: Validation & Documentation

**Goal**: Final verification and documentation updates

**Files Modified**:
- `CHANGELOG` - Add v0.4.1 entry documenting performance improvements
- `VERSION` - Update to 0.4.1
- `analytics/*.go` - Update godoc comments noting performance characteristics

**Success Criteria**: All benchmarks show improvements, no regressions, documentation complete

## Dependencies

**External**: None (using standard library only)

**Internal**: 
- Must maintain compatibility with existing `analytics` package public API
- Must not change behavior of any public functions
- Must pass all existing test suite without modification

## Risk Mitigation

**Risk**: Cache grows unbounded with unique schemas
- **Mitigation**: Document expected memory usage (<10MB for <1000 schemas); consider future TTL/LRU if needed

**Risk**: Thread-safety bugs in cache implementation
- **Mitigation**: Use `go test -race` extensively; RWMutex for read-heavy workload; thorough concurrent testing

**Risk**: Optimization changes output behavior subtly
- **Mitigation**: Run full test suite after each change; compare outputs byte-for-byte; use table-driven tests

**Risk**: Performance improvements don't materialize
- **Mitigation**: Benchmark before/after each change; rollback if <10% improvement; document actual gains

## Rollback Plan

If any phase fails validation:
1. Revert commits for that phase
2. Re-run baseline tests to confirm stability
3. Analyze failure and adjust approach
4. All phases are independent - can ship P1 without P2/P3

## Testing Strategy

**Unit Tests**: All existing tests must pass without modification (backward compatibility proof)

**Benchmark Tests**: New benchmarks added for each optimization with b.ReportAllocs()

**Race Detection**: `go test -race ./...` must pass with zero warnings

**Coverage**: Maintain ≥85% coverage across all modified files

**Validation**: Compare outputs byte-for-byte using existing test fixtures

## Approval Gates

- Phase 0 complete: Baseline documented, tests stable
- Phase 1 complete: Allocations reduced, all tests pass
- Phase 2 complete: Cache working, no races, tests pass
- Phase 3 complete: Strings optimized, tests pass
- Final: Benchmarks show cumulative 20-35% improvement, documentation updated

**Ready for Task Generation**: ✅ Yes - plan is complete and validated
