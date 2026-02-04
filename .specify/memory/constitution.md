<!--
Sync Impact Report - v1.1.0 Memory Management Principle
========================================================
Version Change: 1.0.0 → 1.1.0 (MINOR: new principle added)
Modified Principles: Added Principle VII (Memory Management & Resource Bounds)
Added Sections: 
  - Principle VII: Memory Management & Resource Bounds (addresses unbounded cache growth identified in v0.4.1)
Removed Sections: None
Reason for Amendment: v0.4.1 performance optimization introduced unbounded schema cache without limits, violating production stability requirements. New principle establishes clear constraints on memory-persistent state.
Templates Status:
  ✅ All templates remain compatible - no changes required
  ⚠️ Future performance tasks must include bounded cache designs per Principle VII
Follow-up TODOs: 
  - v0.4.2 implementation: Add LRU cache with configurable limit per Principle VII requirements
  - Document cache behavior in godoc per Principle VI
-->

# Snowplow Golang Analytics SDK Constitution

## Core Principles

### I. Performance-First Design

All code changes MUST demonstrate performance awareness. New features or modifications SHALL NOT introduce performance regressions without explicit justification and approval. Benchmark tests are REQUIRED for data transformation paths, parsing logic, and memory-intensive operations.

**Rationale**: This SDK processes high-volume event streams where microseconds matter. Performance degradation directly impacts customer infrastructure costs and real-time processing capabilities.

### II. Zero-Allocation Optimization

Memory allocation patterns MUST be minimized through pre-allocation, object reuse, and efficient data structures. String operations SHALL use `strings.Builder` or pre-allocated buffers. Maps and slices MUST be pre-allocated with capacity hints when size is predictable.

**Rationale**: Garbage collection pressure in high-throughput scenarios causes latency spikes. Minimizing allocations ensures consistent, predictable performance at scale.

### III. Test Coverage & Correctness (NON-NEGOTIABLE)

Every public function MUST have comprehensive unit tests covering normal cases, edge cases, and error conditions. Benchmark tests are REQUIRED for performance-critical paths. Test coverage SHALL NOT decrease below 85%. All tests MUST pass before merge.

**Rationale**: Data transformation errors corrupt analytics pipelines silently. Comprehensive testing is the only guarantee of correctness in production.

### IV. API Stability & Backward Compatibility

Public API changes MUST follow semantic versioning strictly: MAJOR for breaking changes, MINOR for backward-compatible additions, PATCH for bug fixes. Breaking changes require migration documentation and deprecation warnings spanning at least one minor version.

**Rationale**: This SDK is embedded in production pipelines globally. Breaking changes cascade to thousands of downstream systems, requiring coordinated upgrades.

### V. Error Transparency

Errors MUST include sufficient context for debugging: field names, values causing errors, and operation context wrapped using `fmt.Errorf` with `%w`. Silent failures or swallowed errors are PROHIBITED. All error paths MUST be tested.

**Rationale**: Event parsing failures in production must be diagnosable from error messages alone. Incomplete error context forces engineers to instrument code retrospectively.

### VI. Documentation as Contract

Every exported function, type, and constant MUST have godoc comments describing purpose, parameters, return values, and error conditions. Examples MUST be provided for complex APIs. README MUST stay synchronized with API changes.

**Rationale**: Golang's documentation culture expects inline docs. Missing documentation signals incomplete APIs and creates support burden.

### VII. Memory Management & Resource Bounds

Caches, buffers, and in-memory state MUST have explicit size limits to prevent unbounded growth. Long-lived maps or slices SHALL use LRU eviction, TTL expiry, or periodic clearing. Memory-persistent structures MUST document maximum memory footprint and growth characteristics in godoc and README.

**Rationale**: Unbounded caches in long-running services cause memory leaks and eventual OOM crashes. Production systems require predictable resource consumption for capacity planning and stability guarantees.

## Performance Standards

### Benchmark Requirements

- **Parsing**: Target <100µs per enriched event for typical payloads (130 fields)
- **Transformation**: Target <50µs for ToMap/ToJson operations
- **Memory**: Target <10 allocations per event transformation
- **Context Shredding**: Target <200µs for events with 5+ contexts

All benchmark tests MUST track allocations (`b.ReportAllocs()`) and operations per second. Regressions >10% require explicit approval.

### Optimization Priority

1. **Hot paths**: ParseEvent, ToMap, ToJson, shredContexts - optimize aggressively
2. **Warm paths**: GetValue, GetSubsetMap - optimize opportunistically  
3. **Cold paths**: Schema parsing, validation - correctness over speed

## Development Workflow

### Change Process

1. **Proposal**: For API changes or performance optimizations, create GitHub issue with benchmarks
2. **Tests**: Write or update tests BEFORE implementation (Red-Green-Refactor)
3. **Implementation**: Make minimal changes to pass tests
4. **Benchmarks**: Run benchmarks, compare against baseline, document results
5. **Review**: Two approvals required for breaking changes, one for non-breaking
6. **Documentation**: Update godocs, README, CHANGELOG before merge

### Quality Gates

- All tests pass (`go test ./...`)
- Benchmarks show no regressions or regressions are justified
- Code coverage ≥85% for modified packages
- `go vet` and `golint` pass with zero warnings
- No panics in production code paths (test code may panic)

### Dependencies

- Minimize external dependencies - standard library preferred
- New dependencies require justification: performance benefit, reduced maintenance burden, or essential functionality
- Pin dependencies with `go.mod` for reproducible builds
- Security vulnerabilities in dependencies addressed within 14 days

## Governance

This constitution supersedes informal practices and tribal knowledge. All pull requests MUST demonstrate compliance with these principles. Violations require explicit justification documented in PR description.

**Amendment Pro1.0 | **Ratified**: 2026-02-03 | **Last Amended**: 2026-02-04ew by maintainers, and approval by majority. Breaking principle changes require MAJOR version bump of constitution itself.

**Compliance Review**: Every quarterly release cycle includes constitution compliance audit. Non-compliance tracked as technical debt with remediation plans.

**Enforcement**: Maintainers may reject non-compliant PRs without detailed review. Contributors should reference specific principles in PR descriptions demonstrating compliance.

**Version**: 1.0.0 | **Ratified**: 2026-02-03 | **Last Amended**: 2026-02-03
