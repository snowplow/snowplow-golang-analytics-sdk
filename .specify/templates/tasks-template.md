---

description: "Task list template for feature implementation"
---

# Tasks: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: The examples below include test tasks. Tests are OPTIONAL - only include them if explicitly requested in the feature specification.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Go Library (this SDK)**: `analytics/` (main package), `cmd/` (CLI tools), `internal/` (private utilities), `examples/` (usage demos)
- **Go Service**: `cmd/[service]/`, `pkg/` (public packages), `internal/` (private code)
- **Go CLI**: `cmd/[tool]/`, `pkg/` (reusable packages)
- Paths shown below assume Go library structure - adjust based on plan.md structure
- Tests colocated: `analytics/[name]_test.go`, benchmarks: `analytics/[name]_bench_test.go`

<!-- 
  ============================================================================
  IMPORTANT: The tasks below are SAMPLE TASKS for illustration purposes only.
  
  The /speckit.tasks command MUST replace these with actual tasks based on:
  - User stories from spec.md (with their priorities P1, P2, P3...)
  - Feature requirements from plan.md
  - Entities from data-model.md
  - Endpoints from contracts/
  
  Tasks MUST be organized by user story so each story can be:
  - Implemented independently
  - Tested independently
  - Delivered as an MVP increment
  
  DO NOT keep these sample tasks in the generated tasks.md file.
  ============================================================================
-->

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create project structure per implementation plan (analytics/, cmd/, internal/ as needed)
- [ ] T002 Update go.mod with new dependencies (if needed)
- [ ] T003 [P] Configure golint and gofmt standards (if not already configured)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

Examples of foundational tasks (adjust based on your project):

- [ ] T004 Define core types and interfaces in analytics/[types].go
- [ ] T005 [P] Implement error handling patterns with wrapped errors
- [ ] T006 [P] Create test fixtures and helpers in analytics/[fixture]_test.go
- [ ] T007 Establish performance baseline with benchmarks
- [ ] T008 Setup memory profiling utilities (if needed)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - [Title] (Priority: P1) 🎯 MVP

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 1 (REQUIRED per Constitution Principle III) ✅

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation (RED-GREEN-REFACTOR)**

- [ ] T010 [P] [US1] Unit tests for [function] in analytics/[name]_test.go
  - Normal cases (happy paths)
  - Edge cases (boundary conditions, empty inputs)
  - Error cases (invalid inputs, error propagation)
- [ ] T011 [P] [US1] Benchmark tests in analytics/[name]_bench_test.go
  - Measure operations per second
  - Track allocations with b.ReportAllocs()
  - Document baseline performance

### Implementation for User Story 1

- [ ] T012 [P] [US1] Implement core function in analytics/[name].go
- [ ] T013 [US1] Optimize for zero allocations (pre-allocate maps/slices, use strings.Builder)
- [ ] T014 [US1] Add godoc comments with examples
- [ ] T015 [US1] Add error context with fmt.Errorf("%w", err)
- [ ] T016 [US1] Verify tests pass (GREEN)
- [ ] T017 [US1] Run benchmarks, compare against baseline
- [ ] T018 [US1] Verify coverage ≥85% with `go test -cover`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - [Title] (Priority: P2)

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 2 (REQUIRED per Constitution Principle III) ✅

- [ ] T020 [P] [US2] Unit tests in analytics/[name]_test.go (normal, edge, error cases)
- [ ] T021 [P] [US2] Benchmark tests in analytics/[name]_bench_test.go

### Implementation for User Story 2

- [ ] T022 [P] [US2] Implement core function in analytics/[name].go
- [ ] T023 [US2] Optimize memory allocations
- [ ] T024 [US2] Add godoc and examples
- [ ] T025 [US2] Verify tests pass and coverage ≥85%

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - [Title] (Priority: P3)

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 3 (REQUIRED per Constitution Principle III) ✅

- [ ] T026 [P] [US3] Unit tests in analytics/[name]_test.go (normal, edge, error cases)
- [ ] T027 [P] [US3] Benchmark tests in analytics/[name]_bench_test.go

### Implementation for User Story 3

- [ ] T028 [P] [US3] Implement core function in analytics/[name].go
- [ ] T029 [US3] Optimize memory allocations
- [ ] T030 [US3] Add godoc and examples
- [ ] T031 [US3] Verify tests pass and coverage ≥85%

**Checkpoint**: All user stories should now be independently functional

---

[Add more user story phases as needed, following the same pattern]

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] TXXX [P] Update README.md with new API examples
- [ ] TXXX [P] Update CHANGELOG with semantic version
- [ ] TXXX Code cleanup and refactoring (if needed)
- [ ] TXXX Performance validation: no regressions >10%
- [ ] TXXX Security review (if handling untrusted input)
- [ ] TXXX Final coverage check: ensure ≥85% maintained
- [ ] TXXX Run all benchmarks: `go test -bench=. -benchmem`
- [ ] TXXX Verify `go vet` and `golint` pass

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1 but should be independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - May integrate with US1/US2 but should be independently testable

### Within Each User Story

- Tests (if included) MUST be written and FAIL before implementation
- Models before services
- Services before endpoints
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All tests for a user story marked [P] can run in parallel
- Models within a story marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch all tests for User Story 1 together (if tests requested):
Task: "Contract test for [endpoint] in tests/contract/test_[name].py"
Task: "Integration test for [user journey] in tests/integration/test_[name].py"

# Launch all models for User Story 1 together:
Task: "Create [Entity1] model in src/models/[entity1].py"
Task: "Create [Entity2] model in src/models/[entity2].py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
