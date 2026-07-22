# Issue 25816 MessageBoard Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add temporary, behavior-neutral diagnostics that conclusively distinguish MessageBoard generation replacement from a missing HashBuild terminal send in issue #25816, then reproduce the failure on host 50 and preserve the evidence.

**Architecture:** Give every MessageBoard a process-local monotonic diagnostic ID and log lifecycle/send/receive events with one stable marker. Capture mutable state under existing mutexes, release locks before logging, and do not change registration, reset, delivery, or timeout semantics. Correlate statement ID, board ID, mapped-board ID, JoinMap tag, and event order across the two CN logs.

**Tech Stack:** Go, MatrixOne MessageBoard and HashJoin messaging, MatrixOne CGo test wrapper, dev multi-CN cluster, pprof, Git.

## Global Constraints

- Diagnostics only; do not implement a root-cause fix in this branch.
- Do not log while a MessageBoard or MessageCenter lock is held.
- Do not rely on the existing `reset` debug flag alone; it can be stale after Compile pooling.
- Stage only files named by this plan because the checkout contains unrelated untracked user files.
- Push the diagnostic branch requested by the user; do not open a PR.

---

### Task 1: Pin the diagnostic identity contract with failing tests

**Files:**
- Modify: `pkg/vm/message/message_test.go`

- [x] Add `TestMessageBoardDiagnosticIDIsUnique`: two newly allocated boards have distinct, non-zero IDs.
- [x] Add `TestMessageBoardDiagnosticIDTracksRegisteredGeneration`: a second board calling `SetMultiCN` for the same statement returns the first registered board and its ID.
- [x] Run the new tests and observe the expected compile failure:

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s -run 'TestMessageBoardDiagnosticID(IsUnique|TracksRegisteredGeneration)$' ./pkg/vm/message
```

Expected: FAIL because `MessageBoard.diagnosticID` does not exist.

### Task 2: Implement stable MessageBoard diagnostic identity

**Files:**
- Modify: `pkg/vm/message/message.go`
- Test: `pkg/vm/message/message_test.go`

- [x] Add a package-level `atomic.Uint64`, immutable `diagnosticID uint64`, and assign `Add(1)` in `NewMessageBoard`.
- [x] Add a helper that snapshots ID, statement ID, multi-CN/reset state, and MessageCenter pointer under the board read lock.
- [x] Run the focused tests again; expected PASS.

### Task 3: Add lifecycle and JoinMap event diagnostics

**Files:**
- Modify: `pkg/vm/message/message.go`
- Modify: `pkg/vm/message/joinMapMsg.go`

- [x] Use `issue25816-messageboard` in every diagnostic line and include a process-local monotonic `event_seq`.
- [x] Log `board-new`, `set-multicn-new`, `set-multicn-hit`, `before-run-once`, `reset-multicn`, and `reset-singlecn` after releasing locks.
- [x] Make `reset-multicn` include mapped board ID, map presence, and pointer equality.
- [x] After enqueue and unlock, log `joinmap-send` only for `JoinMapMsg`, including tag, nil status, and shuffle metadata.
- [x] In `ReceiveJoinMap`, log `joinmap-receive-start` before waiting and `joinmap-receive-result` on every actual return path; distinguish message, context cancellation, and error while retaining the board reset snapshot.
- [x] Format and test:

```bash
"$(go env GOROOT)/bin/gofmt" -w pkg/vm/message/message.go pkg/vm/message/joinMapMsg.go pkg/vm/message/message_test.go
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/vm/message
```

### Task 4: Verify and self-review the patch

- [x] Run CGo classification, build, vet, full package tests, and race tests:

```bash
GOWORK=off go list -mod=readonly -deps -test -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./pkg/vm/message
env GOWORK=off CGO_CFLAGS="-I$PWD/cgo -I$PWD/thirdparties/install/include" CGO_LDFLAGS="-L$PWD/thirdparties/install/lib" go build -mod=readonly ./pkg/vm/message
env GOWORK=off CGO_CFLAGS="-I$PWD/cgo -I$PWD/thirdparties/install/include" CGO_LDFLAGS="-L$PWD/thirdparties/install/lib" go vet -mod=readonly ./pkg/vm/message
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/vm/message
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=180s ./pkg/vm/message
```

- [x] Audit that identity is immutable; no log occurs under board/center locks; reset/registration behavior is unchanged; every receive terminal path is logged; nil payloads are safe; cleanup cannot be blocked.
- [x] Inspect `git diff --check`, the exact diff/stat, and `git status --short`.

### Task 5: Commit and push the diagnostic branch

- [ ] Stage only the design, plan, and three diagnostic source/test files.
- [ ] Inspect the staged diff and commit as `chore: trace issue 25816 messageboard lifecycle`.
- [ ] Push `agent/issue25816-messageboard-diagnostics` to origin; do not open a PR.

### Task 6: Deploy the branch to host 50

- [ ] In `mo@10.222.1.50:/home/mo/matrixone`, preserve untracked files, fetch the branch, and switch only tracked source to the pushed commit.
- [ ] Build and restart the existing two-CN dev cluster with `make dev-build TYPECHECK=0` and `make dev-up`, retaining pprof and mounted-source settings.
- [ ] Verify both CNs run the diagnostic binary and emit the marker.

### Task 7: Reproduce and apply the root-cause oracle

- [ ] Run the direct-CN1 control query; preserve its coherent send/receive trace.
- [ ] Run the exact direct-CN2 slow-Hive join; preserve statement ID, time, connection ID, and both CN logs.
- [ ] While blocked, collect both goroutine profiles and all marker lines; confirm `HashJoin.build -> ReceiveJoinMap -> ReceiveMessage`.
- [ ] Apply this oracle:
  - Send and receive board IDs differ, with lifecycle replacement evidence: MessageBoard generation split is confirmed.
  - No matching `joinmap-send` for statement/tag: HashBuild terminal send omission is confirmed.
  - Same board ID plus matching send: neither hypothesis is proven; extend diagnostics and do not guess.
- [ ] Kill only the diagnostic query connection and verify active queries and `ReceiveJoinMap` waiters return to zero.
