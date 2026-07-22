# Issue 25816 MessageBoard Diagnostic Design

## Goal

Add temporary, observation-only diagnostics that determine why a Multi-CN
hash join can wait forever for `JoinMapMsg`: either the sender and receiver use
different `MessageBoard` instances for one statement, or the hash-build path
never sends a terminal join-map message.

This branch must not fix or otherwise change MessageBoard, HashBuild, or
HashJoin lifecycle semantics.

## Reproduction Contract

- Source revision: `c0486a2446c84c6dd6bf142494fe85243877ddc5`.
- Environment: host 50, two CNs, one TN, one Log service, and proxy.
- Data: 2,000 Hive-style parquet partitions in MinIO.
- Timing amplifier: Toxiproxy adds `2000 +/- 200 ms` only to the MinIO response
  path; CN-to-CN RPC is not modified.
- Control: direct CN1 execution completes in about 21 seconds.
- Failure: direct CN2 execution leaves remote CN1 in
  `HashJoin.build -> ReceiveJoinMap -> ReceiveMessage`.

## Considered Approaches

### 1. Log raw pointers only

Log `%p` for each board at send, receive, registration, and reset sites.

This is the smallest edit, but pointer reuse across the Compile pool makes
cross-generation analysis error-prone. It also makes automated assertions and
log grouping harder.

### 2. Stable per-board diagnostic identity and structured lifecycle events

Assign each new `MessageBoard` a monotonically increasing diagnostic ID and log
the statement ID, board ID, event, join-map tag, and relevant map identity at
registration, reset, send, and receive boundaries.

This is the selected approach. It distinguishes board generations even when Go
reuses memory addresses, and it directly answers both remaining hypotheses in
one deterministic reproduction.

### 3. External debugger or heap/core inspection

Use Delve, ptrace, or a core dump without source changes.

This avoids logging overhead, but cannot reconstruct a completed send or an
earlier `StmtIDToBoard` deletion reliably. It is less deterministic and more
expensive operationally.

## Instrumentation

Only `pkg/vm/message/message.go`, `pkg/vm/message/joinMapMsg.go`, and their unit
tests may change.

Each `MessageBoard` receives an immutable `diagnosticID uint64` assigned by a
package-level atomic counter in `NewMessageBoard`.

The following events are emitted at info level with a common marker
`issue25816-messageboard`:

| Event | Required fields |
|---|---|
| `board-new` | `board_id` |
| `set-multicn-new` | `stmt_id`, candidate `board_id` |
| `set-multicn-hit` | `stmt_id`, existing and candidate `board_id` |
| `before-run-once` | `stmt_id`, `board_id`, prior reset flag |
| `reset-multicn` | `stmt_id`, resetting `board_id`, currently mapped `board_id`, pointer-match boolean |
| `reset-singlecn` | `board_id`, prior reset flag |
| `joinmap-send` | `stmt_id`, `board_id`, tag, nil-map, shuffle fields |
| `joinmap-receive-start` | `stmt_id`, `board_id`, tag, shuffle fields |
| `joinmap-receive-result` | `stmt_id`, `board_id`, tag, result (`map`, `nil-map`, or `context-done`) |

Mutable board metadata used for logging must be read under the board RWMutex.
The immutable diagnostic ID may be read without locking. Logging must not add a
new lock-order inversion: `MessageCenter` remains outer to `MessageBoard` where
both are needed.

## Root-Cause Oracle

One direct-CN2 reproduction is sufficient when it yields one of these complete
event chains:

1. A `joinmap-send` exists on board A while the stuck
   `joinmap-receive-start` uses board B for the same statement and tag. The root
   cause is MessageBoard generation split/replacement; `reset-multicn` events
   identify the exact deletion/re-registration sequence.
2. The receiver starts on board A but no matching `joinmap-send` exists on any
   board for the same statement and tag. The root cause is a HashBuild terminal
   path that does not send; the last lifecycle event bounds the missing edge.
3. Send and receive use the same board and tag. In that case the captured board
   queue/waiter events are insufficient, and the investigation must stop rather
   than infer a cause; no fix is authorized from this branch.

The existing `reset` debug flag is not accepted as a root-cause oracle because
it can survive Compile pool reuse before the current remote statement.

## Verification

- Add focused tests proving diagnostic IDs are non-zero, unique per newly
  allocated board, stable when `SetMultiCN` hits an existing board, and changed
  after Multi-CN reset returns a new board.
- Run the `pkg/vm/message` build, vet, focused tests, full package tests, and
  race tests using the MatrixOne CGo wrapper when the dependency graph requires
  it.
- Build the diagnostic MO image on host 50 using `make dev-build TYPECHECK=0`.
- Recreate the existing two-CN dev cluster without deleting reproduction data,
  rerun direct CN1 control and direct CN2 failure, and collect all marker events
  by statement ID.
- Kill any diagnostic query left waiting and verify no `ReceiveJoinMap`
  goroutine remains.

## Delivery And Rollback

- Push only this design, its implementation plan, the diagnostic source edits,
  and focused tests to `origin/agent/issue25816-messageboard-diagnostics`.
- Do not open a merge-ready PR; this is a temporary diagnostic branch.
- Host 50 pulls that exact branch and records the built commit SHA.
- After evidence collection, keep the remote branch for auditability but do not
  merge it. Restore host 50 to `c0486a2` when the user requests cleanup.
