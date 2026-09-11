# MVCC Garbage Collection

## Overview

The MVCC store keeps every row version in memory: inserts, updates, deletes,
and rolled-back garbage. Without GC, memory grows with write volume. GC
reclaims versions that no active reader can see and that are redundant with
the B-tree.

GC is driven by:

- **LWM (low-water mark)**: `min(tx.begin_ts)` across Active/Preparing
  transactions, or `u64::MAX` if none.
- **ckpt_max** (`durable_txid_max`): highest commit timestamp whose data was
  written through a finished pager commit for that checkpoint.
- **materialized_at**: WAL position stamped on a version only after the pager
  write for that key finished and the pager transaction committed. `ORIGIN`
  means "not stamped yet."
- **min_reader_mark**: lowest reader mark among MVCC txs, pager-pinned WAL
  readers not yet in `txs`, and `backfill_floor`.

All chain rules live in `gc_version_chain`, shared by checkpoint write-set GC,
incremental GC, and Finalize sweeps.

## Rules (in order)

1. **Aborted garbage** (`begin=None, end=None`) — remove.
2. **Superseded versions** (`end=Timestamp(e), e ≤ lwm`) — remove when safe:
   - Passive: only once stamped and `min_reader_mark >= materialized_at`.
   - Truncate: tombstone / `btree_resident` / `ckpt_max` guards (#7638).
3. **Sole current** (`end` unpacks as `None`, chain length 1) — `clear()` the
   chain when:
   - `drop_current_if_in_btree` is true (every live path),
   - the version is stamped and reachable by every reader mark
     (`materialized_for_readers`),
   - Truncate also requires `b ≤ ckpt_max`,
   - **and `lwm == u64::MAX`** (no open snapshot).

There is no retirement / `RETIRED_TAG` / Rule 3b. Rule 3 drops the SkipMap
copy outright. Last-current reclaim is **idle-only** on both Passive and
Truncate. Under load, Rule 2 still reclaims superseded history; sole currents
wait for quiescence.

## Why Rule 3 is idle-only

A dual-cursor scan reads the SkipMap and the B-tree at different moments.
Emptying a sole current mid-scan can drop the row from both sides (B-tree
already skipped it as shadowed; MVCC chain is gone). Passive never falls
through to the B-tree for live readers. Truncate incremental GC runs under
the checkpoint *read* lock concurrent with readers, so the same hazard
applies. `lwm == MAX` is the shared proof that no dual-cursor is in flight.

Truncate Finalize still reclaims last currents: the blocking write lock waits
out open MVCC txs, so LWM is `MAX` for that pass.

## Materialization stamps

Stamps are **not** applied when a row is handed to the pager. They run in
`GcTableRows` / `GcIndexRows` after `CommitPagerTxn`, and only for keys whose
pager write or delete finished (`written_table_rowids` /
`written_index_slots`). Skip-writes stay `ORIGIN`, so a high `ckpt_max` alone
never means "this leaf was written."

## Lock order

Sample LWM under the clock (`sample_gc_lwm`) **before** taking any
version-chain write lock, then call `gc_chain_now(lwm, ...)`. Never take the
clock while holding a chain lock. Passive publish takes the clock then
`seqcompact_commit_delete` → chain write; the opposite order deadlocks.

## When GC runs

1. **Incremental** (`gc_incremental` on commit): bounded table + index sweeps.
2. **Checkpoint write-set** (`gc_checkpointed_*`): stamp then GC keys this
   checkpoint wrote.
3. **Finalize**: Truncate uses `drop_unused_row_versions_and_slots`; Passive
   uses `drop_unused_row_versions_unlink_empty_at(gc_floor_reader_mark())`.

## Dual cursor / write buffer

`chain_is_write_buffer_for` is false only for a sole stamped current inside
the durable bound. Truncate may fall through to the B-tree for that shape
when no checkpoint is in progress. Passive never falls through for live
readers; it keeps SkipMap cover until idle Rule 3 clears the chain.

Hazards still guarded by Rule 2:

- Removing a tombstone before its delete is durable resurrects a B-tree row.
- Removing a current while leaving superseded history hides the B-tree row
  with no MVCC payload left to serve.

## Recovery versions

Logical-log replay rebuilds each version with the `commit_ts` recorded in the
log, so recovered versions look like any other committed version. What keeps
them safe is `ckpt_max`: `durable_txid_max` starts at 0 and only rises when a
checkpoint publishes a durable bound, so every recovered `begin`/`end` is
above it and Rules 2/3 cannot fire. Recovered versions are also unstamped
(`materialized_at == ORIGIN`), which blocks the Passive path independently.

## Passive `backfill_floor`

Passive Rule 2 / Rule 3 reader marks are clamped by `backfill_floor` so a
version materialized in un-backfilled WAL frames is not dropped while a
db-file reader might still need the SkipMap copy.

## Key files

| File | Contents |
|------|----------|
| `core/mvcc/database/mod.rs` | `sample_gc_lwm`, `gc_chain_now`, `gc_version_chain`, `stamp_chain_materialized`, incremental + Finalize sweeps |
| `core/mvcc/database/checkpoint_state_machine.rs` | `written_*`, stamp sites, `gc_checkpointed_*`, `gc_floor_reader_mark` |
| `core/mvcc/database/tests.rs` | Rule 2/3 unit tests, Passive transfer / unique-miss regressions |
