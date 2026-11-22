# Compaction

`pg-cdc compact` merges delta files into a new base snapshot. This reduces file count, speeds up consumer refresh, and manages tombstones.

## What Happens

```
pg-cdc compact
  1. Read the manifest once and PIN the compaction scope per table:
     deltas with base_epoch < epoch <= latest_delta_epoch
  2. For each active table:
     a. Read current base Parquet into memory (keyed by PK)
     b. Read only the pinned delta files (in epoch order) and apply:
        - I (insert): add row
        - U (update): replace row by PK
        - D (delete): mark row with __deleted_at tombstone
     c. Purge tombstones older than 30 days
     d. Write new base Parquet (base epoch = pinned latest_delta_epoch)
     e. Record merged deltas in compaction-journal.json — nothing is deleted
  3. Re-read the manifest and merge base advances onto the FRESH state,
     so batches the streamer committed during the run are preserved
  4. Write the updated manifest (new base_epoch, row_count)
  5. Purge journal entries whose handoff window has elapsed AND whose
     covering base is published
```

## Scope pinning vs. the live streamer

Compaction runs while `pg-cdc start` keeps streaming. The two coordinate without locking:

- **What gets merged is fixed at start-up.** Deltas the streamer lands after
  compaction reads its manifest are outside the run's pinned scope. They are
  not read, not merged, and never deleted by this run.
- **The manifest is re-read before publishing.** If the streamer advanced
  `latest_delta_epoch` during the run, the new base is published at the
  pinned tip while the newer `latest_delta_epoch` is preserved. Readers then
  see the new base plus the newer deltas on top of it.
- **A missing manifest-confirmed delta aborts the table.** An epoch gap in
  the pinned range fails the run rather than publishing a base with a data
  hole that later deltas could not repair.

## Delta handoff window

Merged deltas are **not** deleted when the base is published. Instead each is
written to `compaction-journal.json` (alongside `manifest.json` in the sink)
with its compaction timestamp and covering base epoch. A delta is deleted
only when **both** conditions hold:

1. It is older than `compaction.delta_retention` (default 7 days), and
2. The covering base is published in the current manifest
   (`base_epoch >=` the entry's base epoch).

The second condition protects against a crash between journaling and manifest
commit: an entry whose base never shipped is never reclaimed, so the delta
stays authoritative. Deletion failures also leave the journal entry in place;
the next run retries.

This guarantees that a consumer working from an older manifest can always
download every delta it references — compaction cannot delete a file that has
not outlived the handoff window.

## Tombstones

When a row is deleted, compaction doesn't remove it immediately. Instead, it marks it with `__deleted_at`:

```
Row: {id: 50, name: "...", __deleted_at: "2026-04-12T14:00:00Z"}
```

This ensures consumers who haven't refreshed recently can still propagate the delete. Tombstones are purged after 30 days.

If a consumer is offline longer than the tombstone retention period, their next `pg-warehouse refresh` detects the gap (local epoch < base_epoch) and does a full re-pull from the latest base.

## Governance During Compaction

Compaction only processes tables in `manifest.ActiveTables()`. Excluded tables are untouched. Their manifest entries (status=excluded, tags, reason) are preserved.

## Usage

```bash
# Retention from compaction.delta_retention in pg-cdc.yml (default 168h)
pg-cdc compact --config pg-cdc.yml

# Override the handoff window for one run
pg-cdc compact --config pg-cdc.yml --delta-retention 72h
```

## When to Compact

- **After accumulating many deltas** — hundreds of small files slow down consumer refresh
- **On a schedule** — hourly or daily via cron
- **Before a consumer's first refresh** — fewer files to download

Run compaction more often than the retention window demands space: retained
deltas only occupy storage; they never block future compactions, because each
run re-derives its scope from the current base epoch.

## Output

```
pg-cdc compact
  public.orders: compacted 15 deltas → base_000015 (10042 rows, 18 applied, 0 purged); deltas retained for handoff
  public.customers: up to date (base=0, latest=0)
  public.products: compacted 3 deltas → base_000003 (100 rows, 5 applied, 1 purged); deltas retained for handoff
Purged 12 delta files past the handoff window
Compaction complete
```
