package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"

	goparquet "github.com/parquet-go/parquet-go"
)

// defaultDeltaRetention is how long a delta stays readable after being folded
// into a published base. It must cover the slowest consumer's manifest refresh
// interval; 7 days matches the documented tombstone handoff guidance.
const defaultDeltaRetention = 7 * 24 * time.Hour

// compactClock is indirected so tests can pin compaction time and exercise
// handoff-window expiry without real sleeps.
var compactClock = func() time.Time { return time.Now().UTC() }

// SetCompactClockForTest overrides the compaction clock and returns a restore
// function. Test-only.
func SetCompactClockForTest(now func() time.Time) func() {
	previous := compactClock
	compactClock = now
	return func() { compactClock = previous }
}

// ResolveDeltaRetention parses the configured handoff window, falling back to
// the package default when unconfigured or unparseable.
func ResolveDeltaRetention(configured string) time.Duration {
	if configured == "" {
		return defaultDeltaRetention
	}
	d, err := time.ParseDuration(configured)
	if err != nil || d <= 0 {
		fmt.Printf("  warning: invalid compaction.delta_retention %q, using default %s\n", configured, defaultDeltaRetention)
		return defaultDeltaRetention
	}
	return d
}

// Compact merges delta Parquet files into a new base snapshot for each table.
//
// Scope safety: the set of deltas merged is pinned to the manifest read at the
// start of the run. Delta files the running streamer lands while compaction is
// in progress are neither read nor deleted, and the manifest is re-read before
// publishing so concurrently committed batches are preserved.
//
// Deletion safety: compacted deltas are never deleted inline. They are
// recorded in the sink-side compaction journal and removed only after the
// handoff retention window elapses and the covering base is published, so
// consumers following an older manifest never reference a missing file.
func Compact(ctx context.Context, sink ports.Sink, deltaRetention time.Duration) error {
	// The start-up manifest is the sole authority for what this run may merge.
	startManifest, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	// Snapshot the per-table scope before touching any data. Values in this
	// map are frozen; later manifest refreshes never widen the scope.
	startTables := startManifest.ActiveTables()
	fmt.Printf("Compacting %d tables (scope pinned to start-of-run manifest)\n", len(startTables))

	journal, err := loadCompactionJournal(ctx, sink)
	if err != nil {
		return fmt.Errorf("load compaction journal: %w", err)
	}

	now := compactClock()
	var compactedAny bool
	for tableName, table := range startTables {
		if table.LatestDeltaEpoch <= table.BaseEpoch {
			fmt.Printf("  %s: up to date (base=%d, latest=%d)\n", tableName, table.BaseEpoch, table.LatestDeltaEpoch)
			continue
		}

		schema, ok := startManifest.Schemas[tableName]
		if !ok {
			fmt.Printf("  %s: skip (no schema)\n", tableName)
			continue
		}

		handoffs, err := compactTable(ctx, sink, tableName, table, schema, now)
		if err != nil {
			return fmt.Errorf("compact %s: %w", tableName, err)
		}
		if len(handoffs) > 0 {
			journal.add(handoffs...)
			compactedAny = true
		}
	}

	if !compactedAny {
		// Still run a purge: a previous run may have left entries whose
		// retention window has since elapsed.
		if purged, err := purgeDueDeltas(ctx, sink, journal, startManifest, deltaRetention, now); err != nil {
			return fmt.Errorf("purge expired deltas: %w", err)
		} else if purged > 0 {
			fmt.Printf("Purged %d delta files past the handoff window\n", purged)
		}
		fmt.Println("Compaction complete (no new bases)")
		return nil
	}

	// Re-read the manifest immediately before publishing. The streamer may
	// have committed new batches (and evolved schemas) while this run was
	// working; merge this run's base advances onto that fresh state instead
	// of overwriting it with the stale start-of-run copy.
	fresh, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("re-read manifest before publish: %w", err)
	}
	for tableName, table := range startTables {
		if table.LatestDeltaEpoch <= table.BaseEpoch {
			continue
		}
		if _, ok := startManifest.Schemas[tableName]; !ok {
			continue
		}
		freshTable, ok := fresh.Tables[tableName]
		if !ok {
			// Table was removed (teardown) during the run; its new base is
			// orphaned but harmless. Don't resurrect it in the manifest.
			continue
		}
		// Publish only what this run actually covered. A concurrently advanced
		// LatestDeltaEpoch stays ahead of the new BaseEpoch, so later batches
		// remain readable as deltas on top of the new base.
		if table.LatestDeltaEpoch > freshTable.BaseEpoch {
			freshTable.BaseEpoch = table.LatestDeltaEpoch
		}
		fresh.Tables[tableName] = freshTable
	}
	fresh.Compaction.LastCompactedAt = now
	fresh.UpdatedAt = now

	// Journal before the manifest commit. If the manifest write fails, the
	// entries are harmless: purgeDueDeltas only deletes entries whose covering
	// base is published, so they simply wait for the next run. Writing the
	// journal first also prevents a storage leak — a failed journal write
	// after a successful manifest publish would leave merged deltas
	// unreferenced by every future run.
	journal.sortPending()
	if err := saveCompactionJournal(ctx, sink, journal); err != nil {
		return fmt.Errorf("save compaction journal: %w", err)
	}

	if err := sink.WriteManifest(ctx, fresh); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	purged, err := purgeDueDeltas(ctx, sink, journal, fresh, deltaRetention, now)
	if err != nil {
		return fmt.Errorf("purge expired deltas: %w", err)
	}
	if purged > 0 {
		fmt.Printf("Purged %d delta files past the handoff window\n", purged)
	}

	fmt.Println("Compaction complete")
	return nil
}

// selectCompactionScope returns the delta files this run is allowed to merge
// for one table: epochs strictly after baseEpoch and no later than the
// start-of-run manifest's LatestDeltaEpoch, in ascending epoch order.
//
// Any gap inside that pinned range is a hard error. Silently skipping a
// missing delta would publish a base with a data hole that no later delta
// could repair (later deltas assume the missing changes were already applied).
func selectCompactionScope(deltaFiles []string, baseEpoch, latestEpoch int64) ([]string, error) {
	inRange, err := epoch.FilterAfter(deltaFiles, baseEpoch)
	if err != nil {
		return nil, err
	}

	scoped := make([]string, 0, len(inRange))
	expected := baseEpoch + 1
	for _, path := range inRange {
		n, parseErr := epoch.ParseFromFilename(path)
		if parseErr != nil {
			continue // FilterAfter already skips non-epoch names; stay defensive.
		}
		if n > latestEpoch {
			// Landed after the start-of-run manifest — outside this run's scope.
			continue
		}
		if n != expected {
			return nil, fmt.Errorf("delta epoch %d confirmed by manifest is missing from sink (expected contiguous epoch %d after base %d)",
				n, expected, baseEpoch)
		}
		scoped = append(scoped, path)
		expected++
	}

	if expected-1 < latestEpoch {
		return nil, fmt.Errorf("delta epoch %d confirmed by manifest is missing from sink (highest contiguous: %d)",
			latestEpoch, expected-1)
	}
	return scoped, nil
}

// compactTable merges one table's pinned delta scope into a new base. It
// returns the handoff records for the deltas that must be retained past the
// handoff window. The manifest is not mutated here — publishing happens once,
// centrally, in Compact.
func compactTable(ctx context.Context, sink ports.Sink, tableName string, table manifest.Table, schema manifest.Schema, now time.Time) ([]DeltaHandoff, error) {
	// Step 1: Read current base into memory (keyed by PK)
	rows, err := readBase(ctx, sink, table)
	if err != nil {
		return nil, fmt.Errorf("read base: %w", err)
	}

	pkCols := table.PrimaryKey
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("no primary key for %s", tableName)
	}

	// Index by PK
	indexed := make(map[string]writer.Row, len(rows))
	for _, row := range rows {
		key := pkKey(row, pkCols)
		indexed[key] = row
	}

	// Step 2: List deltas and pin the scope to the start-of-run manifest.
	deltaPath := table.Path + "/deltas/"
	deltaFiles, err := sink.ListFiles(ctx, deltaPath)
	if err != nil {
		return nil, fmt.Errorf("list deltas: %w", err)
	}

	scopedDeltas, err := selectCompactionScope(deltaFiles, table.BaseEpoch, table.LatestDeltaEpoch)
	if err != nil {
		return nil, err
	}
	if len(scopedDeltas) == 0 {
		return nil, nil
	}

	var applied int
	for _, deltaFile := range scopedDeltas {
		deltaRows, err := readParquetRows(ctx, sink, deltaFile)
		if err != nil {
			return nil, fmt.Errorf("read delta %s: %w", deltaFile, err)
		}

		for _, dRow := range deltaRows {
			op, _ := dRow[btypes.ColOp].(string)
			key := pkKey(dRow, pkCols)

			switch op {
			case "I", "U":
				clean := make(writer.Row, len(dRow))
				for k, v := range dRow {
					if k == btypes.ColOp || k == btypes.ColEpoch {
						continue
					}
					clean[k] = v
				}
				indexed[key] = clean
			case "D":
				if existing, ok := indexed[key]; ok {
					existing[btypes.ColDeletedAt] = now.Format(time.RFC3339)
					indexed[key] = existing
				}
			}
			applied++
		}
	}

	// Step 3: Purge expired tombstones (default 30 days)
	tombstoneRetention := 30 * 24 * time.Hour
	var purged int
	for key, row := range indexed {
		if delAt, ok := row[btypes.ColDeletedAt]; ok && delAt != nil {
			if ts, ok := delAt.(string); ok {
				if t, err := time.Parse(time.RFC3339, ts); err == nil {
					if now.Sub(t) > tombstoneRetention {
						delete(indexed, key)
						purged++
					}
				}
			}
		}
	}

	// Step 4: Write new base. The new base epoch is the tip of the *pinned
	// scope*, not whatever the streamer may have landed since.
	newBaseEpoch := table.LatestDeltaEpoch
	newRows := make([]writer.Row, 0, len(indexed))
	for _, row := range indexed {
		newRows = append(newRows, row)
	}

	newBaseName := fmt.Sprintf("base_%06d.parquet", newBaseEpoch)
	remotePath := filepath.Join(table.Path, "base", newBaseName)
	tmpPath := filepath.Join("/tmp", "pgcdc-compact", remotePath)

	if err := writer.WriteBaseParquet(tmpPath, newRows, schema.Columns); err != nil {
		return nil, fmt.Errorf("write new base: %w", err)
	}

	if err := uploadFile(ctx, sink, remotePath, tmpPath); err != nil {
		return nil, fmt.Errorf("upload base: %w", err)
	}

	// Step 5: Enroll merged deltas in the handoff journal. Nothing is deleted
	// here — deletion waits for the retention window plus a published covering
	// base (see purgeDueDeltas).
	handoffs := make([]DeltaHandoff, 0, len(scopedDeltas))
	for _, deltaFile := range scopedDeltas {
		deltaEpoch, _ := epoch.ParseFromFilename(deltaFile)
		handoffs = append(handoffs, DeltaHandoff{
			Path:        deltaFile,
			Table:       tableName,
			Epoch:       deltaEpoch,
			BaseEpoch:   newBaseEpoch,
			CompactedAt: now,
		})
	}

	fmt.Printf("  %s: compacted %d deltas → base_%06d (%d rows, %d applied, %d purged); deltas retained for handoff\n",
		tableName, len(scopedDeltas), newBaseEpoch, len(indexed), applied, purged)
	return handoffs, nil
}

func readBase(ctx context.Context, sink ports.Sink, table manifest.Table) ([]writer.Row, error) {
	basePath := table.Path + "/base/"
	files, err := sink.ListFiles(ctx, basePath)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	return readParquetRows(ctx, sink, files[len(files)-1])
}

func readParquetRows(ctx context.Context, sink ports.Sink, path string) ([]writer.Row, error) {
	rc, err := sink.ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	// Download to temp for parquet-go (needs seekable file)
	tmpFile, err := os.CreateTemp("", "pgcdc-read-*.parquet")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	if _, err := tmpFile.ReadFrom(rc); err != nil {
		return nil, err
	}
	if err := tmpFile.Close(); err != nil {
		return nil, err
	}

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("open parquet %s: %w", path, err)
	}

	schema := pf.Schema()
	fields := schema.Fields()
	colNames := make([]string, len(fields))
	for i, field := range fields {
		colNames[i] = field.Name()
	}

	var rows []writer.Row
	for _, rg := range pf.RowGroups() {
		rgRows := rg.NumRows()
		pqRows := make([]goparquet.Row, rgRows)
		for i := range pqRows {
			pqRows[i] = make(goparquet.Row, len(fields))
		}

		rgReader := rg.Rows()
		n, _ := rgReader.ReadRows(pqRows)
		_ = rgReader.Close()

		for i := 0; i < n; i++ {
			r := make(writer.Row, len(colNames))
			for j, colName := range colNames {
				if j < len(pqRows[i]) {
					r[colName] = parquetValueToGo(pqRows[i][j])
				}
			}
			rows = append(rows, r)
		}
	}

	return rows, nil
}

func parquetValueToGo(v goparquet.Value) any {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case goparquet.Boolean:
		return v.Boolean()
	case goparquet.Int32:
		return int64(v.Int32())
	case goparquet.Int64:
		return v.Int64()
	case goparquet.Float:
		return float64(v.Float())
	case goparquet.Double:
		return v.Double()
	case goparquet.ByteArray:
		return string(v.ByteArray())
	default:
		return string(v.ByteArray())
	}
}

func pkKey(row writer.Row, pks []string) string {
	key := ""
	for _, pk := range pks {
		key += fmt.Sprintf("%v|", row[pk])
	}
	return key
}
