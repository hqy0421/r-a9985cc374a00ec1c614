package internal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal"
	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

const e2eTable = "public.items"

func e2eSchema() manifest.Schema {
	return manifest.Schema{
		Version: 1,
		Columns: []manifest.Column{
			{Name: "id", PgType: "int4", ParquetType: "INT32", Nullable: false},
			{Name: "label", PgType: "text", ParquetType: "UTF8", Nullable: true},
		},
	}
}

type e2eJournal struct {
	Version int `json:"version"`
	Pending []struct {
		Path string `json:"path"`
	} `json:"pending"`
}

// TestCompactEndToEnd_Filesystem drives the real filesystem adapter across
// multiple Compact invocations, proving the handoff journal persists on disk
// and deltas survive between separate process-like runs until the window
// elapses.
func TestCompactEndToEnd_Filesystem(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	sink, err := filesystem.New(root)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			e2eTable: {
				Status:           "active",
				PrimaryKey:       []string{"id"},
				BaseEpoch:        0,
				LatestDeltaEpoch: 2,
				Path:             e2eTable,
			},
		},
		Schemas: map[string]manifest.Schema{e2eTable: e2eSchema()},
	}
	if err := sink.WriteManifest(ctx, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	deltaPath := func(n int64) string {
		return filepath.Join(root, e2eTable, "deltas", epoch.FormatFilename(n))
	}
	for n, id := range map[int64]int64{1: 10, 2: 20} {
		tmp := filepath.Join(t.TempDir(), "d.parquet")
		rows := []writer.Row{{"__op": "I", "id": id, "label": "x"}}
		if err := writer.WriteDeltaParquet(tmp, rows, e2eSchema().Columns, n); err != nil {
			t.Fatalf("delta %d: %v", n, err)
		}
		data, err := os.ReadFile(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteFile(ctx, e2eTable+"/deltas/"+epoch.FormatFilename(n), bytes.NewReader(data)); err != nil {
			t.Fatalf("upload delta %d: %v", n, err)
		}
	}

	compactedAt := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	restore := internal.SetCompactClockForTest(func() time.Time { return compactedAt })
	defer restore()

	// Run 1: compaction publishes base_2 and journals deltas 1-2.
	if err := internal.Compact(ctx, sink, 24*time.Hour); err != nil {
		t.Fatalf("compact run 1: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, e2eTable, "base", "base_000002.parquet")); err != nil {
		t.Fatalf("new base missing: %v", err)
	}
	for _, n := range []int64{1, 2} {
		if _, err := os.Stat(deltaPath(n)); err != nil {
			t.Fatalf("delta %d removed before handoff window: %v", n, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "compaction-journal.json")); err != nil {
		t.Fatalf("journal not persisted on disk: %v", err)
	}

	// Run 2 with a brand-new sink instance: journal state must survive via the
	// sink alone (no in-memory state carried between runs).
	sink2, err := filesystem.New(root)
	if err != nil {
		t.Fatalf("sink2: %v", err)
	}
	restore2 := internal.SetCompactClockForTest(func() time.Time {
		return compactedAt.Add(24*time.Hour + time.Minute)
	})
	if err := internal.Compact(ctx, sink2, 24*time.Hour); err != nil {
		t.Fatalf("compact run 2: %v", err)
	}
	restore2()

	for _, n := range []int64{1, 2} {
		if _, err := os.Stat(deltaPath(n)); err == nil {
			t.Fatalf("delta %d should have been purged after window on run 2", n)
		}
	}

	// Journal should now be empty and base still present.
	journalData, err := os.ReadFile(filepath.Join(root, "compaction-journal.json"))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	var j e2eJournal
	if err := json.Unmarshal(journalData, &j); err != nil {
		t.Fatalf("parse journal: %v", err)
	}
	if len(j.Pending) != 0 {
		t.Fatalf("journal not emptied after purge: %s", journalData)
	}
	if _, err := os.Stat(filepath.Join(root, e2eTable, "base", "base_000002.parquet")); err != nil {
		t.Fatalf("base removed during purge: %v", err)
	}
}
