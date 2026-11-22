package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// memSink is an in-memory ports.Sink for compaction tests. It stores blobs by
// path so tests can simulate the streamer landing files while a compaction run
// is in progress.
type memSink struct {
	mu        sync.Mutex
	files     map[string][]byte
	manifest  *manifest.Manifest
	deleteErr map[string]error // path -> injected delete failure
}

func newMemSink() *memSink {
	return &memSink{
		files:     make(map[string][]byte),
		deleteErr: make(map[string]error),
	}
}

func (s *memSink) WriteFile(_ context.Context, remotePath string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[remotePath] = data
	return nil
}

func (s *memSink) ReadFile(_ context.Context, path string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[path]
	if !ok {
		return nil, &memNotFound{path: path}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *memSink) ListFiles(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for p := range s.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *memSink) DeleteFile(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.deleteErr[path]; ok {
		return err
	}
	delete(s.files, path)
	return nil
}

func (s *memSink) ReadManifest(_ context.Context) (*manifest.Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manifest == nil {
		return nil, &memNotFound{path: "manifest.json"}
	}
	// Return a deep-ish copy so callers mutating the manifest don't change
	// stored state until WriteManifest is called.
	return cloneManifest(s.manifest), nil
}

func (s *memSink) WriteManifest(_ context.Context, m *manifest.Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifest = cloneManifest(m)
	return nil
}

func (s *memSink) Backend() string { return "mem" }

func (s *memSink) hasFile(path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.files[path]
	return ok
}

func (s *memSink) filePaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.files))
	for p := range s.files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

type memNotFound struct{ path string }

func (e *memNotFound) Error() string { return "not found: " + e.path }

// manifestSnapshot returns a copy of the stored manifest without going through
// ReadManifest (used to simulate another process's writes).
func (s *memSink) manifestSnapshot() *manifest.Manifest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneManifest(s.manifest)
}

// hookedSink wraps a Sink and runs hooks before manifest reads/writes,
// letting tests simulate a concurrent writer at the publish boundary.
type hookedSink struct {
	ports.Sink
	beforeReadManifest  func()
	beforeWriteManifest func()
	// failManifestWrites makes the first N WriteManifest calls return errManifestInjected.
	failManifestWrites int
}

var errManifestInjected = fmt.Errorf("injected manifest write failure")

func (h *hookedSink) ReadManifest(ctx context.Context) (*manifest.Manifest, error) {
	if h.beforeReadManifest != nil {
		h.beforeReadManifest()
	}
	return h.Sink.ReadManifest(ctx)
}

func (h *hookedSink) WriteManifest(ctx context.Context, m *manifest.Manifest) error {
	if h.beforeWriteManifest != nil {
		h.beforeWriteManifest()
	}
	if h.failManifestWrites > 0 {
		h.failManifestWrites--
		return errManifestInjected
	}
	return h.Sink.WriteManifest(ctx, m)
}

func cloneManifest(m *manifest.Manifest) *manifest.Manifest {
	cp := *m
	cp.Tables = make(map[string]manifest.Table, len(m.Tables))
	for k, v := range m.Tables {
		cp.Tables[k] = v
	}
	cp.Schemas = make(map[string]manifest.Schema, len(m.Schemas))
	for k, v := range m.Schemas {
		cp.Schemas[k] = v
	}
	return &cp
}

// --- test fixtures -------------------------------------------------------

const testTable = "public.orders"

func testSchema() manifest.Schema {
	return manifest.Schema{
		Version: 1,
		Columns: []manifest.Column{
			{Name: "id", PgType: "int4", ParquetType: "INT32", Nullable: false},
			{Name: "name", PgType: "text", ParquetType: "UTF8", Nullable: true},
		},
	}
}

func testManifest(baseEpoch, latestEpoch int64) *manifest.Manifest {
	return &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			testTable: {
				Status:           "active",
				PrimaryKey:       []string{"id"},
				BaseEpoch:        baseEpoch,
				LatestDeltaEpoch: latestEpoch,
				Path:             testTable,
			},
		},
		Schemas: map[string]manifest.Schema{
			testTable: testSchema(),
		},
	}
}

// putDelta writes a delta parquet for the given epoch into the sink.
func putDelta(t *testing.T, sink *memSink, epochNum int64, rows []writer.Row) {
	t.Helper()
	tmp := t.TempDir() + "/delta.parquet"
	if err := writer.WriteDeltaParquet(tmp, rows, testSchema().Columns, epochNum); err != nil {
		t.Fatalf("write delta %d: %v", epochNum, err)
	}
	path := testTable + "/deltas/" + epoch.FormatFilename(epochNum)
	data := bytes.NewReader(mustReadFile(t, tmp))
	if err := sink.WriteFile(context.Background(), path, data); err != nil {
		t.Fatalf("put delta %d: %v", epochNum, err)
	}
}

func deltaRow(op string, id int64, name string) writer.Row {
	return writer.Row{"__op": op, "id": id, "name": name}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
