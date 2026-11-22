package internal

import (
	"context"
	"testing"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// pinClock fixes the compaction clock and restores it on cleanup.
func pinClock(t *testing.T, at time.Time) {
	t.Helper()
	original := compactClock
	compactClock = func() time.Time { return at }
	t.Cleanup(func() { compactClock = original })
}

// TestCompact_PinsScopeToStartManifest verifies requirement 1: a delta the
// streamer lands after compaction reads its manifest is neither merged into the
// new base nor deleted.
func TestCompact_PinsScopeToStartManifest(t *testing.T) {
	ctx := context.Background()
	compactedAt := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	pinClock(t, compactedAt)

	sink := newMemSink()
	sink.manifest = testManifest(0, 2)
	putDelta(t, sink, 1, []writer.Row{deltaRow("I", 1, "a")})
	putDelta(t, sink, 2, []writer.Row{deltaRow("I", 2, "b")})

	// Simulate the running streamer landing epoch 3 *after* compaction has
	// pinned its scope but before it finishes. We inject the file before
	// calling Compact; the manifest still says latest=2, which is exactly the
	// state the live streamer would leave until its next manifest commit.
	putDelta(t, sink, 3, []writer.Row{deltaRow("I", 3, "c")})

	if err := Compact(ctx, sink, time.Hour); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// The new base must be epoch 2 (the pinned tip), not 3.
	basePath := testTable + "/base/base_000002.parquet"
	if !sink.hasFile(basePath) {
		t.Fatalf("expected base at %s, files: %v", basePath, sink.filePaths())
	}
	baseRows, err := readParquetRows(ctx, sink, basePath)
	if err != nil {
		t.Fatalf("read new base: %v", err)
	}
	if len(baseRows) != 2 {
		t.Fatalf("base should contain 2 rows from pinned scope, got %d: %v", len(baseRows), baseRows)
	}
	for _, r := range baseRows {
		if id, _ := r["id"].(int64); id == 3 {
			t.Fatalf("epoch 3 row must not be in base; compaction scope leaked past start manifest: %v", baseRows)
		}
	}

	// Epoch 3 delta must remain untouched and readable for later batches.
	epoch3 := testTable + "/deltas/" + epoch.FormatFilename(3)
	if !sink.hasFile(epoch3) {
		t.Fatalf("streamer's post-snapshot delta %s was deleted", epoch3)
	}

	// Epochs 1-2 are within the handoff window, so they remain too.
	for _, n := range []int64{1, 2} {
		if !sink.hasFile(testTable + "/deltas/" + epoch.FormatFilename(n)) {
			t.Fatalf("delta %d deleted before handoff window elapsed", n)
		}
	}

	// The published manifest keeps base at 2 while latest must still be able to
	// advance past it; here the streamer hadn't re-committed, so latest stays 2.
	got := sink.manifest.Tables[testTable]
	if got.BaseEpoch != 2 {
		t.Fatalf("BaseEpoch = %d, want 2", got.BaseEpoch)
	}
	if got.LatestDeltaEpoch != 2 {
		t.Fatalf("LatestDeltaEpoch = %d, want 2", got.LatestDeltaEpoch)
	}
}

// TestCompact_PreservesConcurrentStreamerBatches verifies requirement 2: when
// the streamer commits new batches (with manifest updates) while compaction
// runs, publishing the new base must not roll LatestDeltaEpoch back, and the
// newer deltas stay readable on top of the new base.
func TestCompact_PreservesConcurrentStreamerBatches(t *testing.T) {
	ctx := context.Background()
	pinClock(t, time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC))

	sink := newMemSink()
	sink.manifest = testManifest(0, 2)
	putDelta(t, sink, 1, []writer.Row{deltaRow("I", 1, "a")})
	putDelta(t, sink, 2, []writer.Row{deltaRow("I", 2, "b")})
	putDelta(t, sink, 3, []writer.Row{deltaRow("I", 3, "c")})
	putDelta(t, sink, 4, []writer.Row{deltaRow("U", 1, "a2")})

	// Emulate a concurrent streamer manifest commit landing between
	// compaction's start-of-run read and its pre-publish re-read. On the
	// second ReadManifest call, another process first commits epochs 3-4
	// (files already present, manifest advanced to latest=4); the re-read then
	// returns that fresh state for compaction to merge onto.
	reads := 0
	hooked := &hookedSink{
		Sink: sink,
		beforeReadManifest: func() {
			reads++
			if reads != 2 {
				return
			}
			fresh := sink.manifestSnapshot()
			tbl := fresh.Tables[testTable]
			tbl.LatestDeltaEpoch = 4
			fresh.Tables[testTable] = tbl
			if err := sink.WriteManifest(ctx, fresh); err != nil {
				t.Fatalf("simulated streamer commit: %v", err)
			}
		},
	}

	if err := Compact(ctx, hooked, time.Hour); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	got := sink.manifest.Tables[testTable]
	if got.BaseEpoch != 2 {
		t.Fatalf("BaseEpoch = %d, want 2 (pinned scope tip)", got.BaseEpoch)
	}
	if got.LatestDeltaEpoch != 4 {
		t.Fatalf("LatestDeltaEpoch = %d, want 4 — concurrent streamer batch was lost", got.LatestDeltaEpoch)
	}

	// Deltas 3-4 remain so a reader on the new base (epoch 2) can still catch up.
	for _, n := range []int64{3, 4} {
		if !sink.hasFile(testTable + "/deltas/" + epoch.FormatFilename(n)) {
			t.Fatalf("post-compaction delta %d must stay readable", n)
		}
	}
}

// TestCompact_RetainsDeltasThroughoutHandoffWindow verifies requirement 3:
// compacted deltas are deleted only after the retention window elapses, so a
// consumer holding an older manifest never references a removed file.
func TestCompact_RetainsDeltasThroughoutHandoffWindow(t *testing.T) {
	ctx := context.Background()
	compactedAt := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	pinClock(t, compactedAt)

	sink := newMemSink()
	sink.manifest = testManifest(0, 2)
	putDelta(t, sink, 1, []writer.Row{deltaRow("I", 1, "a")})
	putDelta(t, sink, 2, []writer.Row{deltaRow("I", 2, "b")})

	if err := Compact(ctx, sink, 7*24*time.Hour); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	for _, n := range []int64{1, 2} {
		if !sink.hasFile(testTable + "/deltas/" + epoch.FormatFilename(n)) {
			t.Fatalf("delta %d deleted immediately; old-manifest consumers would break", n)
		}
	}
	journal, err := loadCompactionJournal(ctx, sink)
	if err != nil {
		t.Fatalf("load journal: %v", err)
	}
	if len(journal.Pending) != 2 {
		t.Fatalf("journal pending = %d, want 2", len(journal.Pending))
	}

	// Run again just inside the window: nothing may be purged.
	pinClock(t, compactedAt.Add(7*24*time.Hour-time.Second))
	if err := Compact(ctx, sink, 7*24*time.Hour); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	for _, n := range []int64{1, 2} {
		if !sink.hasFile(testTable + "/deltas/" + epoch.FormatFilename(n)) {
			t.Fatalf("delta %d purged before retention elapsed", n)
		}
	}

	// Run again after the window: both deltas purge, base and journal stay valid.
	pinClock(t, compactedAt.Add(7*24*time.Hour+time.Second))
	if err := Compact(ctx, sink, 7*24*time.Hour); err != nil {
		t.Fatalf("third Compact: %v", err)
	}
	for _, n := range []int64{1, 2} {
		if sink.hasFile(testTable + "/deltas/" + epoch.FormatFilename(n)) {
			t.Fatalf("delta %d should be purged after handoff window", n)
		}
	}
	journal, _ = loadCompactionJournal(ctx, sink)
	if len(journal.Pending) != 0 {
		t.Fatalf("journal should be empty after purge, got %d entries", len(journal.Pending))
	}
	if !sink.hasFile(testTable + "/base/base_000002.parquet") {
		t.Fatal("base file must survive delta purge")
	}
}

// TestCompact_FailsWhenManifestConfirmedDeltaIsMissing guards against silently
// publishing a base with an epoch gap. A manifest-confirmed delta absent from
// the sink must stop the run rather than be skipped.
func TestCompact_FailsWhenManifestConfirmedDeltaIsMissing(t *testing.T) {
	ctx := context.Background()
	pinClock(t, time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC))

	sink := newMemSink()
	sink.manifest = testManifest(0, 3)
	putDelta(t, sink, 1, []writer.Row{deltaRow("I", 1, "a")})
	// Epoch 2 missing.
	putDelta(t, sink, 3, []writer.Row{deltaRow("I", 3, "c")})

	err := Compact(ctx, sink, time.Hour)
	if err == nil {
		t.Fatal("Compact must fail when a manifest-confirmed delta is missing")
	}

	// No base or manifest mutation on failure.
	if sink.hasFile(testTable + "/base/base_000003.parquet") {
		t.Fatal("base must not be published when the scope is incomplete")
	}
	if sink.manifest.Tables[testTable].BaseEpoch != 0 {
		t.Fatal("manifest BaseEpoch mutated on failed compaction")
	}
	if sink.hasFile(compactionJournalPath) {
		t.Fatal("journal must not be written when no base was published")
	}
}

// TestCompact_JournalEntryBeforeManifestCommitIsNotPurged models a crash
// between journaling and manifest publish: the entry's covering base is not
// published, so even once the retention window elapses the delta must remain.
func TestCompact_JournalEntryBeforeManifestCommitIsNotPurged(t *testing.T) {
	ctx := context.Background()
	compactedAt := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)

	sink := newMemSink()
	// Manifest still points at base 0 — the base-2 commit never happened.
	sink.manifest = testManifest(0, 2)
	putDelta(t, sink, 1, []writer.Row{deltaRow("I", 1, "a")})
	putDelta(t, sink, 2, []writer.Row{deltaRow("I", 2, "b")})

	journal := &CompactionJournal{
		Version: 1,
		Pending: []DeltaHandoff{
			{Path: testTable + "/deltas/" + epoch.FormatFilename(1), Table: testTable, Epoch: 1, BaseEpoch: 2, CompactedAt: compactedAt},
			{Path: testTable + "/deltas/" + epoch.FormatFilename(2), Table: testTable, Epoch: 2, BaseEpoch: 2, CompactedAt: compactedAt},
		},
	}
	if err := saveCompactionJournal(ctx, sink, journal); err != nil {
		t.Fatalf("seed journal: %v", err)
	}

	// Long after the window, the unpublish-base guard keeps the deltas.
	later := compactedAt.Add(30 * 24 * time.Hour)
	loaded, err := loadCompactionJournal(ctx, sink)
	if err != nil {
		t.Fatalf("load journal: %v", err)
	}
	fresh, _ := sink.ReadManifest(ctx)
	purged, err := purgeDueDeltas(ctx, sink, loaded, fresh, time.Hour, later)
	if err != nil {
		t.Fatalf("purgeDueDeltas: %v", err)
	}
	if purged != 0 {
		t.Fatalf("purged %d entries whose base was never published", purged)
	}
	for _, n := range []int64{1, 2} {
		if !sink.hasFile(testTable + "/deltas/" + epoch.FormatFilename(n)) {
			t.Fatalf("delta %d deleted without a published covering base", n)
		}
	}
}

// TestCompact_RetryAfterManifestWriteFailure models a crash between journal
// commit and manifest publish: the retry must produce exactly one base, one
// journal set, and no lost or duplicated delta coverage.
func TestCompact_RetryAfterManifestWriteFailure(t *testing.T) {
	ctx := context.Background()
	pinClock(t, time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC))

	sink := newMemSink()
	sink.manifest = testManifest(0, 2)
	putDelta(t, sink, 1, []writer.Row{deltaRow("I", 1, "a")})
	putDelta(t, sink, 2, []writer.Row{deltaRow("I", 2, "b")})

	// First run: journal lands, manifest write fails.
	failing := &hookedSink{Sink: sink, failManifestWrites: 1}
	if err := Compact(ctx, failing, time.Hour); err == nil {
		t.Fatal("expected injected manifest failure to surface")
	}

	// Base was uploaded and journal written, but the manifest still says base 0.
	if got := sink.manifest.Tables[testTable].BaseEpoch; got != 0 {
		t.Fatalf("BaseEpoch = %d, want 0 after failed publish", got)
	}
	journal, err := loadCompactionJournal(ctx, sink)
	if err != nil {
		t.Fatalf("journal should exist after journal-first commit: %v", err)
	}
	if len(journal.Pending) != 2 {
		t.Fatalf("journal pending = %d, want 2", len(journal.Pending))
	}

	// Second run: redoes the same pinned scope, then publishes successfully.
	if err := Compact(ctx, sink, time.Hour); err != nil {
		t.Fatalf("retry Compact: %v", err)
	}
	if got := sink.manifest.Tables[testTable].BaseEpoch; got != 2 {
		t.Fatalf("BaseEpoch after retry = %d, want 2", got)
	}

	// Journal must hold exactly the two original entries — add() de-duplicates
	// the re-enrolled paths rather than growing duplicates.
	journal, _ = loadCompactionJournal(ctx, sink)
	if len(journal.Pending) != 2 {
		t.Fatalf("journal pending after retry = %d, want 2 (no duplicates)", len(journal.Pending))
	}
	for _, n := range []int64{1, 2} {
		if !sink.hasFile(testTable + "/deltas/" + epoch.FormatFilename(n)) {
			t.Fatalf("delta %d must remain within handoff window", n)
		}
	}
}

// TestSelectCompactionScope covers the scope-pinning helper directly.
func TestSelectCompactionScope(t *testing.T) {
	base := testTable + "/deltas/"
	files := []string{
		base + epoch.FormatFilename(1),
		base + epoch.FormatFilename(2),
		base + epoch.FormatFilename(3),
		base + epoch.FormatFilename(4),
		base + "readme.txt",
	}

	got, err := selectCompactionScope(files, 1, 3)
	if err != nil {
		t.Fatalf("selectCompactionScope: %v", err)
	}
	want := []string{base + epoch.FormatFilename(2), base + epoch.FormatFilename(3)}
	if len(got) != len(want) {
		t.Fatalf("scope = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scope = %v, want %v", got, want)
		}
	}

	// Missing tip inside the pinned range is an error.
	if _, err := selectCompactionScope(files, 1, 6); err == nil {
		t.Fatal("expected error for missing manifest-confirmed epoch 6")
	}
}

// TestResolveDeltaRetention checks config parsing and fallbacks.
func TestResolveDeltaRetention(t *testing.T) {
	if d := ResolveDeltaRetention(""); d != defaultDeltaRetention {
		t.Fatalf("empty = %v, want default %v", d, defaultDeltaRetention)
	}
	if d := ResolveDeltaRetention("48h"); d != 48*time.Hour {
		t.Fatalf("48h = %v, want 48h", d)
	}
	if d := ResolveDeltaRetention("not-a-duration"); d != defaultDeltaRetention {
		t.Fatalf("invalid = %v, want default", d)
	}
	if d := ResolveDeltaRetention("-5h"); d != defaultDeltaRetention {
		t.Fatalf("negative = %v, want default", d)
	}
}

// Ensure the manifest's tombstone column constant still exists in the types
// package; keeps the delete-marking path honest if upstream renames it.
func TestTombstoneColumnConstant(t *testing.T) {
	if btypes.ColDeletedAt != "__deleted_at" {
		t.Fatalf("ColDeletedAt = %q, want __deleted_at", btypes.ColDeletedAt)
	}
}
