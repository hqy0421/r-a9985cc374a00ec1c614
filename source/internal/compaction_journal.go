package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// compactionJournalPath is the sink-relative location of the durable
// compaction handoff journal. It lives next to manifest.json so the journal
// survives process restarts and works identically on filesystem, S3, and GCS
// sinks (only ports.Sink primitives are used).
const compactionJournalPath = "compaction-journal.json"

// DeltaHandoff records one delta file that has been folded into a published
// base but must remain readable until the handoff window elapses.
type DeltaHandoff struct {
	// Path is the sink-relative delta file path.
	Path string `json:"path"`
	// Table is the manifest table name the delta belongs to.
	Table string `json:"table"`
	// Epoch is the delta's epoch number, parsed from its filename.
	Epoch int64 `json:"epoch"`
	// BaseEpoch is the base epoch that now contains this delta's changes.
	BaseEpoch int64 `json:"base_epoch"`
	// CompactedAt is when the delta was folded into the base. The handoff
	// window is measured from this timestamp.
	CompactedAt time.Time `json:"compacted_at"`
}

// CompactionJournal is the durable record of delta files awaiting deletion.
type CompactionJournal struct {
	Version int            `json:"version"`
	Pending []DeltaHandoff `json:"pending"`
}

// loadCompactionJournal reads the journal from the sink. A missing journal
// (first compaction, or a sink where the file has never been written) is
// treated as an empty journal, not an error.
func loadCompactionJournal(ctx context.Context, sink ports.Sink) (*CompactionJournal, error) {
	rc, err := sink.ReadFile(ctx, compactionJournalPath)
	if err != nil {
		// Every shipped adapter surfaces a not-found read as an error; there
		// is no shared sentinel. An empty journal is the safe default either
		// way, and any genuine read problem simply means deletion is deferred
		// to a later run — never unsafe.
		return &CompactionJournal{Version: 1}, nil
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read compaction journal: %w", err)
	}

	var j CompactionJournal
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse compaction journal: %w", err)
	}
	if j.Version < 1 {
		j.Version = 1
	}
	return &j, nil
}

// saveCompactionJournal serialises and writes the journal to the sink.
func saveCompactionJournal(ctx context.Context, sink ports.Sink, j *CompactionJournal) error {
	j.Version = 1
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal compaction journal: %w", err)
	}
	if err := sink.WriteFile(ctx, compactionJournalPath, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write compaction journal: %w", err)
	}
	return nil
}

// add records the given handoffs, de-duplicating by delta path. Re-enrolling
// an existing entry preserves its original CompactedAt so a crash-retry cannot
// reset the handoff clock.
func (j *CompactionJournal) add(handoffs ...DeltaHandoff) {
	existing := make(map[string]int, len(j.Pending))
	for i, h := range j.Pending {
		existing[h.Path] = i
	}
	for _, h := range handoffs {
		if idx, ok := existing[h.Path]; ok {
			// Keep the earliest compaction timestamp; the base coverage can
			// only move forward, so refresh it.
			if h.CompactedAt.Before(j.Pending[idx].CompactedAt) {
				h.CompactedAt = j.Pending[idx].CompactedAt
			}
			j.Pending[idx] = h
			continue
		}
		j.Pending = append(j.Pending, h)
	}
}

// remove drops entries for the given delta paths.
func (j *CompactionJournal) remove(paths map[string]struct{}) {
	if len(paths) == 0 {
		return
	}
	kept := j.Pending[:0]
	for _, h := range j.Pending {
		if _, drop := paths[h.Path]; drop {
			continue
		}
		kept = append(kept, h)
	}
	j.Pending = kept
}

// purgeDueDeltas deletes pending deltas that are both:
//
//  1. older than the handoff retention window, and
//  2. already covered by a base published in the given (freshly re-read)
//     manifest — a journal entry written by a run that crashed before its
//     manifest commit must never be deleted, because readers still need the
//     delta.
//
// The journal is rewritten after the purge. A failed delete leaves both the
// file and the journal entry in place for the next run.
func purgeDueDeltas(ctx context.Context, sink ports.Sink, journal *CompactionJournal, fresh *manifest.Manifest, retention time.Duration, now time.Time) (int, error) {
	if len(journal.Pending) == 0 {
		return 0, nil
	}

	deleted := make(map[string]struct{})
	var deletedCount int
	for _, h := range journal.Pending {
		if now.Sub(h.CompactedAt) < retention {
			continue
		}
		table, ok := fresh.Tables[h.Table]
		if !ok {
			// Table vanished from the manifest (e.g. torn down). Nothing can
			// read through it anymore; leave the entry alone rather than
			// deleting data out from under an unknown consumer layout.
			continue
		}
		if table.BaseEpoch < h.BaseEpoch {
			// Covering base was never published; keep the delta.
			continue
		}

		if err := sink.DeleteFile(ctx, h.Path); err != nil {
			// Keep the journal entry and stop: partial purge retries cleanly.
			break
		}
		deleted[h.Path] = struct{}{}
		deletedCount++
	}

	if len(deleted) > 0 {
		journal.remove(deleted)
		if err := saveCompactionJournal(ctx, sink, journal); err != nil {
			return deletedCount, err
		}
	}
	return deletedCount, nil
}

// sortPending orders journal entries by table and epoch for deterministic
// journal contents and easier diffing.
func (j *CompactionJournal) sortPending() {
	sort.Slice(j.Pending, func(i, k int) bool {
		if j.Pending[i].Table != j.Pending[k].Table {
			return j.Pending[i].Table < j.Pending[k].Table
		}
		return j.Pending[i].Epoch < j.Pending[k].Epoch
	})
}
