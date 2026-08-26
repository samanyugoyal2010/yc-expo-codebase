// Copyright (c) 2026 Samanyu Goyal. All Rights Reserved.
//
// PROPRIETARY AND CONFIDENTIAL
//
// This source code and all related documentation are the exclusive intellectual
// property of Samanyu Goyal. No part of this software may be used, copied,
// reproduced, modified, disclosed, or distributed in any form or by any means
// without the prior explicit written permission of Samanyu Goyal.
//
// Any unauthorized use or reproduction of this software, in whole or in part,
// constitutes a violation of copyright law and may result in civil and criminal
// penalties. All rights reserved worldwide.

package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// segSizeThreshold is the soft size limit per segment file.
// When the active segment exceeds this, a new segment is started on the next Append.
const segSizeThreshold = 64 << 20 // 64 MiB

// WAL is an append-only, CRC-protected write-ahead log with group commit.
//
// File layout:
//
//	{dir}/
//	    0000000000000000001.wal   ← oldest segment (firstLSN = 1)
//	    0000000000000004097.wal   ← newer segment  (firstLSN = 4097)
//	    0000000000000009001.wal   ← active segment (highest firstLSN)
//
// Each segment is a sequence of encoded Records (see record.go).
// Segment filenames encode the first LSN so they sort into replay order.
//
// Group commit:
//   A single committer goroutine drains the pending channel, writes all
//   buffered data in one call, fsyncs once, advances durableLSN, then
//   signals every producer that was waiting in that batch. This converts
//   N concurrent fsync() calls (each 0.1–10 ms) into one.
type WAL struct {
	dir string

	// appendMu serialises LSN assignment and channel sends so that records
	// always arrive at the committer in LSN order.
	appendMu sync.Mutex
	lsn      uint64 // next LSN to assign (protected by appendMu)

	durableLSN atomic.Uint64 // highest LSN flushed to disk

	// active segment
	segMu    sync.Mutex // protects seg* fields
	segFile  *os.File
	segSize  int64   // bytes written to current segment
	segFirst uint64  // first LSN in current segment

	// segment manifest (firstLSN of each closed segment, ascending)
	segsMu   sync.Mutex
	segs     []uint64 // firstLSNs of closed segments

	pendingCh chan pendingWrite
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

type pendingWrite struct {
	data []byte
	lsn  uint64
	done chan<- error
}

// Open opens (or creates) a WAL in dir.  It replays the last segment to
// establish the current LSN (so future Appends continue monotonically).
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:       dir,
		pendingCh: make(chan pendingWrite, 512),
		stopCh:    make(chan struct{}),
	}

	if err := w.loadSegments(); err != nil {
		return nil, err
	}

	w.wg.Add(1)
	go w.committer()
	return w, nil
}

// loadSegments discovers existing segment files, determines the current LSN,
// and opens the active segment for writing.
func (w *WAL) loadSegments() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	var firstLSNs []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		var fl uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.wal", &fl); err != nil {
			continue
		}
		firstLSNs = append(firstLSNs, fl)
	}
	sort.Slice(firstLSNs, func(i, j int) bool { return firstLSNs[i] < firstLSNs[j] })

	if len(firstLSNs) == 0 {
		// Brand new WAL — create the first segment starting at LSN 1.
		return w.openNewSegment(1)
	}

	// All but the last are closed segments.
	w.segs = firstLSNs[:len(firstLSNs)-1]
	activeFirst := firstLSNs[len(firstLSNs)-1]

	// Find the highest LSN in the active segment by replaying it.
	activePath := w.segPath(activeFirst)
	maxLSN, fileSize, err := scanSegmentMaxLSN(activePath)
	if err != nil {
		return fmt.Errorf("wal: scan active segment: %w", err)
	}

	// Open the active segment for appending.
	f, err := os.OpenFile(activePath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.segFile = f
	w.segSize = fileSize
	w.segFirst = activeFirst

	nextLSN := maxLSN + 1
	if maxLSN == 0 {
		nextLSN = activeFirst // empty segment
	}
	w.lsn = nextLSN - 1 // Append will pre-increment
	w.durableLSN.Store(maxLSN)
	return nil
}

// openNewSegment creates a new segment file with firstLSN = first.
func (w *WAL) openNewSegment(first uint64) error {
	path := w.segPath(first)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.segFile = f
	w.segSize = 0
	w.segFirst = first
	return nil
}

// Close stops the committer goroutine and closes the active segment.
func (w *WAL) Close() error {
	close(w.stopCh)
	w.wg.Wait()
	w.segMu.Lock()
	defer w.segMu.Unlock()
	if w.segFile != nil {
		w.segFile.Sync()
		return w.segFile.Close()
	}
	return nil
}

// Append encodes rec, appends it to the log, and waits for the group commit
// fsync before returning the assigned LSN.
func (w *WAL) Append(rec Record) (uint64, error) {
	return w.AppendBatch([]Record{rec})
}

// AppendBatch encodes all records in recs and commits them with a single fsync.
// Returns the LSN of the last record in the batch.
func (w *WAL) AppendBatch(recs []Record) (uint64, error) {
	if len(recs) == 0 {
		return w.DurableLSN(), nil
	}

	done := make(chan error, 1)

	w.appendMu.Lock()
	var buf []byte
	var lastLSN uint64
	for _, rec := range recs {
		w.lsn++
		lsn := w.lsn
		encoded, err := Encode(lsn, rec)
		if err != nil {
			w.appendMu.Unlock()
			return 0, err
		}
		buf = append(buf, encoded...)
		lastLSN = lsn
	}
	w.pendingCh <- pendingWrite{data: buf, lsn: lastLSN, done: done}
	w.appendMu.Unlock()

	if err := <-done; err != nil {
		return 0, err
	}
	return lastLSN, nil
}

// DurableLSN returns the highest LSN that has been fsynced to disk.
// The page store uses this to gate slot reuse (§8 of the design).
func (w *WAL) DurableLSN() uint64 {
	return w.durableLSN.Load()
}

// Size returns the total bytes written across all WAL segments (active + closed).
// Used by the queue ticker to trigger checkpoints when the WAL grows large.
func (w *WAL) Size() int64 {
	w.segsMu.Lock()
	closed := int64(len(w.segs)) * segSizeThreshold // approximate for closed segs
	w.segsMu.Unlock()
	w.segMu.Lock()
	active := w.segSize
	w.segMu.Unlock()
	return closed + active
}

// Sync forces an fsync of the active segment without appending any record.
func (w *WAL) Sync() error {
	w.segMu.Lock()
	f := w.segFile
	w.segMu.Unlock()
	if f == nil {
		return nil
	}
	return f.Sync()
}

// Replay calls fn for every record whose LSN >= from, in order.
// A CRC mismatch or short read at the tail of the active segment is treated as
// a torn write and terminates replay cleanly (the tail is not returned).
// A CRC mismatch mid-log is a hard failure.
func (w *WAL) Replay(from uint64, fn func(lsn uint64, rec Record) error) error {
	// Collect all segment firstLSNs in order.
	w.segsMu.Lock()
	segs := append([]uint64(nil), w.segs...)
	w.segsMu.Unlock()
	w.segMu.Lock()
	segs = append(segs, w.segFirst)
	w.segMu.Unlock()

	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })

	// Find the first segment that could contain records >= from.
	startIdx := 0
	for i, fl := range segs {
		if fl <= from {
			startIdx = i
		}
	}

	for _, fl := range segs[startIdx:] {
		path := w.segPath(fl)
		isActive := fl == w.segFirst
		if err := replaySegment(path, from, isActive, fn); err != nil {
			return err
		}
	}
	return nil
}

// TruncatePrefix deletes all segment files whose records are fully covered
// by the checkpoint at upTo (i.e. all records have LSN <= upTo).
// Only closed segments are removed; the active segment is never deleted.
func (w *WAL) TruncatePrefix(upTo uint64) error {
	w.segsMu.Lock()
	defer w.segsMu.Unlock()

	var keep []uint64
	for i, fl := range w.segs {
		// A closed segment covers [fl, nextFL-1] where nextFL is the first LSN
		// of the following segment (or the active segment's firstLSN).
		var nextFL uint64
		if i+1 < len(w.segs) {
			nextFL = w.segs[i+1]
		} else {
			w.segMu.Lock()
			nextFL = w.segFirst
			w.segMu.Unlock()
		}
		maxInSeg := nextFL - 1
		if maxInSeg <= upTo {
			// Entire segment is superseded — delete it.
			if err := os.Remove(w.segPath(fl)); err != nil && !os.IsNotExist(err) {
				return err
			}
		} else {
			keep = append(keep, fl)
		}
	}
	w.segs = keep
	return nil
}

// committer is the single goroutine that batches writes and issues one fsync
// per group, then notifies all waiting producers.
func (w *WAL) committer() {
	defer w.wg.Done()

	for {
		var batch []pendingWrite
		var buf []byte

		// Wait for the first pending write (or stop signal).
		select {
		case pw := <-w.pendingCh:
			batch = append(batch, pw)
			buf = append(buf, pw.data...)
		case <-w.stopCh:
			return
		}

		// Drain any additional writes that arrived concurrently (non-blocking).
	drain:
		for {
			select {
			case pw := <-w.pendingCh:
				batch = append(batch, pw)
				buf = append(buf, pw.data...)
			default:
				break drain
			}
		}

		maxLSN := batch[len(batch)-1].lsn

		// Rotate the segment if needed before writing.
		err := w.maybeRotate(maxLSN)

		// Write + fsync.
		if err == nil {
			w.segMu.Lock()
			_, err = w.segFile.Write(buf)
			if err == nil {
				err = w.segFile.Sync()
			}
			w.segSize += int64(len(buf))
			w.segMu.Unlock()
		}

		if err == nil {
			w.durableLSN.Store(maxLSN)
		}

		// Notify all producers in this batch.
		for _, pw := range batch {
			pw.done <- err
		}
	}
}

// maybeRotate closes the current segment and opens a new one when the active
// segment has exceeded segSizeThreshold.
func (w *WAL) maybeRotate(nextLSN uint64) error {
	w.segMu.Lock()
	defer w.segMu.Unlock()
	if w.segSize < segSizeThreshold {
		return nil
	}
	// Close current segment.
	if err := w.segFile.Sync(); err != nil {
		return err
	}
	if err := w.segFile.Close(); err != nil {
		return err
	}
	w.segsMu.Lock()
	w.segs = append(w.segs, w.segFirst)
	w.segsMu.Unlock()

	// Open new segment.
	return w.openNewSegment(nextLSN)
}

// --- helpers -----------------------------------------------------------------

func (w *WAL) segPath(firstLSN uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%019d.wal", firstLSN))
}

// scanSegmentMaxLSN reads a segment file and returns its highest LSN and file size.
// Stops at the first CRC mismatch (torn tail) rather than failing hard.
func scanSegmentMaxLSN(path string) (maxLSN uint64, fileSize int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	fileSize = info.Size()

	var pos int64
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return maxLSN, fileSize, err
		}
		total := int(binary.LittleEndian.Uint32(lenBuf[:]))
		if total < recordHeaderSize+recordCRCSize || total > 16<<20 {
			break // torn write
		}
		rest := make([]byte, total-4)
		if _, err := io.ReadFull(f, rest); err != nil {
			break
		}
		rec, err := Decode(append(lenBuf[:], rest...))
		if err != nil {
			break // CRC mismatch at tail → stop
		}
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		pos += int64(total)
	}
	return maxLSN, fileSize, nil
}

// replaySegment reads all valid records from a segment file and calls fn for
// those with LSN >= from. isActive controls error handling: a bad record at the
// tail of the active segment is a torn write (stop cleanly); mid-log is a hard fail.
func replaySegment(path string, from uint64, isActive bool, fn func(uint64, Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	var pos int64
	var lastGoodPos int64

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return fmt.Errorf("wal: read length at %d: %w", pos, err)
		}
		total := int(binary.LittleEndian.Uint32(lenBuf[:]))
		if total < recordHeaderSize+recordCRCSize || total > 16<<20 {
			if isActive {
				break // torn write at tail of active segment
			}
			return fmt.Errorf("wal: bad record length %d at pos %d in closed segment %s", total, pos, path)
		}
		rest := make([]byte, total-4)
		if _, err := io.ReadFull(f, rest); err != nil {
			if isActive {
				break // torn write
			}
			return fmt.Errorf("wal: short record at %d: %w", pos, err)
		}
		rec, err := Decode(append(lenBuf[:], rest...))
		if err != nil {
			if isActive && pos >= lastGoodPos {
				break // torn tail
			}
			return fmt.Errorf("wal: %w at pos %d in %s (mid-log)", err, pos, path)
		}
		lastGoodPos = pos + int64(total)
		pos += int64(total)

		if rec.LSN >= from {
			if err := fn(rec.LSN, rec); err != nil {
				return err
			}
		}
	}
	return nil
}
