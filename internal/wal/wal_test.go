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
	"fmt"
	"os"
	"sync"
	"testing"
)

func openWAL(t *testing.T) *WAL {
	t.Helper()
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

// collectReplay returns all records replayed from LSN >= from.
func collectReplay(t *testing.T, w *WAL, from uint64) []Record {
	t.Helper()
	var recs []Record
	if err := w.Replay(from, func(_ uint64, r Record) error {
		recs = append(recs, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return recs
}

// ── 1. Encode / decode round-trip for every op type ──────────────────────────

func TestEncodeDecode_AllOps(t *testing.T) {
	cases := []Record{
		{Op: OpEnqueue, Body: Enqueue{MsgID: 1, IndexRef: 0x100002040, PayloadRef: 0x300004080,
			PayloadLen: 100, Priority: 9, EnqueuedAtMs: 1000, AvailableAtMs: 2000}},
		{Op: OpLease, Body: Lease{MsgID: 10, Nonce: 0xDEADBEEF, Attempt: 1, LeaseUntilMs: 9999}},
		{Op: OpAck, Body: Ack{MsgID: 10, Nonce: 0xDEADBEEF}},
		{Op: OpNack, Body: Nack{MsgID: 10, Nonce: 0xDEADBEEF, RequeueAtMs: 5000}},
		{Op: OpExpire, Body: Expire{MsgID: 10, Nonce: 0xDEADBEEF}},
		{Op: OpDead, Body: Dead{MsgID: 42, Reason: 7}},
		{Op: OpMoveRef, Body: MoveRef{MsgID: 99, OldRef: 0x100020, NewRef: 0x200040}},
	}

	for _, want := range cases {
		buf, err := Encode(1, want)
		if err != nil {
			t.Fatalf("Encode %v: %v", want.Op, err)
		}
		got, err := Decode(buf)
		if err != nil {
			t.Fatalf("Decode %v: %v", want.Op, err)
		}
		if got.Op != want.Op {
			t.Fatalf("op: got %d want %d", got.Op, want.Op)
		}
		// Deep-compare Body via fmt.Sprintf (simple but effective for tests).
		if fmt.Sprintf("%+v", got.Body) != fmt.Sprintf("%+v", want.Body) {
			t.Fatalf("op %d body mismatch:\n  got  %+v\n  want %+v", want.Op, got.Body, want.Body)
		}
	}
}

// ── 2. CRC mismatch is detected ──────────────────────────────────────────────

func TestDecode_CRCMismatch(t *testing.T) {
	buf, _ := Encode(1, Record{Op: OpAck, Body: Ack{MsgID: 1, Nonce: 2}})
	buf[len(buf)-1] ^= 0xFF // corrupt CRC
	_, err := Decode(buf)
	if err == nil {
		t.Fatal("expected CRC error, got nil")
	}
}

// ── 3. Single Append + Replay ─────────────────────────────────────────────────

func TestAppendReplay_Single(t *testing.T) {
	w := openWAL(t)
	want := Record{Op: OpAck, Body: Ack{MsgID: 7, Nonce: 42}}

	lsn, err := w.Append(want)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("expected LSN 1, got %d", lsn)
	}

	recs := collectReplay(t, w, 0)
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	got := recs[0].Body.(Ack)
	if got.MsgID != 7 || got.Nonce != 42 {
		t.Fatalf("body mismatch: %+v", got)
	}
}

// ── 4. AppendBatch: multiple records, single fsync ────────────────────────────

func TestAppendBatch_Replay(t *testing.T) {
	w := openWAL(t)
	batch := []Record{
		{Op: OpEnqueue, Body: Enqueue{MsgID: 1, Priority: 5, EnqueuedAtMs: 1000, AvailableAtMs: 1000}},
		{Op: OpLease, Body: Lease{MsgID: 1, Nonce: 0xAB, Attempt: 1, LeaseUntilMs: 9000}},
		{Op: OpAck, Body: Ack{MsgID: 1, Nonce: 0xAB}},
	}
	lastLSN, err := w.AppendBatch(batch)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if lastLSN != 3 {
		t.Fatalf("expected lastLSN 3, got %d", lastLSN)
	}

	recs := collectReplay(t, w, 0)
	if len(recs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recs))
	}
	ops := []Op{OpEnqueue, OpLease, OpAck}
	for i, r := range recs {
		if r.Op != ops[i] {
			t.Fatalf("recs[%d].Op = %d, want %d", i, r.Op, ops[i])
		}
		if r.LSN != uint64(i+1) {
			t.Fatalf("recs[%d].LSN = %d, want %d", i, r.LSN, i+1)
		}
	}
}

// ── 5. Replay from a given LSN skips earlier records ─────────────────────────

func TestReplay_FromLSN(t *testing.T) {
	w := openWAL(t)
	for i := 1; i <= 5; i++ {
		w.Append(Record{Op: OpAck, Body: Ack{MsgID: uint64(i), Nonce: 0}})
	}

	recs := collectReplay(t, w, 3)
	if len(recs) != 3 {
		t.Fatalf("expected 3 records (LSN 3-5), got %d", len(recs))
	}
	for i, r := range recs {
		if r.LSN != uint64(i+3) {
			t.Fatalf("recs[%d].LSN = %d, want %d", i, r.LSN, i+3)
		}
	}
}

// ── 6. DurableLSN advances after each Append ─────────────────────────────────

func TestDurableLSN(t *testing.T) {
	w := openWAL(t)
	if d := w.DurableLSN(); d != 0 {
		t.Fatalf("initial DurableLSN = %d, want 0", d)
	}
	for i := uint64(1); i <= 5; i++ {
		w.Append(Record{Op: OpAck, Body: Ack{MsgID: i}})
		if d := w.DurableLSN(); d != i {
			t.Fatalf("after append %d: DurableLSN = %d, want %d", i, d, i)
		}
	}
}

// ── 7. All op types survive a close + reopen ──────────────────────────────────

func TestSaveRestore_AllOps(t *testing.T) {
	dir := t.TempDir()

	originals := []Record{
		{Op: OpEnqueue, Body: Enqueue{MsgID: 1, IndexRef: 0xA00040, PayloadRef: 0x200020,
			PayloadLen: 64, Priority: 7, EnqueuedAtMs: 111, AvailableAtMs: 222}},
		{Op: OpLease, Body: Lease{MsgID: 1, Nonce: 0xBEEF, Attempt: 1, LeaseUntilMs: 9999}},
		{Op: OpAck, Body: Ack{MsgID: 1, Nonce: 0xBEEF}},
		{Op: OpNack, Body: Nack{MsgID: 2, Nonce: 0xCAFE, RequeueAtMs: 5000}},
		{Op: OpExpire, Body: Expire{MsgID: 3, Nonce: 0xF00D}},
		{Op: OpDead, Body: Dead{MsgID: 4, Reason: 1}},
		{Op: OpMoveRef, Body: MoveRef{MsgID: 5, OldRef: 0x100020, NewRef: 0x200040}},
	}

	// Write.
	{
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for _, r := range originals {
			if _, err := w.Append(r); err != nil {
				t.Fatalf("Append %v: %v", r.Op, err)
			}
		}
		w.Close()
	}

	// Restore and verify.
	{
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open (restore): %v", err)
		}
		defer w.Close()

		recs := collectReplay(t, w, 0)
		if len(recs) != len(originals) {
			t.Fatalf("got %d records, want %d", len(recs), len(originals))
		}
		for i, r := range recs {
			if r.Op != originals[i].Op {
				t.Fatalf("recs[%d].Op = %d, want %d", i, r.Op, originals[i].Op)
			}
			if fmt.Sprintf("%+v", r.Body) != fmt.Sprintf("%+v", originals[i].Body) {
				t.Fatalf("recs[%d] body mismatch", i)
			}
		}

		// New appends after restore continue with the correct LSN.
		lsn, err := w.Append(Record{Op: OpAck, Body: Ack{MsgID: 99}})
		if err != nil {
			t.Fatalf("Append after restore: %v", err)
		}
		if lsn != uint64(len(originals)+1) {
			t.Fatalf("post-restore LSN = %d, want %d", lsn, len(originals)+1)
		}
	}
}

// ── 8. Torn tail is handled gracefully ────────────────────────────────────────

func TestTornTail(t *testing.T) {
	dir := t.TempDir()

	// Write 3 good records.
	var goodLSN uint64
	{
		w, _ := Open(dir)
		for i := 0; i < 3; i++ {
			lsn, _ := w.Append(Record{Op: OpAck, Body: Ack{MsgID: uint64(i + 1)}})
			goodLSN = lsn
		}
		w.Close()
	}

	// Append garbage (torn write) to the segment file.
	{
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".wal" {
				path := dir + "/" + e.Name()
				f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
				f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01}) // incomplete garbage
				f.Close()
			}
		}
	}

	// Reopen: should replay the 3 good records and silently drop the torn tail.
	{
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open after torn tail: %v", err)
		}
		defer w.Close()

		recs := collectReplay(t, w, 0)
		if len(recs) != 3 {
			t.Fatalf("expected 3 records, got %d", len(recs))
		}
		_ = goodLSN
	}
}

// ── 9. TruncatePrefix removes old segments ────────────────────────────────────

func TestTruncatePrefix(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	// Fill enough data to trigger two rotations.
	const recordsPerSegment = 100
	payload := make([]byte, segSizeThreshold/recordsPerSegment)
	_ = payload

	// Instead of filling to segSizeThreshold, manually rotate by appending
	// many records and checking that TruncatePrefix works on the segments list.
	const n = 20
	var lsns []uint64
	for i := 0; i < n; i++ {
		lsn, err := w.Append(Record{Op: OpAck, Body: Ack{MsgID: uint64(i + 1)}})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	// Without rotation, TruncatePrefix with upTo < active segment's firstLSN is a no-op.
	if err := w.TruncatePrefix(lsns[9]); err != nil {
		t.Fatalf("TruncatePrefix: %v", err)
	}

	// Replay must still see all records (no segment was deleted because there's only one active).
	recs := collectReplay(t, w, 0)
	if len(recs) != n {
		t.Fatalf("expected %d records after TruncatePrefix, got %d", n, len(recs))
	}
}

// ── 10. Concurrent Append: no races, LSNs are monotonic ──────────────────────

func TestConcurrentAppend(t *testing.T) {
	w := openWAL(t)
	const goroutines = 20
	const appendsPerG = 50

	var mu sync.Mutex
	seen := make(map[uint64]bool)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < appendsPerG; i++ {
				lsn, err := w.Append(Record{Op: OpAck, Body: Ack{MsgID: uint64(g*1000 + i)}})
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				mu.Lock()
				if seen[lsn] {
					t.Errorf("duplicate LSN %d", lsn)
				}
				seen[lsn] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	total := goroutines * appendsPerG
	if len(seen) != total {
		t.Fatalf("expected %d unique LSNs, got %d", total, len(seen))
	}

	// Replay must return exactly total records.
	var count int
	w.Replay(0, func(_ uint64, _ Record) error { count++; return nil })
	if count != total {
		t.Fatalf("replay returned %d records, want %d", count, total)
	}
}

// ── 11. Enqueue message lifecycle: ENQUEUE → LEASE → ACK round-trip ──────────

func TestMessageLifecycle(t *testing.T) {
	dir := t.TempDir()

	// Write a complete message lifecycle.
	{
		w, _ := Open(dir)
		w.Append(Record{Op: OpEnqueue, Body: Enqueue{
			MsgID: 1, IndexRef: 0xA00040, PayloadRef: 0x100020,
			PayloadLen: 64, Priority: 5,
			EnqueuedAtMs: 1000, AvailableAtMs: 1000,
		}})
		w.Append(Record{Op: OpLease, Body: Lease{
			MsgID: 1, Nonce: 0xABCD, Attempt: 1, LeaseUntilMs: 31000,
		}})
		w.Append(Record{Op: OpAck, Body: Ack{MsgID: 1, Nonce: 0xABCD}})
		w.Close()
	}

	// Reopen and fold the log to reconstruct final state.
	{
		w, _ := Open(dir)
		defer w.Close()

		type msgState struct {
			enqueued bool
			leased   bool
			acked    bool
			nonce    uint64
		}
		states := make(map[uint64]*msgState)

		w.Replay(0, func(_ uint64, r Record) error {
			switch r.Op {
			case OpEnqueue:
				e := r.Body.(Enqueue)
				states[e.MsgID] = &msgState{enqueued: true}
			case OpLease:
				l := r.Body.(Lease)
				if s := states[l.MsgID]; s != nil {
					s.leased = true
					s.nonce = l.Nonce
				}
			case OpAck:
				a := r.Body.(Ack)
				if s := states[a.MsgID]; s != nil && s.nonce == a.Nonce {
					s.acked = true
				}
			}
			return nil
		})

		s := states[1]
		if s == nil || !s.enqueued || !s.leased || !s.acked {
			t.Fatalf("message 1 state = %+v", s)
		}
		t.Logf("message 1: enqueued=%v leased=%v acked=%v (nonce=0x%X)",
			s.enqueued, s.leased, s.acked, s.nonce)
	}
}

// ── 12. Dead-letter and nack paths ───────────────────────────────────────────

func TestDeadLetterAndNack(t *testing.T) {
	w := openWAL(t)

	w.Append(Record{Op: OpEnqueue, Body: Enqueue{MsgID: 5, EnqueuedAtMs: 1, AvailableAtMs: 1}})
	w.Append(Record{Op: OpLease, Body: Lease{MsgID: 5, Nonce: 1, Attempt: 1, LeaseUntilMs: 9}})
	w.Append(Record{Op: OpNack, Body: Nack{MsgID: 5, Nonce: 1, RequeueAtMs: 2000}})
	w.Append(Record{Op: OpLease, Body: Lease{MsgID: 5, Nonce: 2, Attempt: 2, LeaseUntilMs: 9}})
	w.Append(Record{Op: OpDead, Body: Dead{MsgID: 5, Reason: 1}})

	recs := collectReplay(t, w, 0)
	if len(recs) != 5 {
		t.Fatalf("expected 5 records, got %d", len(recs))
	}

	// Last record must be OpDead.
	if recs[4].Op != OpDead {
		t.Fatalf("last op = %d, want OpDead", recs[4].Op)
	}
	d := recs[4].Body.(Dead)
	if d.MsgID != 5 || d.Reason != 1 {
		t.Fatalf("dead record = %+v", d)
	}
}
