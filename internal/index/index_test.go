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

package index

import (
	"os"
	"sync"
	"testing"

	"github.com/samanyugoyal2010/frankenqueue/internal/pagestore"
)

func openIndexAndStore(t *testing.T) (*Index, *pagestore.Store) {
	t.Helper()
	dir := t.TempDir()
	s, err := pagestore.Open(dir)
	if err != nil {
		t.Fatalf("pagestore.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	idx, err := Open(dir, s)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return idx, s
}

func allocSlot(t *testing.T, idx *Index) uint64 {
	t.Helper()
	ref, err := idx.AllocSlot()
	if err != nil {
		t.Fatalf("AllocSlot: %v", err)
	}
	return ref
}

func makeSlot(msgID uint64, state uint8) Slot {
	return Slot{
		MsgID:         msgID,
		EnqueuedAtMs:  1000,
		AvailableAtMs: 1000,
		PayloadRef:    0x300004080, // a fake SlotRef
		PayloadLen:    64,
		Priority:      5,
		State:         state,
	}
}

// ── 1. Entry wire size is 72 bytes ───────────────────────────────────────────

func TestEntrySize(t *testing.T) {
	if entrySize != 72 {
		t.Fatalf("entrySize = %d, want 72", entrySize)
	}
}

// ── 2. AllocSlot + Put + GetByRef ────────────────────────────────────────────

func TestAllocPutGet(t *testing.T) {
	idx, _ := openIndexAndStore(t)
	ref := allocSlot(t, idx)
	idx.Put(ref, makeSlot(42, StateReady))

	got, ok := idx.GetByRef(ref)
	if !ok || got.MsgID != 42 || got.State != StateReady {
		t.Fatalf("GetByRef: ok=%v slot=%+v", ok, got)
	}
}

// ── 3. FreeSlot removes the slot ─────────────────────────────────────────────

func TestFreeSlot(t *testing.T) {
	idx, _ := openIndexAndStore(t)
	ref := allocSlot(t, idx)
	idx.Put(ref, makeSlot(99, StateReady))
	idx.FreeSlot(ref)

	_, ok := idx.GetByRef(ref)
	if ok {
		t.Fatal("expected slot to be gone after FreeSlot")
	}
}

// ── 4. Len counts only live slots ────────────────────────────────────────────

func TestLen(t *testing.T) {
	idx, _ := openIndexAndStore(t)
	if idx.Len() != 0 {
		t.Fatalf("initial Len = %d", idx.Len())
	}
	var refs []uint64
	for i := 0; i < 5; i++ {
		ref := allocSlot(t, idx)
		idx.Put(ref, makeSlot(uint64(i+1), StateReady))
		refs = append(refs, ref)
	}
	if idx.Len() != 5 {
		t.Fatalf("Len = %d, want 5", idx.Len())
	}
	idx.FreeSlot(refs[2])
	if idx.Len() != 4 {
		t.Fatalf("Len after free = %d, want 4", idx.Len())
	}
}

// ── 5. ScanPayloadPage returns only matching slots ────────────────────────────

func TestScanPayloadPage(t *testing.T) {
	idx, _ := openIndexAndStore(t)
	// payloadRef encodes pageID in bits 63..20
	const bits = 20
	for i := uint64(1); i <= 6; i++ {
		ref := allocSlot(t, idx)
		s := makeSlot(i, StateReady)
		s.PayloadRef = (i % 3) << bits // pages 0,1,2
		idx.Put(ref, s)
	}
	var found int
	idx.ScanPayloadPage(1, func(_ uint64, _ Slot) { found++ })
	if found != 2 {
		t.Fatalf("ScanPayloadPage(1): got %d, want 2", found)
	}
}

// ── 6. Scan visits all live slots ────────────────────────────────────────────

func TestScan(t *testing.T) {
	idx, _ := openIndexAndStore(t)
	for i := uint64(1); i <= 10; i++ {
		ref := allocSlot(t, idx)
		idx.Put(ref, makeSlot(i, StateReady))
	}
	var count int
	idx.Scan(func(_ uint64, _ Slot) { count++ })
	if count != 10 {
		t.Fatalf("Scan count = %d, want 10", count)
	}
}

// ── 7. Checkpoint: all fields survive save + restore ─────────────────────────

func TestCheckpointRestore(t *testing.T) {
	dir := t.TempDir()
	type entry struct {
		ref  uint64
		slot Slot
	}
	var saved []entry

	{
		s, _ := pagestore.Open(dir)
		idx, _ := Open(dir, s)
		for i := uint64(1); i <= 20; i++ {
			ref, _ := idx.AllocSlot()
			sl := Slot{
				MsgID:         i,
				EnqueuedAtMs:  int64(i * 1000),
				AvailableAtMs: int64(i * 1000),
				PayloadRef:    uint64(i) << 20,
				PayloadLen:    uint32(i * 64),
				Priority:      uint8(i % 8),
				State:         StateReady,
				Attempts:      uint16(i % 3),
			}
			idx.Put(ref, sl)
			saved = append(saved, entry{ref, sl})
		}
		if err := idx.Checkpoint(42); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		s.Close()
	}

	{
		s, _ := pagestore.Open(dir)
		idx, err := Open(dir, s)
		defer s.Close()
		if err != nil {
			t.Fatalf("Open restore: %v", err)
		}
		if idx.CheckpointLSN() != 42 {
			t.Fatalf("LSN = %d, want 42", idx.CheckpointLSN())
		}
		if idx.Len() != 20 {
			t.Fatalf("Len = %d, want 20", idx.Len())
		}
		for _, e := range saved {
			got, ok := idx.GetByRef(e.ref)
			if !ok || got.MsgID != e.slot.MsgID || got.State != e.slot.State ||
				got.Attempts != e.slot.Attempts || got.PayloadRef != e.slot.PayloadRef ||
				got.PayloadLen != e.slot.PayloadLen {
				t.Fatalf("ref %x mismatch: got %+v want %+v", e.ref, got, e.slot)
			}
		}
	}
}

// ── 8. Corrupt index.dat → fallback to empty ─────────────────────────────────

func TestCorruptCheckpointFallback(t *testing.T) {
	dir := t.TempDir()
	{
		s, _ := pagestore.Open(dir)
		idx, _ := Open(dir, s)
		ref, _ := idx.AllocSlot()
		idx.Put(ref, makeSlot(77, StateReady))
		idx.Checkpoint(10)
		s.Close()
	}
	os.WriteFile(dir+"/index.dat", []byte("totally corrupt !@#$"), 0o644)

	s, _ := pagestore.Open(dir)
	defer s.Close()
	idx, err := Open(dir, s)
	if err != nil {
		t.Fatalf("Open with corrupt file: %v", err)
	}
	if idx.CheckpointLSN() != 0 || idx.Len() != 0 {
		t.Fatalf("expected empty fallback: LSN=%d Len=%d", idx.CheckpointLSN(), idx.Len())
	}
}

// ── 9. All slot fields round-trip through encode/decode ──────────────────────

func TestSlotRoundTrip(t *testing.T) {
	want := Slot{
		MsgID:         0xDEADBEEFCAFEBABE,
		EnqueuedAtMs:  -1234567890,
		AvailableAtMs: 9876543210,
		PayloadRef:    0xABCDEF001,
		PayloadLen:    65535,
		Priority:      255,
		State:         StateDead,
		Attempts:      0xBEEF,
		LeaseNonce:    0x0123456789ABCDEF,
	}
	var buf [64]byte
	encodeSlot(buf[:], want)
	got := decodeSlot(buf[:])
	if got != want {
		t.Fatalf("round-trip mismatch:\n  got  %+v\n  want %+v", got, want)
	}
}

// ── 10. Slot CRC mismatch → whole checkpoint rejected ────────────────────────

func TestCheckpointSlotCRCDetected(t *testing.T) {
	dir := t.TempDir()
	{
		s, _ := pagestore.Open(dir)
		idx, _ := Open(dir, s)
		ref, _ := idx.AllocSlot()
		idx.Put(ref, makeSlot(1, StateReady))
		idx.Checkpoint(5)
		s.Close()
	}
	data, _ := os.ReadFile(dir + "/index.dat")
	data[headerSize+10] ^= 0xFF // corrupt a byte in entry 0
	os.WriteFile(dir+"/index.dat", data, 0o644)

	s, _ := pagestore.Open(dir)
	defer s.Close()
	idx, _ := Open(dir, s)
	if idx.CheckpointLSN() != 0 {
		t.Fatalf("expected LSN=0 after CRC corrupt, got %d", idx.CheckpointLSN())
	}
}

// ── 11. Concurrent AllocSlot + Put + FreeSlot under -race ────────────────────

func TestConcurrentAllocPutFree(t *testing.T) {
	idx, _ := openIndexAndStore(t)
	const goroutines, ops = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			var refs []uint64
			for i := 0; i < ops; i++ {
				ref, err := idx.AllocSlot()
				if err != nil {
					return
				}
				idx.Put(ref, makeSlot(uint64(g*10000+i+1), StateReady))
				refs = append(refs, ref)
			}
			for _, ref := range refs {
				idx.FreeSlot(ref)
			}
		}()
	}
	wg.Wait()
}

// ── 12. Checkpoint atomicity: leftover .tmp doesn't corrupt open ──────────────

func TestCheckpointAtomicity(t *testing.T) {
	dir := t.TempDir()

	s1, _ := pagestore.Open(dir)
	idx1, _ := Open(dir, s1)
	ref, _ := idx1.AllocSlot()
	idx1.Put(ref, makeSlot(1, StateReady))
	if err := idx1.Checkpoint(1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	s1.Close()

	os.WriteFile(dir+"/index.dat.tmp", []byte("incomplete"), 0o644)

	s2, _ := pagestore.Open(dir)
	defer s2.Close()
	idx2, err := Open(dir, s2)
	if err != nil {
		t.Fatalf("Open after stale .tmp: %v", err)
	}
	if idx2.CheckpointLSN() != 1 {
		t.Fatalf("expected LSN=1, got %d", idx2.CheckpointLSN())
	}
}

