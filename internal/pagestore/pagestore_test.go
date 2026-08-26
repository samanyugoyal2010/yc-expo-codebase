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

package pagestore

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// --- helpers -----------------------------------------------------------------

func openStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func mustAlloc(t *testing.T, s *Store, size uint32) (SlotRef, []byte) {
	t.Helper()
	ref, buf, err := s.Alloc(size)
	if err != nil {
		t.Fatalf("Alloc(%d): %v", size, err)
	}
	return ref, buf
}

func mustRead(t *testing.T, s *Store, ref SlotRef) []byte {
	t.Helper()
	buf, err := s.Read(ref)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return buf
}

func pageFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, _ := filepath.Glob(filepath.Join(dir, "pages", "*.page"))
	return entries
}

// --- header.go ---------------------------------------------------------------

func TestHeaderWriteRead(t *testing.T) {
	h := NewSlabHeader(42, 3)
	var buf [PageSize]byte
	WriteHeader(buf[:], h)

	got, err := ReadHeader(buf[:])
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if got.PageID != 42 || got.ScIdx != 3 || got.IsJumbo() {
		t.Fatalf("unexpected header: %+v", got)
	}
	if got.Capacity == 0 || got.SlotsStart == 0 {
		t.Fatalf("layout not computed: capacity=%d slotsStart=%d", got.Capacity, got.SlotsStart)
	}
}

func TestHeaderJumboWriteRead(t *testing.T) {
	h := NewJumboHeader(7, 3)
	buf := make([]byte, PageSize*3)
	WriteHeader(buf, h)

	got, err := ReadHeader(buf)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if !got.IsJumbo() || got.ScIdx != ScIdxJumbo {
		t.Fatalf("expected jumbo header: %+v", got)
	}
	if got.SlotsStart != HeaderSize {
		t.Fatalf("jumbo SlotsStart=%d want %d", got.SlotsStart, HeaderSize)
	}
}

func TestHeaderValidateBadMagic(t *testing.T) {
	var buf [PageSize]byte
	WriteHeader(buf[:], NewSlabHeader(1, 0))
	buf[0] ^= 0xFF
	if _, err := ReadHeader(buf[:]); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestHeaderValidateBadCRC(t *testing.T) {
	var buf [PageSize]byte
	WriteHeader(buf[:], NewSlabHeader(1, 0))
	buf[10] ^= 0x01
	if _, err := ReadHeader(buf[:]); err == nil {
		t.Fatal("expected error for bad CRC")
	}
}

func TestHeaderBumpGen(t *testing.T) {
	var buf [PageSize]byte
	WriteHeader(buf[:], NewSlabHeader(1, 0))
	gen := BumpGen(buf[:])
	if gen != 1 {
		t.Fatalf("BumpGen returned %d, want 1", gen)
	}
	got, err := ReadHeader(buf[:])
	if err != nil {
		t.Fatalf("ReadHeader after BumpGen: %v", err)
	}
	if got.Gen != 1 {
		t.Fatalf("gen in header = %d, want 1", got.Gen)
	}
}

func TestHeaderSetGen(t *testing.T) {
	var buf [PageSize]byte
	WriteHeader(buf[:], NewSlabHeader(1, 0))
	SetGen(buf[:], 99)
	got, err := ReadHeader(buf[:])
	if err != nil {
		t.Fatalf("ReadHeader after SetGen: %v", err)
	}
	if got.Gen != 99 {
		t.Fatalf("gen = %d, want 99", got.Gen)
	}
}

func TestPageFlags(t *testing.T) {
	f := FlagJumbo | FlagSealed
	if !f.Has(FlagJumbo) || !f.Has(FlagSealed) {
		t.Fatal("Has() failed")
	}
	if f.Has(FlagCompressed) {
		t.Fatal("unexpected FlagCompressed set")
	}
	if s := f.String(); s == "" || s == "none" {
		t.Fatalf("unexpected String(): %q", s)
	}
}

func TestSlabLayout(t *testing.T) {
	for i, sc := range SizeClasses {
		h := NewSlabHeader(0, uint8(i))
		if h.Capacity == 0 {
			t.Fatalf("class %d (%d bytes): capacity is 0", i, sc)
		}
		if h.SlotsStart%sc != 0 {
			t.Fatalf("class %d: slotsStart %d not aligned to slot %d", i, h.SlotsStart, sc)
		}
		if h.SlotsStart <= HeaderSize {
			t.Fatalf("class %d: slotsStart %d <= HeaderSize", i, h.SlotsStart)
		}
		if end := h.SlotsStart + h.Capacity*sc; end > PageSize {
			t.Fatalf("class %d: slots overflow page (end=%d)", i, end)
		}
	}
}

// --- page.go bitmap ----------------------------------------------------------

func newTestPage(t *testing.T, sci int) *page {
	t.Helper()
	h := NewSlabHeader(1, uint8(sci))
	data := make([]byte, PageSize)
	WriteHeader(data, h)
	pg := &page{
		data:       data,
		sizeClass:  h.SlotSize(),
		capacity:   h.Capacity,
		slotsStart: h.SlotsStart,
	}
	pg.initBitmap()
	return pg
}

func TestInitBitmap(t *testing.T) {
	pg := newTestPage(t, 0)
	if got := pg.freeCount.Load(); got != int32(pg.capacity) {
		t.Fatalf("freeCount=%d want %d", got, pg.capacity)
	}
	for i := 0; i < pg.numWords(); i++ {
		if w := *pg.bitmapWord(i); w != 0 {
			t.Fatalf("bitmap word %d = %016x, want 0", i, w)
		}
	}
}

func TestAllocFreeSlot(t *testing.T) {
	pg := newTestPage(t, 0)
	cap := pg.capacity

	idx, ok := pg.allocSlot()
	if !ok {
		t.Fatal("allocSlot on fresh page returned false")
	}
	if pg.freeCount.Load() != int32(cap)-1 {
		t.Fatalf("freeCount after alloc = %d, want %d", pg.freeCount.Load(), cap-1)
	}
	if empty := pg.freeSlot(idx); !empty {
		t.Fatal("freeSlot of only allocation should report page empty")
	}
	if pg.freeCount.Load() != int32(cap) {
		t.Fatalf("freeCount after free = %d, want %d", pg.freeCount.Load(), cap)
	}
}

func TestAllocPageFull(t *testing.T) {
	pg := newTestPage(t, 0)
	for i := 0; i < int(pg.capacity); i++ {
		if _, ok := pg.allocSlot(); !ok {
			t.Fatalf("allocSlot failed at slot %d (capacity %d)", i, pg.capacity)
		}
	}
	if _, ok := pg.allocSlot(); ok {
		t.Fatal("allocSlot on full page should return false")
	}
}

func TestBitmapConcurrent(t *testing.T) {
	pg := newTestPage(t, 0)
	cap := int(pg.capacity)

	var mu sync.Mutex
	var wg sync.WaitGroup
	allocated := make([]uint32, 0, cap)

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx, ok := pg.allocSlot()
				if !ok {
					return
				}
				mu.Lock()
				allocated = append(allocated, idx)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(allocated) != cap {
		t.Fatalf("allocated %d slots, want %d", len(allocated), cap)
	}
	seen := make(map[uint32]bool, cap)
	for _, idx := range allocated {
		if seen[idx] {
			t.Fatalf("slot %d allocated twice", idx)
		}
		seen[idx] = true
	}
}

// --- store.go: basic alloc/read ----------------------------------------------

func TestAllocReadSmall(t *testing.T) {
	s, _ := openStore(t)
	payload := []byte("hello, pagestore")
	ref, buf := mustAlloc(t, s, uint32(len(payload)))
	copy(buf, payload)
	got := mustRead(t, s, ref)
	if !bytes.Equal(got[:len(payload)], payload) {
		t.Fatalf("got %q want %q", got[:len(payload)], payload)
	}
}

func TestAllocAllSizeClasses(t *testing.T) {
	s, _ := openStore(t)
	for i, sc := range SizeClasses {
		ref, buf := mustAlloc(t, s, sc)
		for j := range buf {
			buf[j] = byte(i*31 + j)
		}
		got := mustRead(t, s, ref)
		for j, b := range got {
			if want := byte(i*31 + j); b != want {
				t.Fatalf("class %d off %d: got %d want %d", i, j, b, want)
			}
		}
	}
}

func TestAllocJumbo(t *testing.T) {
	s, _ := openStore(t)
	size := uint32(JumboThreshold + 1)
	ref, buf := mustAlloc(t, s, size)
	pattern := bytes.Repeat([]byte{0xAB}, int(size))
	copy(buf, pattern)
	got := mustRead(t, s, ref)
	if !bytes.Equal(got[:size], pattern) {
		t.Fatal("jumbo payload mismatch")
	}
}

func TestAllocZeroErrors(t *testing.T) {
	s, _ := openStore(t)
	if _, _, err := s.Alloc(0); err == nil {
		t.Fatal("Alloc(0) should return error")
	}
}

func TestPageFillCreatesNewPage(t *testing.T) {
	s, _ := openStore(t)
	h := NewSlabHeader(0, 0)
	cap := int(h.Capacity)
	refs := make([]SlotRef, cap+1)
	for i := range refs {
		refs[i], _ = mustAlloc(t, s, SizeClasses[0])
	}
	if refs[0].PageID() == refs[cap].PageID() {
		t.Fatal("all records stayed on same page after filling capacity")
	}
}

// TestSlotAddressFromRef verifies that ref.Offset points exactly to the written
// payload inside the mmap, and that the page header is readable from data[0].
func TestSlotAddressFromRef(t *testing.T) {
	s, _ := openStore(t)
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	ref, buf := mustAlloc(t, s, uint32(len(payload)))
	copy(buf, payload)

	s.mu.Lock()
	pg := s.pgs[ref.PageID()]
	s.mu.Unlock()

	slotSize := uint32(SizeClassFor(uint32(len(payload))))
	_ = slotSize
	direct := pg.data[ref.Offset() : ref.Offset()+pg.sizeClass]
	if !bytes.Equal(direct[:len(payload)], payload) {
		t.Fatal("data at ref.Offset does not match written payload")
	}
	h, err := ReadHeader(pg.data)
	if err != nil || h.PageID != ref.PageID() {
		t.Fatalf("bad header at page base: %v", err)
	}
}

// --- store.go: auto-reclaim --------------------------------------------------

func TestPageAutoReclaimSlab(t *testing.T) {
	s, dir := openStore(t)
	h := NewSlabHeader(0, 0)
	cap := int(h.Capacity)

	refs := make([]SlotRef, cap)
	for i := range refs {
		refs[i], _ = mustAlloc(t, s, SizeClasses[0])
	}
	if s.Stats().Pages != 1 {
		t.Fatalf("expected 1 page, got %d", s.Stats().Pages)
	}

	for _, ref := range refs {
		if err := s.Release(ref); err != nil {
			t.Fatalf("Release: %v", err)
		}
	}

	st := s.Stats()
	if st.Pages != 0 {
		t.Fatalf("pages after full release = %d, want 0", st.Pages)
	}
	if st.LiveBytes != 0 {
		t.Fatalf("LiveBytes = %d, want 0", st.LiveBytes)
	}
	if files := pageFiles(t, dir); len(files) != 0 {
		t.Fatalf("%d page files remain on disk after reclaim", len(files))
	}
}

func TestPageAutoReclaimJumbo(t *testing.T) {
	s, dir := openStore(t)
	ref, _ := mustAlloc(t, s, JumboThreshold+512)
	if err := s.Release(ref); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if s.Stats().Pages != 0 {
		t.Fatal("jumbo page not reclaimed after release")
	}
	if files := pageFiles(t, dir); len(files) != 0 {
		t.Fatalf("jumbo file remains on disk: %v", files)
	}
}

func TestPartialReleaseSurvives(t *testing.T) {
	s, _ := openStore(t)
	ref1, _ := mustAlloc(t, s, 64)
	ref2, _ := mustAlloc(t, s, 64)

	s.Release(ref1)
	if s.Stats().Pages == 0 {
		t.Fatal("page reclaimed while ref2 still live")
	}
	s.Release(ref2)
	if s.Stats().Pages != 0 {
		t.Fatal("page not reclaimed after last release")
	}
}

func TestStaleRefAfterReclaim(t *testing.T) {
	s, _ := openStore(t)
	ref, _ := mustAlloc(t, s, 64)
	s.Release(ref)
	if _, err := s.Read(ref); err == nil {
		t.Fatal("Read of reclaimed page should return error")
	}
}

// --- store.go: shrink to zero — 1M small records ----------------------------

// TestStoreShrinksToZero allocates 1 million 64-byte records, verifies every
// read-back, then releases all of them.  The store must fully reclaim every page:
//
//	Stats().Pages == 0
//	Stats().LiveBytes == 0
//	No .page files remain on disk
func TestStoreShrinksToZero(t *testing.T) {
	s, dir := openStore(t)

	const N = 1_000_000
	const size = uint32(64)

	refs := make([]SlotRef, N)
	for i := 0; i < N; i++ {
		ref, buf, err := s.Alloc(size)
		if err != nil {
			t.Fatalf("Alloc %d: %v", i, err)
		}
		buf[0] = byte(i)
		buf[1] = byte(i >> 8)
		refs[i] = ref
	}
	t.Logf("after alloc: pages=%d live=%d total=%d",
		s.Stats().Pages, s.Stats().LiveBytes, s.Stats().TotalBytes)

	for i, ref := range refs {
		data, err := s.Read(ref)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if data[0] != byte(i) || data[1] != byte(i>>8) {
			t.Fatalf("record %d: data mismatch", i)
		}
	}

	for i, ref := range refs {
		if err := s.Release(ref); err != nil {
			t.Fatalf("Release %d: %v", i, err)
		}
	}

	st := s.Stats()
	t.Logf("after release: pages=%d live=%d total=%d", st.Pages, st.LiveBytes, st.TotalBytes)

	if st.Pages != 0 {
		t.Fatalf("pages=%d after full release, want 0", st.Pages)
	}
	if st.LiveBytes != 0 {
		t.Fatalf("LiveBytes=%d, want 0", st.LiveBytes)
	}
	if files := pageFiles(t, dir); len(files) != 0 {
		t.Fatalf("%d page files remain on disk", len(files))
	}
}

// --- recovery ----------------------------------------------------------------

func TestSaveRestoreSlab(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("durable slab record")
	var ref SlotRef
	{
		s, _ := Open(dir)
		var buf []byte
		ref, buf, _ = s.Alloc(uint32(len(payload)))
		copy(buf, payload)
		s.Sync([]SlotRef{ref})
		s.Close()
	}
	{
		s, _ := Open(dir)
		defer s.Close()
		s.RebuildVolatile([]SlotRef{ref})
		got, err := s.Read(ref)
		if err != nil {
			t.Fatalf("Read after restore: %v", err)
		}
		if !bytes.Equal(got[:len(payload)], payload) {
			t.Fatalf("got %q want %q", got[:len(payload)], payload)
		}
	}
}

func TestSaveRestoreJumbo(t *testing.T) {
	dir := t.TempDir()
	size := uint32(JumboThreshold + 1024)
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	var ref SlotRef
	{
		s, _ := Open(dir)
		var buf []byte
		ref, buf, _ = s.Alloc(size)
		copy(buf, payload)
		s.Sync([]SlotRef{ref})
		s.Close()
	}
	{
		s, _ := Open(dir)
		defer s.Close()
		s.RebuildVolatile([]SlotRef{ref})
		got, err := s.Read(ref)
		if err != nil {
			t.Fatalf("Read after restore: %v", err)
		}
		if !bytes.Equal(got[:len(payload)], payload) {
			t.Fatal("jumbo payload mismatch after restore")
		}
	}
}

func TestRebuildVolatilePartialLiveSet(t *testing.T) {
	dir := t.TempDir()
	h := NewSlabHeader(0, 0)
	cap := int(h.Capacity)
	var liveRefs []SlotRef
	{
		s, _ := Open(dir)
		for i := 0; i < cap; i++ {
			ref, _ := mustAlloc(t, s, SizeClasses[0])
			liveRefs = append(liveRefs, ref)
		}
		s.Sync(liveRefs)
		s.Close()
	}
	{
		s, _ := Open(dir)
		defer s.Close()
		half := liveRefs[:cap/2]
		s.RebuildVolatile(half)

		s.mu.Lock()
		var pg *page
		for _, p := range s.pgs {
			pg = p
			break
		}
		s.mu.Unlock()

		if pg == nil {
			t.Fatal("no pages after RebuildVolatile")
		}
		want := int32(cap - cap/2)
		if got := pg.freeCount.Load(); got != want {
			t.Fatalf("freeCount=%d want %d", got, want)
		}
	}
}

// --- concurrency -------------------------------------------------------------

func TestConcurrentAllocRelease(t *testing.T) {
	s, _ := openStore(t)
	const goroutines = 16
	const perG = 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < perG; i++ {
				size := uint32(rng.Intn(512) + 1)
				ref, buf, err := s.Alloc(size)
				if err != nil {
					return
				}
				buf[0] = byte(g)
				data, err := s.Read(ref)
				if err == nil && data[0] == byte(g) {
					s.Release(ref)
				}
			}
		}(g)
	}
	wg.Wait()
}

// --- stats -------------------------------------------------------------------

func TestStats(t *testing.T) {
	s, _ := openStore(t)
	ref, _ := mustAlloc(t, s, 64)
	st := s.Stats()
	if st.Pages != 1 || st.LiveBytes == 0 {
		t.Fatalf("after alloc: %+v", st)
	}
	s.Release(ref)
	st = s.Stats()
	if st.Pages != 0 || st.LiveBytes != 0 {
		t.Fatalf("after release: %+v", st)
	}
}

// --- benchmarks --------------------------------------------------------------

func BenchmarkAlloc64(b *testing.B) {
	s, _ := Open(b.TempDir())
	defer s.Close()
	refs := make([]SlotRef, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ref, _, _ := s.Alloc(64)
		refs[i] = ref
	}
	b.StopTimer()
	for _, ref := range refs {
		s.Release(ref)
	}
}

func BenchmarkAllocRelease64(b *testing.B) {
	s, _ := Open(b.TempDir())
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ref, _, _ := s.Alloc(64)
		s.Release(ref)
	}
}

// --- random size alloc/free --------------------------------------------------

// randSize returns a random payload size that exercises every size class and
// occasionally produces a jumbo record.
func randSize(rng *rand.Rand) uint32 {
	// 95% slab (pick a random size class), 5% jumbo
	if rng.Intn(20) < 19 {
		sci := rng.Intn(len(SizeClasses))
		// request a random size within that class (not just exactly at boundary)
		prev := uint32(0)
		if sci > 0 {
			prev = SizeClasses[sci-1]
		}
		lo, hi := prev+1, SizeClasses[sci]
		return lo + uint32(rng.Intn(int(hi-lo+1)))
	}
	// jumbo: JumboThreshold+1 .. JumboThreshold+3*PageSize/4
	return JumboThreshold + 1 + uint32(rng.Intn(int(PageSize*3/4)))
}

// TestRandomAllocFreeShrinksToZero allocates records of random sizes, verifies
// each read-back, then releases all and confirms the store is completely empty.
func TestRandomAllocFreeShrinksToZero(t *testing.T) {
	s, dir := openStore(t)
	rng := rand.New(rand.NewSource(42))

	const N = 5_000
	type entry struct {
		ref  SlotRef
		size uint32
		tag  byte
	}
	entries := make([]entry, N)

	// Phase 1: alloc with random sizes and write a tag byte.
	for i := 0; i < N; i++ {
		sz := randSize(rng)
		ref, buf, err := s.Alloc(sz)
		if err != nil {
			t.Fatalf("Alloc(%d) at i=%d: %v", sz, i, err)
		}
		tag := byte(i ^ int(sz))
		buf[0] = tag
		buf[len(buf)-1] = tag
		entries[i] = entry{ref, sz, tag}
	}
	t.Logf("after alloc: pages=%d live=%d total=%d",
		s.Stats().Pages, s.Stats().LiveBytes, s.Stats().TotalBytes)

	// Phase 2: verify every record.
	for i, e := range entries {
		data, err := s.Read(e.ref)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if data[0] != e.tag || data[len(data)-1] != e.tag {
			t.Fatalf("record %d size=%d: tag mismatch got [%d..%d] want %d",
				i, e.size, data[0], data[len(data)-1], e.tag)
		}
	}

	// Phase 3: release in random order.
	perm := rng.Perm(N)
	for _, idx := range perm {
		if err := s.Release(entries[idx].ref); err != nil {
			t.Fatalf("Release %d: %v", idx, err)
		}
	}

	st := s.Stats()
	t.Logf("after release: pages=%d live=%d total=%d", st.Pages, st.LiveBytes, st.TotalBytes)

	if st.Pages != 0 {
		t.Fatalf("pages=%d after full release, want 0", st.Pages)
	}
	if st.LiveBytes != 0 {
		t.Fatalf("LiveBytes=%d, want 0", st.LiveBytes)
	}
	if files := pageFiles(t, dir); len(files) != 0 {
		t.Fatalf("%d page files remain on disk", len(files))
	}
}

// randSmallSize returns a random size in the smallest 7 size classes (64B–4K).
// Used for peak-memory tests where we want many records per page so that the
// page count bound is meaningful and predictable.
func randSmallSize(rng *rand.Rand) uint32 {
	sci := rng.Intn(7) // sci 0-6: 64, 128, 256, 512, 1K, 2K, 4K
	prev := uint32(0)
	if sci > 0 {
		prev = SizeClasses[sci-1]
	}
	lo, hi := prev+1, SizeClasses[sci]
	return lo + uint32(rng.Intn(int(hi-lo+1)))
}

// TestSlidingWindowPeakMemory keeps a fixed-size window of live records.
// For every new alloc it frees the oldest record.  This models a queue that
// is constantly producing and consuming.  The store should not accumulate pages
// beyond what is needed for the window — peak must stay bounded.
func TestSlidingWindowPeakMemory(t *testing.T) {
	s, dir := openStore(t)
	rng := rand.New(rand.NewSource(7))

	const window = 500   // live records at any time
	const total = 20_000 // total alloc/free cycles

	queue := make([]SlotRef, 0, window)
	var peakPages int

	for i := 0; i < total; i++ {
		// Use small sizes (up to 4K) so pages hold many records and the
		// page count is a meaningful bound on peak memory.
		sz := randSmallSize(rng)
		ref, buf, err := s.Alloc(sz)
		if err != nil {
			t.Fatalf("Alloc %d: %v", i, err)
		}
		buf[0] = byte(i)
		queue = append(queue, ref)

		// Free the oldest record once the window is full.
		if len(queue) > window {
			if err := s.Release(queue[0]); err != nil {
				t.Fatalf("Release oldest: %v", err)
			}
			queue = queue[1:]
		}

		if p := s.Stats().Pages; p > peakPages {
			peakPages = p
		}
	}

	// Drain the remaining window.
	for _, ref := range queue {
		s.Release(ref)
	}

	st := s.Stats()
	t.Logf("peak pages=%d  final pages=%d live=%d", peakPages, st.Pages, st.LiveBytes)

	if st.Pages != 0 {
		t.Fatalf("pages=%d after drain, want 0", st.Pages)
	}
	if st.LiveBytes != 0 {
		t.Fatalf("LiveBytes=%d, want 0", st.LiveBytes)
	}
	if files := pageFiles(t, dir); len(files) != 0 {
		t.Fatalf("page files remain: %v", files)
	}

	// Sanity: peak page count should be modest (not proportional to total).
	// With window=500 records of mixed sizes, a generous upper bound is 30 pages.
	if peakPages > 30 {
		t.Errorf("peak pages=%d seems high (window=%d records)", peakPages, window)
	}
}

// TestFragmentationRecovery fills many pages, then releases records in random
// order.  Each page must be reclaimed exactly when its last slot is freed —
// fragmented pages must not linger.
func TestFragmentationRecovery(t *testing.T) {
	s, dir := openStore(t)
	rng := rand.New(rand.NewSource(99))

	// Use a single size class so pages pack tightly and we can predict capacity.
	sci := 2 // 256-byte slots
	sz := SizeClasses[sci]
	h := NewSlabHeader(0, uint8(sci))
	cap := int(h.Capacity)
	pages := 10
	N := cap * pages // exactly fill 10 pages

	refs := make([]SlotRef, N)
	for i := range refs {
		ref, buf, err := s.Alloc(sz)
		if err != nil {
			t.Fatalf("Alloc %d: %v", i, err)
		}
		buf[0] = byte(i % 251)
		refs[i] = ref
	}

	pagesAfterAlloc := s.Stats().Pages
	t.Logf("after alloc: %d records across %d pages (expected ~%d)",
		N, pagesAfterAlloc, pages)

	// Shuffle release order to create fragmentation on every page.
	perm := rng.Perm(N)

	var prevPages int
	reclaimEvents := 0
	for step, idx := range perm {
		before := s.Stats().Pages
		if err := s.Release(refs[idx]); err != nil {
			t.Fatalf("Release %d: %v", step, err)
		}
		after := s.Stats().Pages
		if after < before {
			reclaimEvents++
		}
		if p := s.Stats().Pages; step == 0 {
			prevPages = p
		} else if p > prevPages {
			t.Errorf("step %d: pages grew from %d to %d (no allocs should cause growth)",
				step, prevPages, p)
		}
		prevPages = s.Stats().Pages
	}

	st := s.Stats()
	t.Logf("reclaim events=%d  final: pages=%d live=%d", reclaimEvents, st.Pages, st.LiveBytes)

	if st.Pages != 0 {
		t.Fatalf("pages=%d after full release, want 0", st.Pages)
	}
	if files := pageFiles(t, dir); len(files) != 0 {
		t.Fatalf("page files remain: %v", files)
	}
	if reclaimEvents != pages {
		t.Errorf("expected %d reclaim events (one per page), got %d", pages, reclaimEvents)
	}
}

// suppress unused import for os (used by pageFiles indirectly via filepath)
var _ = os.DevNull
