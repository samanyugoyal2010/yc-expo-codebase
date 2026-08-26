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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
)

// Stats is a point-in-time snapshot of page-store utilisation.
type Stats struct {
	Pages      int
	LiveBytes  int64
	TotalBytes int64
}

// Store is the shared payload allocator — one instance per process, shared
// across all queues.
type Store struct {
	dir string
	jnl *journal

	mu     sync.Mutex
	pgs    map[uint32]*page
	nextID uint32

	// active[i] is a pageID hint for size class i that has free slots.
	// 0 means no hint; Alloc falls through to creating a new page.
	active [len(SizeClasses)]uint32
}

// Open opens or creates the page store at dir.
// The store.log journal is replayed to rebuild the page inventory and header
// generations.  Volatile state (bitmap, freeCount) is NOT rebuilt here — call
// RebuildVolatile after all queue WALs have been replayed.
func Open(dir string) (*Store, error) {
	for _, sub := range []string{"pages", "pagestore"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	jnl, err := openJournal(filepath.Join(dir, "pagestore", "store.log"))
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, jnl: jnl, pgs: make(map[uint32]*page), nextID: 1}
	if err := s.replayJournal(); err != nil {
		jnl.close()
		return nil, err
	}
	return s, nil
}

// Close fsyncs and unmaps all pages, then closes the journal.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for _, pg := range s.pgs {
		errs = append(errs, pg.file.Sync(), syscall.Munmap(pg.data), pg.file.Close())
	}
	s.pgs = make(map[uint32]*page)
	return errors.Join(append(errs, s.jnl.close())...)
}

// Alloc reserves size bytes and returns a SlotRef plus a writable mmap-backed
// slice covering the entire aligned slot (≥ size, rounded up to the size class).
// Write the payload into the slice, then call Sync before appending the WAL
// record (payload must be durable first — invariant §6.1).
// The caller is responsible for recording payloadLen = size alongside the
// SlotRef so that Read can return the right number of bytes.
func (s *Store) Alloc(size uint32) (SlotRef, []byte, error) {
	if size == 0 {
		return 0, nil, fmt.Errorf("pagestore: Alloc(0)")
	}
	if size > JumboThreshold {
		return s.newJumboPage(size)
	}
	return s.allocSlab(size)
}

// Read returns the full aligned slot for ref — zero-copy mmap slice.
// The slice covers the entire size-class slot; callers trim to their payloadLen.
// Returns an error if the page has been reclaimed (ref is stale).
func (s *Store) Read(ref SlotRef) ([]byte, error) {
	s.mu.Lock()
	pg, ok := s.pgs[ref.PageID()]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("pagestore: page %d not found or reclaimed", ref.PageID())
	}
	off := ref.Offset()
	var end uint32
	if pg.jumbo {
		end = uint32(len(pg.data))
	} else {
		end = off + pg.sizeClass
	}
	if end > uint32(len(pg.data)) {
		return nil, fmt.Errorf("pagestore: ref offset %d out of bounds", off)
	}
	return pg.data[off:end], nil
}

// Release returns a slot to the page.
// s.mu is held for the entire critical section so that the bitmap op and the
// pgs-delete happen atomically — no other goroutine can interleave an alloc
// or a concurrent reclaim on the same page between those two steps.
func (s *Store) Release(ref SlotRef) error {
	pgID := ref.PageID()
	s.mu.Lock()
	pg, ok := s.pgs[pgID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("pagestore: page %d not found", pgID)
	}

	var reclaimPg *page

	if pg.jumbo {
		delete(s.pgs, pgID)
		reclaimPg = pg
	} else {
		slotIdx := (ref.Offset() - pg.slotsStart) / pg.sizeClass
		if pg.freeSlot(slotIdx) {
			delete(s.pgs, pgID)
			sci := SizeClassFor(pg.sizeClass)
			if sci >= 0 && s.active[sci] == pgID {
				s.active[sci] = 0
			}
			reclaimPg = pg
		} else {
			sci := SizeClassFor(pg.sizeClass)
			if sci >= 0 && s.active[sci] == 0 {
				s.active[sci] = pgID
			}
		}
	}
	s.mu.Unlock()

	if reclaimPg != nil {
		s.doReclaim(pgID, reclaimPg)
	}
	return nil
}

// Sync fsyncs the page files that back the given refs.
// Must be called after writing payload into the mmap slice and before
// appending the WAL record.
func (s *Store) Sync(refs []SlotRef) error {
	seen := make(map[uint32]bool, len(refs))
	for _, r := range refs {
		pgID := r.PageID()
		if seen[pgID] {
			continue
		}
		seen[pgID] = true
		s.mu.Lock()
		pg, ok := s.pgs[pgID]
		s.mu.Unlock()
		if ok {
			if err := pg.file.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

// RebuildVolatile reconstructs the bitmap and freeCount for every page from
// the set of live refs collected by replaying all queue WALs.
// This is recovery Phase 2 and must complete before serving traffic.
func (s *Store) RebuildVolatile(liveRefs []SlotRef) error {
	byPage := make(map[uint32][]SlotRef, len(liveRefs))
	for _, r := range liveRefs {
		pgID := r.PageID()
		byPage[pgID] = append(byPage[pgID], r)
	}

	s.mu.Lock()

	var orphanJumbo []uint32
	for pageID, pg := range s.pgs {
		if pg.jumbo {
			if len(byPage[pageID]) == 0 {
				orphanJumbo = append(orphanJumbo, pageID)
			}
			continue
		}
		for i := 0; i < pg.numWords(); i++ {
			atomic.StoreUint64(pg.bitmapWord(i), 0)
		}
		for _, r := range byPage[pageID] {
			slotIdx := (r.Offset() - pg.slotsStart) / pg.sizeClass
			wi := int(slotIdx / 64)
			old := atomic.LoadUint64(pg.bitmapWord(wi))
			atomic.StoreUint64(pg.bitmapWord(wi), old|(uint64(1)<<(slotIdx%64)))
		}
		pg.rebuildFreeCount()
	}

	// Rebuild per-class active-page hints.
	for i := range s.active {
		s.active[i] = 0
		for pgID, pg := range s.pgs {
			if !pg.jumbo && SizeClassFor(pg.sizeClass) == i && pg.hasFreeSlot() {
				s.active[i] = pgID
				break
			}
		}
	}
	s.mu.Unlock()

	for _, pgID := range orphanJumbo {
		s.mu.Lock()
		pg := s.pgs[pgID]
		if pg != nil {
			delete(s.pgs, pgID)
		}
		s.mu.Unlock()
		if pg != nil {
			s.doReclaim(pgID, pg)
		}
	}
	return nil
}

// Stats returns a snapshot of utilisation across all pages.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st Stats
	for _, pg := range s.pgs {
		st.Pages++
		total := int64(len(pg.data))
		st.TotalBytes += total
		if pg.jumbo {
			st.LiveBytes += total - HeaderSize
		} else {
			live := int32(pg.capacity) - pg.freeCount.Load()
			st.LiveBytes += int64(live) * int64(pg.sizeClass)
		}
	}
	return st
}

// ─── internal ─────────────────────────────────────────────────────────────────

func (s *Store) pagePath(id uint32) string {
	return filepath.Join(s.dir, "pages", fmt.Sprintf("%09d.page", id))
}

func (s *Store) allocSlab(size uint32) (SlotRef, []byte, error) {
	sci := SizeClassFor(size)

	s.mu.Lock()
	defer s.mu.Unlock()

	if pgID := s.active[sci]; pgID != 0 {
		pg := s.pgs[pgID]
		if pg == nil {
			s.active[sci] = 0
		} else if slotIdx, ok := pg.allocSlot(); ok {
			off := pg.slotOffset(slotIdx)
			return Pack(pgID, off), pg.data[off : off+pg.sizeClass], nil
		} else {
			s.active[sci] = 0
		}
	}

	pg, pgID, err := s.newSlabPage(sci)
	if err != nil {
		return 0, nil, err
	}
	s.active[sci] = pgID

	slotIdx, _ := pg.allocSlot()
	off := pg.slotOffset(slotIdx)
	return Pack(pgID, off), pg.data[off : off+pg.sizeClass], nil
}

// newSlabPage creates a 1-MiB slab page for size class sci.
// Caller must hold s.mu.
func (s *Store) newSlabPage(sci int) (*page, uint32, error) {
	pgID := s.nextID
	s.nextID++

	path := s.pagePath(pgID)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, 0, err
	}
	if err := f.Truncate(PageSize); err != nil {
		f.Close(); os.Remove(path)
		return nil, 0, err
	}
	if err := s.jnl.appendPageCreate(pgID, uint16(sci), 1, 0); err != nil {
		f.Close(); os.Remove(path)
		return nil, 0, err
	}
	data, err := mmapFile(f, PageSize)
	if err != nil {
		f.Close()
		return nil, 0, err
	}

	h := NewSlabHeader(pgID, uint8(sci))
	WriteHeader(data, h)

	pg := &page{
		file:       f,
		data:       data,
		pageID:     pgID,
		sizeClass:  h.SlotSize(),
		spanPages:  1,
		capacity:   h.Capacity,
		slotsStart: h.SlotsStart,
	}
	pg.initBitmap()
	s.pgs[pgID] = pg
	return pg, pgID, nil
}

// newJumboPage allocates a contiguous N × 1-MiB span for a single large payload.
func (s *Store) newJumboPage(size uint32) (SlotRef, []byte, error) {
	spanPages := SpanPagesFor(size)
	totalSize := int(spanPages) * PageSize

	s.mu.Lock()
	pgID := s.nextID
	s.nextID++
	s.mu.Unlock()

	path := s.pagePath(pgID)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return 0, nil, err
	}
	if err := f.Truncate(int64(totalSize)); err != nil {
		f.Close(); os.Remove(path)
		return 0, nil, err
	}
	if err := s.jnl.appendPageCreate(pgID, 0xFFFF, uint32(spanPages), 0); err != nil {
		f.Close(); os.Remove(path)
		return 0, nil, err
	}
	data, err := mmapFile(f, totalSize)
	if err != nil {
		f.Close()
		return 0, nil, err
	}

	h := NewJumboHeader(pgID, spanPages)
	WriteHeader(data, h)

	pg := &page{
		file:       f,
		data:       data,
		pageID:     pgID,
		jumbo:      true,
		spanPages:  uint32(spanPages),
		slotsStart: HeaderSize,
	}

	s.mu.Lock()
	s.pgs[pgID] = pg
	s.mu.Unlock()

	return Pack(pgID, HeaderSize), data[HeaderSize:], nil
}

// doReclaim journals PAGE_RETIRE, bumps the generation, unmaps and deletes
// the page file.  Called after the page has been removed from s.pgs under
// s.mu, so no other goroutine can reach pg.data — no lock needed here.
func (s *Store) doReclaim(pageID uint32, pg *page) {
	oldGen := pg.gen()
	_ = s.jnl.appendPageRetire(pageID, oldGen, oldGen+1)
	BumpGen(pg.data)
	syscall.Munmap(pg.data)
	pg.file.Close()
	os.Remove(s.pagePath(pageID))
}

func (s *Store) replayJournal() error {
	records, err := s.jnl.replay()
	if err != nil {
		return err
	}
	for _, rec := range records {
		switch rec.op {
		case jrnlPageCreate:
			err := s.mmapExisting(rec.pageID, rec.scIdx, rec.spanPages)
			if os.IsNotExist(err) {
				continue // file was deleted by a completed retire before the crash
			}
			if err != nil {
				return fmt.Errorf("replay PAGE_CREATE %d: %w", rec.pageID, err)
			}
			if s.nextID <= rec.pageID {
				s.nextID = rec.pageID + 1
			}

		case jrnlPageRetire:
			pg, ok := s.pgs[rec.pageID]
			if !ok {
				continue // file already deleted; nothing to do
			}
			SetGen(pg.data, rec.newGen)
			syscall.Munmap(pg.data)
			pg.file.Close()
			os.Remove(s.pagePath(rec.pageID))
			delete(s.pgs, rec.pageID)
		}
	}
	return nil
}

func (s *Store) mmapExisting(pageID uint32, scIdx uint16, spanPages uint32) error {
	f, err := os.OpenFile(s.pagePath(pageID), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	data, err := mmapFile(f, int(spanPages)*PageSize)
	if err != nil {
		f.Close()
		return err
	}
	h, err := ReadHeader(data)
	if err != nil {
		f.Close(); syscall.Munmap(data)
		return fmt.Errorf("page %d: %w", pageID, err)
	}
	s.pgs[pageID] = &page{
		file:       f,
		data:       data,
		pageID:     pageID,
		jumbo:      h.IsJumbo(),
		sizeClass:  h.SlotSize(),
		spanPages:  uint32(h.SpanPages),
		capacity:   h.Capacity,
		slotsStart: h.SlotsStart,
	}
	return nil
}

func mmapFile(f *os.File, size int) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}
