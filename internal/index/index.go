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

// Package index implements the per-queue message index.
//
// Index slots are allocated from the shared page store (64-byte slab class).
// Each slot's address in the page store IS its identifier (a SlotRef).
// The WAL ENQUEUE record carries both the index SlotRef and the payload SlotRef,
// so recovery is a pure WAL fold — no separate index.dat file is required for
// correctness, though we still checkpoint to index.dat for fast startup.
//
// On-disk layout (index.dat):
//
//	[Header: 16 bytes]
//	  checkpointLSN  u64
//	  count          u32  (number of live entries)
//	  reserved       u32
//	[Entry × count: 72 bytes each]
//	  ref            u64   (SlotRef — page store address of this index slot)
//	  slot data      64 bytes
package index

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/samanyugoyal2010/frankenqueue/internal/pagestore"
)

// Slot states.
const (
	StateDelayed  = uint8(1)
	StateReady    = uint8(2)
	StateInflight = uint8(3)
	StateAcked    = uint8(4)
	StateDead     = uint8(5)
)

const (
	entrySize  = 72 // ref(8) + slotData(64)
	headerSize = 16
	slotCRCOff = 56 // CRC32 over slot bytes [0:56]
)

// Slot is the in-memory record for one message.
// PayloadRef is the page-store address of the payload (SlotRef as uint64).
// PayloadLen is the exact byte count the caller wrote into that slot.
//
// Wire layout within a checkpoint entry (bytes 8..63, after the 8-byte ref):
//
//	[0:8]   MsgID          u64
//	[8:16]  EnqueuedAtMs   i64
//	[16:24] AvailableAtMs  i64
//	[24:32] LeaseUntilMs   i64   (0 = not leased)
//	[32:40] PayloadRef     u64   (pagestore.SlotRef)
//	[40:44] PayloadLen     u32
//	[44]    Priority       u8
//	[45]    State          u8
//	[46:48] Attempts       u16
//	[48:56] LeaseNonce     u64   (0 = no durable lease)
//	[56:60] CRC32          u32   (over bytes [0:56])
//	[60:64] reserved       u32
type Slot struct {
	MsgID         uint64
	EnqueuedAtMs  int64
	AvailableAtMs int64
	LeaseUntilMs  int64
	PayloadRef    uint64 // pagestore.SlotRef
	PayloadLen    uint32
	Priority      uint8
	State         uint8
	Attempts      uint16
	LeaseNonce    uint64
}

// Index is the in-memory message index backed by the page store.
// All mutations happen in memory; Checkpoint flushes a snapshot to index.dat.
type Index struct {
	pages *pagestore.Store

	mu            sync.RWMutex
	path          string
	live          map[uint64]entry // SlotRef(as uint64) → entry
	checkpointLSN uint64
}

type entry struct {
	ref  uint64 // SlotRef (index slot address in page store)
	slot Slot
}

// Open opens or creates the index for the queue at dir.
// pages is used to allocate/free index slots in the page store.
// If index.dat is missing or corrupt, an empty index is returned (WAL replay from 0).
func Open(dir string, pages *pagestore.Store) (*Index, error) {
	path := filepath.Join(dir, "index.dat")
	idx := &Index{
		pages: pages,
		path:  path,
		live:  make(map[uint64]entry),
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return idx, nil
		}
		return nil, err
	}
	if err := idx.load(data); err != nil {
		idx.live = make(map[uint64]entry) // corrupt checkpoint → full WAL replay
		return idx, nil
	}
	return idx, nil
}

// CheckpointLSN returns the LSN at which the index was last checkpointed.
func (idx *Index) CheckpointLSN() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.checkpointLSN
}

// AllocSlot allocates a 64-byte index slot from the page store and returns its
// SlotRef encoded as a uint64.  The slot is written via Put before use.
func (idx *Index) AllocSlot() (uint64, error) {
	ref, _, err := idx.pages.Alloc(pagestore.SizeClasses[0])
	if err != nil {
		return 0, err
	}
	return uint64(ref), nil
}

// Put records slot under ref (a SlotRef as uint64) in the in-memory index.
func (idx *Index) Put(ref uint64, s Slot) {
	idx.mu.Lock()
	idx.live[ref] = entry{ref: ref, slot: s}
	idx.mu.Unlock()
}

// GetByRef returns the slot for the given SlotRef (as uint64).
func (idx *Index) GetByRef(ref uint64) (Slot, bool) {
	idx.mu.RLock()
	e, ok := idx.live[ref]
	idx.mu.RUnlock()
	return e.slot, ok
}

// FreeSlot removes the slot from the in-memory index and releases the page
// store slot back to the allocator.
func (idx *Index) FreeSlot(ref uint64) {
	idx.mu.Lock()
	delete(idx.live, ref)
	idx.mu.Unlock()
	idx.pages.Release(pagestore.SlotRef(ref))
}

// ScanPayloadPage calls fn for every live slot whose PayloadRef belongs to pageID.
func (idx *Index) ScanPayloadPage(pageID uint32, fn func(ref uint64, s Slot)) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for ref, e := range idx.live {
		if pagestore.SlotRef(e.slot.PayloadRef).PageID() == pageID {
			fn(ref, e.slot)
		}
	}
}

// Scan calls fn for every live slot.
func (idx *Index) Scan(fn func(ref uint64, s Slot)) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for ref, e := range idx.live {
		fn(ref, e.slot)
	}
}

// Len returns the number of live slots.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.live)
}

// Checkpoint atomically writes the current index to index.dat.
// lsn is the WAL LSN up to which this checkpoint is valid.
func (idx *Index) Checkpoint(lsn uint64) error {
	idx.mu.Lock()
	entries := make([]entry, 0, len(idx.live))
	for _, e := range idx.live {
		entries = append(entries, e)
	}
	idx.checkpointLSN = lsn
	idx.mu.Unlock()

	data := encode(lsn, entries)
	dir := filepath.Dir(idx.path)
	tmp := idx.path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close(); os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close(); os.Remove(tmp)
		return err
	}
	f.Close()
	if err := os.Rename(tmp, idx.path); err != nil {
		os.Remove(tmp)
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// --- internal ----------------------------------------------------------------

func encode(lsn uint64, entries []entry) []byte {
	buf := make([]byte, headerSize+len(entries)*entrySize)
	binary.LittleEndian.PutUint64(buf[0:8], lsn)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(entries)))
	for i, e := range entries {
		off := headerSize + i*entrySize
		binary.LittleEndian.PutUint64(buf[off:off+8], e.ref)
		encodeSlot(buf[off+8:off+entrySize], e.slot)
	}
	return buf
}

func (idx *Index) load(data []byte) error {
	if len(data) < headerSize {
		return fmt.Errorf("index: file too short")
	}
	lsn := binary.LittleEndian.Uint64(data[0:8])
	count := int(binary.LittleEndian.Uint32(data[8:12]))
	if len(data) != headerSize+count*entrySize {
		return fmt.Errorf("index: size mismatch: got %d want %d", len(data), headerSize+count*entrySize)
	}
	live := make(map[uint64]entry, count)
	for i := 0; i < count; i++ {
		off := headerSize + i*entrySize
		raw := data[off : off+entrySize]
		ref := binary.LittleEndian.Uint64(raw[0:8])
		slotRaw := raw[8:entrySize]
		computed := crc32.ChecksumIEEE(slotRaw[:slotCRCOff])
		stored := binary.LittleEndian.Uint32(slotRaw[slotCRCOff : slotCRCOff+4])
		if computed != stored {
			return fmt.Errorf("index: entry %d CRC mismatch", i)
		}
		s := decodeSlot(slotRaw)
		live[ref] = entry{ref: ref, slot: s}
	}
	idx.live = live
	idx.checkpointLSN = lsn
	return nil
}

func encodeSlot(b []byte, s Slot) {
	binary.LittleEndian.PutUint64(b[0:8], s.MsgID)
	binary.LittleEndian.PutUint64(b[8:16], uint64(s.EnqueuedAtMs))
	binary.LittleEndian.PutUint64(b[16:24], uint64(s.AvailableAtMs))
	binary.LittleEndian.PutUint64(b[24:32], uint64(s.LeaseUntilMs))
	binary.LittleEndian.PutUint64(b[32:40], s.PayloadRef)
	binary.LittleEndian.PutUint32(b[40:44], s.PayloadLen)
	b[44] = s.Priority
	b[45] = s.State
	binary.LittleEndian.PutUint16(b[46:48], s.Attempts)
	binary.LittleEndian.PutUint64(b[48:56], s.LeaseNonce)
	binary.LittleEndian.PutUint32(b[56:60], crc32.ChecksumIEEE(b[:56]))
	// [60:64] reserved
}

func decodeSlot(b []byte) Slot {
	return Slot{
		MsgID:         binary.LittleEndian.Uint64(b[0:8]),
		EnqueuedAtMs:  int64(binary.LittleEndian.Uint64(b[8:16])),
		AvailableAtMs: int64(binary.LittleEndian.Uint64(b[16:24])),
		LeaseUntilMs:  int64(binary.LittleEndian.Uint64(b[24:32])),
		PayloadRef:    binary.LittleEndian.Uint64(b[32:40]),
		PayloadLen:    binary.LittleEndian.Uint32(b[40:44]),
		Priority:      b[44],
		State:         b[45],
		Attempts:      binary.LittleEndian.Uint16(b[46:48]),
		LeaseNonce:    binary.LittleEndian.Uint64(b[48:56]),
	}
}
