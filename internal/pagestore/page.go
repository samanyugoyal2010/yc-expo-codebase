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
	"math/bits"
	"os"
	"sync/atomic"
	"unsafe"
)

// page is the runtime handle for one physical page file (slab or jumbo span).
//
// Durable state lives in data via mmap: header at [0:HeaderSize], bitmap at
// [HeaderSize:slotsStart] (slab only), payload at [slotsStart:].
// Volatile state (freeCount, hint) is rebuilt on recovery — never written to disk.
type page struct {
	file       *os.File
	data       []byte // full mmap — for jumbo: N × PageSize contiguous
	pageID     uint32
	jumbo      bool
	sizeClass  uint32 // slot size in bytes; 0 for jumbo
	spanPages  uint32
	capacity   uint32 // total slot count; 0 for jumbo
	slotsStart uint32 // byte offset of first slot

	// Volatile — set by initBitmap (new page) or rebuildFreeCount (recovery).
	freeCount atomic.Int32  // free slot count; slab only
	hint      atomic.Uint32 // bitmap word index to start next scan from
}

// gen reads the generation field from the in-memory header (bytes [8:12], little-endian).
func (p *page) gen() uint32 {
	return uint32(p.data[8]) | uint32(p.data[9])<<8 |
		uint32(p.data[10])<<16 | uint32(p.data[11])<<24
}

// hasFreeSlot reports whether at least one slot is available (slab only).
func (p *page) hasFreeSlot() bool { return p.freeCount.Load() > 0 }

// numWords returns the number of uint64 words in the allocation bitmap.
func (p *page) numWords() int { return int((p.capacity + 63) / 64) }

// bitmapWord returns an atomic-safe pointer to the ith uint64 word of the bitmap.
// The bitmap starts at data[HeaderSize] which is 8-byte aligned (HeaderSize = 32).
func (p *page) bitmapWord(i int) *uint64 {
	return (*uint64)(unsafe.Pointer(&p.data[HeaderSize+i*8]))
}

// initBitmap zeroes all bitmap words (all slots free) and initialises freeCount.
// Called once immediately after a new slab page is created.
func (p *page) initBitmap() {
	for i := 0; i < p.numWords(); i++ {
		atomic.StoreUint64(p.bitmapWord(i), 0)
	}
	p.freeCount.Store(int32(p.capacity))
}

// rebuildFreeCount counts the free bits (0s) in the bitmap and stores the result.
// Called during recovery after RebuildVolatile has stamped the live-slot bits.
func (p *page) rebuildFreeCount() {
	nw := p.numWords()
	var free int32
	for i := 0; i < nw; i++ {
		w := atomic.LoadUint64(p.bitmapWord(i))
		if i == nw-1 {
			if rem := p.capacity % 64; rem != 0 {
				w |= ^((uint64(1) << rem) - 1) // mark out-of-range bits as allocated
			}
		}
		free += int32(bits.OnesCount64(^w))
	}
	p.freeCount.Store(free)
}

// allocSlot finds the first free bit (0), atomically sets it (1), and returns
// the slot index.  Returns (0, false) when the page is full.
func (p *page) allocSlot() (uint32, bool) {
	nw := p.numWords()
	if nw == 0 {
		return 0, false
	}
	start := int(p.hint.Load()) % nw
	for i := 0; i < nw; i++ {
		wi := (start + i) % nw
		wp := p.bitmapWord(wi)
		for {
			w := atomic.LoadUint64(wp)
			free := ^w
			if wi == nw-1 {
				if rem := p.capacity % 64; rem != 0 {
					free &= (uint64(1) << rem) - 1 // ignore out-of-range bits
				}
			}
			if free == 0 {
				break // word is full; try next
			}
			bit := bits.TrailingZeros64(free)
			if atomic.CompareAndSwapUint64(wp, w, w|(uint64(1)<<bit)) {
				p.freeCount.Add(-1)
				p.hint.Store(uint32(wi))
				return uint32(wi*64 + bit), true
			}
			// CAS lost to another goroutine — retry the same word
		}
	}
	return 0, false
}

// freeSlot atomically clears the bit for slotIdx.
// Returns true if the page is now fully free (freeCount == capacity).
func (p *page) freeSlot(slotIdx uint32) bool {
	wi := int(slotIdx / 64)
	mask := uint64(1) << (slotIdx % 64)
	wp := p.bitmapWord(wi)
	for {
		w := atomic.LoadUint64(wp)
		if atomic.CompareAndSwapUint64(wp, w, w&^mask) {
			break
		}
	}
	return p.freeCount.Add(1) == int32(p.capacity)
}

// slotOffset converts a slot index to its byte offset within data.
func (p *page) slotOffset(slotIdx uint32) uint32 {
	return p.slotsStart + slotIdx*p.sizeClass
}
