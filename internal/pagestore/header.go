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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"
)

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	Magic          = uint32(0xF4A3_5E1B) // written to every page, validated on open
	HeaderSize     = 32                  // bytes; one cache line, uniform for all page types
	PageSize       = 1 << 20            // 1 MiB per slab page
	JumboThreshold = PageSize / 2       // payloads larger than this get a jumbo page
	ScIdxJumbo     = uint8(0xFF)        // sentinel: not a slab, this is a jumbo page
)

// ─── Size Classes ─────────────────────────────────────────────────────────────

// SizeClasses lists every slab slot size in ascending order.
// Alloc rounds a requested size up to the smallest class that fits.
// Minimum is 8 bytes (enough for any bitmap pointer / future use).
var SizeClasses = [14]uint32{
	64, 128, 256, 512,
	1_024, 2_048, 4_096, 8_192,
	16_384, 32_768, 65_536, 131_072, 262_144, 524_288,
}

// SizeClassFor returns the index into SizeClasses for a given byte count,
// or -1 if size exceeds the largest class (caller must use the jumbo path).
func SizeClassFor(n uint32) int {
	for i, sc := range SizeClasses {
		if n <= sc {
			return i
		}
	}
	return -1
}

// ─── Page Flags ───────────────────────────────────────────────────────────────

// PageFlags is a bit-field packed into one byte of the page header.
// Each flag is a distinct power-of-two constant; combine with | and test with &.
type PageFlags uint8

const (
	// FlagJumbo marks a multi-MiB span allocated as a single contiguous mmap.
	// The header lives only on the first 1-MiB block; the rest is raw payload.
	FlagJumbo PageFlags = 1 << 0

	// FlagCompressed indicates the payload region is compressed (reserved for
	// future use; codec id will live in the reserved header bytes).
	FlagCompressed PageFlags = 1 << 1

	// FlagSealed prevents new allocations on this page (set before compaction
	// begins so the page drains naturally without accepting new messages).
	FlagSealed PageFlags = 1 << 2

	// FlagRetired is set atomically when freeCount reaches capacity.
	// A retired page is unmapped and its file deleted; any Ref with the old
	// generation will fail the CRC check and return an error.
	FlagRetired PageFlags = 1 << 3

	// bits 4-7: reserved for future use
)

// Has reports whether all bits in mask are set.
func (f PageFlags) Has(mask PageFlags) bool { return f&mask == mask }

// String returns a human-readable representation for logging / debugging.
func (f PageFlags) String() string {
	if f == 0 {
		return "none"
	}
	names := [...]struct {
		bit  PageFlags
		name string
	}{
		{FlagJumbo, "jumbo"},
		{FlagCompressed, "compressed"},
		{FlagSealed, "sealed"},
		{FlagRetired, "retired"},
	}
	out := ""
	for _, n := range names {
		if f.Has(n.bit) {
			if out != "" {
				out += "|"
			}
			out += n.name
		}
	}
	return out
}

// ─── Header ───────────────────────────────────────────────────────────────────

// Header is the decoded, in-memory view of the 32-byte durable page header.
// It is written once at page creation and re-written only when gen is bumped
// (page retire). All fields are little-endian on disk.
//
// Wire layout (all little-endian):
//
//	[0:4]   magic      uint32   must equal Magic
//	[4:8]   pageID     uint32   immutable after creation
//	[8:12]  gen        uint32   bumped on every retire+reuse cycle
//	[12]    scIdx      uint8    size-class index or ScIdxJumbo (0xFF)
//	[13]    flags      uint8    PageFlags bit-field
//	[14:16] spanPages  uint16   1 for slab; N for jumbo (N × 1 MiB allocated)
//	[16:20] capacity   uint32   total slot count (slab); 0 (jumbo)
//	[20:24] freeCount  uint32   free slot count  (slab); 0 (jumbo)
//	[24:28] slotsStart uint32   byte offset of first slot (slab); HeaderSize (jumbo)
//	[28:32] headerCRC  uint32   CRC32/IEEE of bytes [0:28]
type Header struct {
	Magic      uint32
	PageID     uint32
	Gen        uint32
	ScIdx      uint8     // 0x00–0x0D = size class, 0xFF = jumbo
	Flags      PageFlags // bit-field; use .Has() to test individual flags
	SpanPages  uint16    // number of contiguous 1-MiB blocks in this page
	Capacity   uint32    // slab: total slot count;   jumbo: 0
	FreeCount  uint32    // slab: free slot count;    jumbo: 0
	SlotsStart uint32    // slab: offset of slot[0];  jumbo: HeaderSize
}

// IsJumbo reports whether this page is a jumbo (multi-MiB) span.
func (h *Header) IsJumbo() bool { return h.Flags.Has(FlagJumbo) }

// SlotSize returns the slot size in bytes for slab pages, or 0 for jumbo.
func (h *Header) SlotSize() uint32 {
	if h.IsJumbo() {
		return 0
	}
	return SizeClasses[h.ScIdx]
}

// BitmapLen returns the number of uint64 words in the allocation bitmap.
// For jumbo pages this is always 0 (no bitmap needed).
func (h *Header) BitmapLen() int {
	if h.IsJumbo() || h.Capacity == 0 {
		return 0
	}
	return int((h.Capacity + 63) / 64) // ceil(capacity / 64)
}

// TotalSize returns the total byte size of the mapped region.
func (h *Header) TotalSize() int { return int(h.SpanPages) * PageSize }

// Validate checks magic, CRC, and internal consistency of the decoded header.
func (h *Header) Validate(raw []byte) error {
	if len(raw) < HeaderSize {
		return fmt.Errorf("header: buffer too small (%d < %d)", len(raw), HeaderSize)
	}
	if h.Magic != Magic {
		return fmt.Errorf("header: bad magic %08x (want %08x)", h.Magic, Magic)
	}
	computed := crc32.ChecksumIEEE(raw[:28])
	stored := binary.LittleEndian.Uint32(raw[28:32])
	if computed != stored {
		return fmt.Errorf("header: CRC mismatch (computed %08x, stored %08x)", computed, stored)
	}
	if !h.IsJumbo() && int(h.ScIdx) >= len(SizeClasses) {
		return fmt.Errorf("header: invalid scIdx %d", h.ScIdx)
	}
	if h.SpanPages == 0 {
		return fmt.Errorf("header: spanPages is 0")
	}
	return nil
}

// ─── Encode / Decode ──────────────────────────────────────────────────────────

// WriteHeader encodes h into the first HeaderSize bytes of dst and writes the
// CRC into [28:32].  dst must be at least HeaderSize bytes.
func WriteHeader(dst []byte, h Header) {
	binary.LittleEndian.PutUint32(dst[0:4], Magic)
	binary.LittleEndian.PutUint32(dst[4:8], h.PageID)
	binary.LittleEndian.PutUint32(dst[8:12], h.Gen)
	dst[12] = h.ScIdx
	dst[13] = uint8(h.Flags)
	binary.LittleEndian.PutUint16(dst[14:16], h.SpanPages)
	binary.LittleEndian.PutUint32(dst[16:20], h.Capacity)
	binary.LittleEndian.PutUint32(dst[20:24], h.FreeCount)
	binary.LittleEndian.PutUint32(dst[24:28], h.SlotsStart)
	binary.LittleEndian.PutUint32(dst[28:32], crc32.ChecksumIEEE(dst[:28]))
}

// ReadHeader decodes and validates the header from the first HeaderSize bytes
// of src.  Returns an error if magic or CRC does not match.
func ReadHeader(src []byte) (Header, error) {
	h := Header{
		Magic:      binary.LittleEndian.Uint32(src[0:4]),
		PageID:     binary.LittleEndian.Uint32(src[4:8]),
		Gen:        binary.LittleEndian.Uint32(src[8:12]),
		ScIdx:      src[12],
		Flags:      PageFlags(src[13]),
		SpanPages:  binary.LittleEndian.Uint16(src[14:16]),
		Capacity:   binary.LittleEndian.Uint32(src[16:20]),
		FreeCount:  binary.LittleEndian.Uint32(src[20:24]),
		SlotsStart: binary.LittleEndian.Uint32(src[24:28]),
	}
	return h, h.Validate(src)
}

// BumpGen increments the generation field and recalculates the CRC.
// Used when retiring a page so outstanding Refs with the old gen go stale.
func BumpGen(data []byte) uint32 {
	gen := binary.LittleEndian.Uint32(data[8:12]) + 1
	binary.LittleEndian.PutUint32(data[8:12], gen)
	binary.LittleEndian.PutUint32(data[28:32], crc32.ChecksumIEEE(data[:28]))
	return gen
}

// SetGen writes an exact generation value and recalculates the CRC.
// Used during journal replay where the target gen is recorded in the journal record.
func SetGen(data []byte, gen uint32) {
	binary.LittleEndian.PutUint32(data[8:12], gen)
	binary.LittleEndian.PutUint32(data[28:32], crc32.ChecksumIEEE(data[:28]))
}

// ─── Header constructors ──────────────────────────────────────────────────────

// NewSlabHeader builds a Header for a fresh slab page.
// slotsStart is computed from scIdx so the caller does not need to.
func NewSlabHeader(pageID uint32, scIdx uint8) Header {
	slotSize := SizeClasses[scIdx]
	capacity, slotsStart := slabLayout(slotSize)
	return Header{
		Magic:      Magic,
		PageID:     pageID,
		Gen:        0,
		ScIdx:      scIdx,
		Flags:      0,
		SpanPages:  1,
		Capacity:   capacity,
		FreeCount:  capacity, // all slots free at birth
		SlotsStart: slotsStart,
	}
}

// NewJumboHeader builds a Header for a fresh jumbo span.
// spanPages is the number of 1-MiB blocks needed (caller computes this).
func NewJumboHeader(pageID uint32, spanPages uint16) Header {
	return Header{
		Magic:      Magic,
		PageID:     pageID,
		Gen:        0,
		ScIdx:      ScIdxJumbo,
		Flags:      FlagJumbo,
		SpanPages:  spanPages,
		Capacity:   0,         // jumbo = one allocation per span
		FreeCount:  0,
		SlotsStart: HeaderSize, // payload starts right after header
	}
}

// ─── Layout helpers ───────────────────────────────────────────────────────────

// slabLayout returns the (capacity, slotsStart) for a given slot size.
//
// The bitmap occupies [HeaderSize : slotsStart).
// slotsStart is rounded up to slotSize so all slots are naturally aligned.
//
//	bitmapWords = ceil(capacity / 64)   — each uint64 covers 64 slots
//	slotsStart  = roundUp(HeaderSize + bitmapWords*8, slotSize)
//	capacity    = (PageSize - slotsStart) / slotSize
//
// This is non-recursive: one pass is sufficient because bitmap size grows
// much slower than slot capacity shrinks as slotSize increases.
func slabLayout(slotSize uint32) (capacity, slotsStart uint32) {
	// Upper-bound capacity without bitmap overhead.
	upperCap := (PageSize - HeaderSize) / slotSize
	bitmapBytes := uint32(bits.Len64(uint64(upperCap)+63)/8 + 8) // conservative
	bitmapBytes = ((uint32((upperCap+63)/64) * 8))               // exact
	slotsStart = roundUp(HeaderSize+bitmapBytes, slotSize)
	capacity = (PageSize - slotsStart) / slotSize
	return capacity, slotsStart
}

// roundUp rounds v up to the nearest multiple of align.
func roundUp(v, align uint32) uint32 {
	return (v + align - 1) &^ (align - 1)
}

// SpanPagesFor returns how many 1-MiB pages are needed to hold payloadSize
// bytes plus the page header.
func SpanPagesFor(payloadSize uint32) uint16 {
	total := uint32(HeaderSize) + payloadSize
	return uint16((total + PageSize - 1) / PageSize)
}
