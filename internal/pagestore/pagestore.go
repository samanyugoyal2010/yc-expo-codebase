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

// Package pagestore implements the shared 1-MiB-aligned payload store.
// Pages are allocated from a pool of 1-MiB-aligned slab pages (fixed-size
// slot arrays) and jumbo spans (contiguous multi-page regions for large payloads).
package pagestore

// pageOffsetBits is log2(PageSize).  Every in-page byte offset fits in these bits.
const pageOffsetBits = 20

// SlotRef is a compressed, 8-byte reference to a slot in the page store.
//
// Encoding (all bits little-endian):
//
//	bits 63..20  pageID  (44 bits — supports > 10¹³ pages)
//	bits 19.. 0  offset  (20 bits — byte offset within the 1-MiB page)
//
// The zero value (SlotRef(0)) is never a valid reference: page IDs start at 1
// and offset 0 is the page header, not a payload slot.
type SlotRef uint64

// Pack encodes a (pageID, offset) pair as a SlotRef.
func Pack(pageID uint32, offset uint32) SlotRef {
	return SlotRef(uint64(pageID)<<pageOffsetBits | uint64(offset))
}

// PageID returns the page identifier encoded in r.
func (r SlotRef) PageID() uint32 { return uint32(r >> pageOffsetBits) }

// Offset returns the byte offset within the page encoded in r.
func (r SlotRef) Offset() uint32 { return uint32(r & (PageSize - 1)) }

// IsZero reports whether r is the zero (unset) value.
func (r SlotRef) IsZero() bool { return r == 0 }
