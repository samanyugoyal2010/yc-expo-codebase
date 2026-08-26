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

package queue

import (
	"container/heap"

	"github.com/samanyugoyal2010/frankenqueue/internal/types"
)

// heapEntry is one slot in the ready heap.
type heapEntry struct {
	msgID        uint64
	priority     uint8
	indexRef     uint64
	enqueuedAtMs int64
}

// readyHeap is a max-priority, direction-aware heap that implements the
// composable comparator from §5 of the design doc:
//
//	primary key:   effective priority (descending)
//	secondary key: msg_id × dir  (dir = +1 FIFO, −1 LIFO)
type readyHeap struct {
	entries  []heapEntry
	dir      int64       // +1 FIFO, -1 LIFO
	agingMs  int64       // 0 = disabled
	nowFn    func() int64
}

func newReadyHeap(order types.Order, agingMs int64, nowFn func() int64) *readyHeap {
	dir := int64(1)
	if order == types.LIFO {
		dir = -1
	}
	return &readyHeap{dir: dir, agingMs: agingMs, nowFn: nowFn}
}

// effPriority returns the effective priority of an entry, boosted by age.
func (h *readyHeap) effPriority(e heapEntry) int {
	p := int(e.priority)
	if h.agingMs > 0 && h.nowFn != nil {
		age := h.nowFn() - e.enqueuedAtMs
		if age > 0 {
			boost := int(age / h.agingMs)
			p += boost
			if p > 255 {
				p = 255
			}
		}
	}
	return p
}

func (h *readyHeap) Len() int { return len(h.entries) }

func (h *readyHeap) Less(i, j int) bool {
	a, b := h.entries[i], h.entries[j]
	pa, pb := h.effPriority(a), h.effPriority(b)
	if pa != pb {
		return pa > pb // higher effective priority first
	}
	// Within the same priority: FIFO or LIFO by msg_id.
	return int64(a.msgID)*h.dir < int64(b.msgID)*h.dir
}

func (h *readyHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *readyHeap) Push(x any) {
	h.entries = append(h.entries, x.(heapEntry))
}

func (h *readyHeap) Pop() any {
	n := len(h.entries)
	x := h.entries[n-1]
	h.entries = h.entries[:n-1]
	return x
}

func (h *readyHeap) push(e heapEntry) { heap.Push(h, e) }

func (h *readyHeap) pop() heapEntry { return heap.Pop(h).(heapEntry) }
