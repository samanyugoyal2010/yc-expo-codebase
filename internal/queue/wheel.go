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

import "container/heap"

// wheelEntry is one slot in a timing wheel.
type wheelEntry struct {
	deadline int64  // Unix milliseconds
	msgID    uint64
	indexRef uint64
	nonce    uint64 // non-zero for inflight wheel (receipt validation)
}

// timingWheel is a min-heap ordered by deadline.
// It is used for both the delayed wheel (keyed by available_at_ms) and the
// inflight wheel (keyed by lease_until_ms).
// O(log n) insert/remove; O(k log n) for popping k expired entries.
type timingWheel struct {
	entries []wheelEntry
}

func (w *timingWheel) Len() int { return len(w.entries) }

func (w *timingWheel) Less(i, j int) bool {
	return w.entries[i].deadline < w.entries[j].deadline
}

func (w *timingWheel) Swap(i, j int) {
	w.entries[i], w.entries[j] = w.entries[j], w.entries[i]
}

func (w *timingWheel) Push(x any) {
	w.entries = append(w.entries, x.(wheelEntry))
}

func (w *timingWheel) Pop() any {
	n := len(w.entries)
	x := w.entries[n-1]
	w.entries = w.entries[:n-1]
	return x
}

func (w *timingWheel) add(e wheelEntry) { heap.Push(w, e) }

// popExpired removes and returns all entries with deadline ≤ now.
func (w *timingWheel) popExpired(now int64) []wheelEntry {
	var out []wheelEntry
	for len(w.entries) > 0 && w.entries[0].deadline <= now {
		out = append(out, heap.Pop(w).(wheelEntry))
	}
	return out
}

// remove removes the first entry with the given msgID.  O(n) — used only on
// Ack/Nack, which are infrequent relative to Lease.
func (w *timingWheel) remove(msgID uint64) (wheelEntry, bool) {
	for i, e := range w.entries {
		if e.msgID == msgID {
			heap.Remove(w, i)
			return e, true
		}
	}
	return wheelEntry{}, false
}
