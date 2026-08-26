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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samanyugoyal2010/frankenqueue/internal/index"
	"github.com/samanyugoyal2010/frankenqueue/internal/pagestore"
	walPkg "github.com/samanyugoyal2010/frankenqueue/internal/wal"
)

// leaseInfo tracks an active lease in the receipts map.
type leaseInfo struct {
	nonce    uint64
	attempt  uint16
	until    int64  // ms
	indexRef uint64 // SlotRef of index slot
}

// Queue is the per-queue runtime: volatile ordering structures on top of the
// durable WAL + Index + shared PageStore.
type Queue struct {
	cfg Config
	dir string

	mu       sync.Mutex
	ready    *readyHeap
	delayed  timingWheel
	inflight timingWheel
	receipts map[uint64]leaseInfo // msgID → current lease

	dead []DeadMessage

	wal   *walPkg.WAL
	idx   *index.Index
	pages *pagestore.Store

	nextMsgID atomic.Uint64

	// Atomic lifetime counters (read without lock for Stats).
	statEnqueued  atomic.Uint64
	statDelivered atomic.Uint64
	statAcked     atomic.Uint64
	statNacked    atomic.Uint64
	statExpired   atomic.Uint64
	statReplayed  atomic.Uint64

	// Long-poll wakeup.  A single buffered channel: non-blocking send on
	// enqueue, blocking recv with timeout on LeaseWait.
	notifyCh chan struct{}

	// Background expiry ticker.
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Checkpoint trigger.
	lastCheckpointMs int64

	// Batch commit: producers submit here, committer flushes as a group.
	commitCh chan commitRequest

	// Overridable clock for deterministic tests.
	nowFn func() time.Time
}

const (
	walCheckpointBytes   = 32 << 20       // 32 MiB WAL size triggers a checkpoint
	checkpointIntervalMs = 5 * 60 * 1000  // 5 minutes

	// Commit batch: flush payload pages + WAL when either limit is reached.
	commitBatchSize    = 64                        // messages per batch
	commitLagDuration = 5 * time.Millisecond      // max wait before forced flush
)

// commitRequest bundles one producer's payload refs and WAL records for the
// committer goroutine to flush as part of a batch.
type commitRequest struct {
	payloadRefs []pagestore.SlotRef
	walRecs     []walPkg.Record
	done        chan<- error
}

// openQueue opens or creates a queue from its directory.  pages is shared with
// all other queues in the broker.
func openQueue(dir string, cfg Config, pages *pagestore.Store) (*Queue, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	walDir := filepath.Join(dir, "wal")
	w, err := walPkg.Open(walDir)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	idx, err := index.Open(dir, pages)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("index: %w", err)
	}

	q := &Queue{
		cfg:              cfg,
		dir:              dir,
		receipts:         make(map[uint64]leaseInfo),
		wal:              w,
		idx:              idx,
		pages:            pages,
		notifyCh:         make(chan struct{}, 1),
		stopCh:           make(chan struct{}),
		nowFn:            time.Now,
		lastCheckpointMs: time.Now().UnixMilli(),
		commitCh:         make(chan commitRequest, commitBatchSize*4),
	}
	q.ready = newReadyHeap(cfg.Order, cfg.AgeBoostMs, q.nowMs)

	// Replay WAL to rebuild volatile state.
	if err := q.recover(); err != nil {
		w.Close()
		return nil, fmt.Errorf("recover: %w", err)
	}

	q.wg.Add(2)
	go q.ticker()
	go q.runCommitter()
	return q, nil
}

// saveMeta writes queue.meta atomically.
func saveMeta(dir string, cfg Config) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "queue.meta.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "queue.meta")); err != nil {
		os.Remove(tmp)
		return err
	}
	d, _ := os.Open(dir)
	if d != nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// loadMeta reads queue.meta.
func loadMeta(dir string) (Config, error) {
	data, err := os.ReadFile(filepath.Join(dir, "queue.meta"))
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	return cfg, json.Unmarshal(data, &cfg)
}

// Config returns the queue's configuration.
func (q *Queue) Config() Config { return q.cfg }

// nowMs returns the current time as Unix milliseconds.
func (q *Queue) nowMs() int64 { return q.nowFn().UnixMilli() }

// --- Enqueue -----------------------------------------------------------------

// Enqueue adds one or more messages atomically.  Payload pages are fsynced
// before the WAL record so the invariant (WAL record → payload durable) holds.
func (q *Queue) Enqueue(reqs ...EnqueueRequest) ([]uint64, error) {
	now := q.nowMs()
	maxDel := q.cfg.maxDelayMs()
	for _, r := range reqs {
		if r.DelayMs > maxDel {
			return nil, fmt.Errorf("%w: requested %d ms, max %d ms", ErrDelayTooLong, r.DelayMs, maxDel)
		}
		if q.cfg.MaxDepth > 0 {
			q.mu.Lock()
			depth := q.ready.Len() + q.delayed.Len()
			q.mu.Unlock()
			if depth >= q.cfg.MaxDepth {
				return nil, ErrFull
			}
		}
	}

	type item struct {
		ref      pagestore.SlotRef
		msgID    uint64
		indexRef uint64
		availMs  int64
		prio     uint8
	}
	items := make([]item, len(reqs))
	refs := make([]pagestore.SlotRef, 0, len(reqs))

	// Phase 1: allocate payload slots and write payload bytes.
	for i, req := range reqs {
		sz := uint32(len(req.Payload))
		if sz == 0 {
			sz = 1
		}
		ref, buf, err := q.pages.Alloc(sz)
		if err != nil {
			return nil, err
		}
		if len(req.Payload) > 0 {
			copy(buf, req.Payload)
		}
		refs = append(refs, ref)
		availMs := now
		if req.DelayMs > 0 {
			availMs = now + req.DelayMs
		}
		items[i] = item{ref: ref, availMs: availMs, prio: req.Priority}
	}

	// Phase 2: assign IDs + allocate index slots + build WAL records.
	// All atomics — no queue lock needed here.
	walRecs := make([]walPkg.Record, len(reqs))
	for i, req := range reqs {
		msgID := q.nextMsgID.Add(1)
		indexRef, err := q.idx.AllocSlot()
		if err != nil {
			return nil, err
		}
		items[i].msgID = msgID
		items[i].indexRef = indexRef
		walRecs[i] = walPkg.Record{
			Op: walPkg.OpEnqueue,
			Body: walPkg.Enqueue{
				MsgID:         msgID,
				IndexRef:      indexRef,
				PayloadRef:    uint64(items[i].ref),
				PayloadLen:    uint32(len(req.Payload)),
				Priority:      req.Priority,
				EnqueuedAtMs:  now,
				AvailableAtMs: items[i].availMs,
			},
		}
	}

	// Phase 3: submit to committer.
	// The committer accumulates requests until commitBatchSize is reached OR
	// commitLagDuration elapses, then does ONE payload fsync + ONE WAL fsync
	// for the whole batch (§6.1 invariant: payload durable before WAL record).
	if err := q.submitCommit(refs, walRecs); err != nil {
		return nil, err
	}

	// Phase 5: update index + volatile heaps (under lock).
	q.mu.Lock()
	ids := make([]uint64, len(reqs))
	for i, it := range items {
		ids[i] = it.msgID
		st := index.StateReady
		if it.availMs > now {
			st = index.StateDelayed
		}
		slot := index.Slot{
			MsgID:         it.msgID,
			EnqueuedAtMs:  now,
			AvailableAtMs: it.availMs,
			PayloadRef:    uint64(it.ref),
			PayloadLen:    uint32(len(reqs[i].Payload)),
			Priority:      it.prio,
			State:         st,
		}
		q.idx.Put(it.indexRef, slot)

		if st == index.StateReady {
			q.ready.push(heapEntry{msgID: it.msgID, priority: it.prio, indexRef: it.indexRef, enqueuedAtMs: now})
		} else {
			q.delayed.add(wheelEntry{deadline: it.availMs, msgID: it.msgID, indexRef: it.indexRef})
		}
	}
	q.statEnqueued.Add(uint64(len(reqs)))
	q.mu.Unlock()

	q.notify()
	return ids, nil
}

// --- Lease -------------------------------------------------------------------

// Lease pops up to max ready messages and marks them inflight.
// visibilityMs overrides the queue default when > 0.
func (q *Queue) Lease(max int, visibilityMs int64) ([]Delivery, error) {
	if max <= 0 {
		max = 1
	}
	visDur := visibilityMs
	if visDur <= 0 {
		visDur = q.cfg.visibilityMs()
	}
	now := q.nowMs()
	until := now + visDur

	q.mu.Lock()
	// Promote due delayed messages.
	q.promoteLocked(now)

	var ds []Delivery
	for len(ds) < max && q.ready.Len() > 0 {
		e := q.ready.pop()
		slot, ok := q.idx.GetByRef(e.indexRef)
		if !ok || slot.MsgID == 0 {
			continue
		}
		nonce := newNonce()
		slot.Attempts++
		slot.State = index.StateInflight
		slot.LeaseUntilMs = until
		q.idx.Put(e.indexRef, slot)

		receipt := encodeReceipt(slot.MsgID, slot.Attempts, nonce)
		q.receipts[slot.MsgID] = leaseInfo{nonce: nonce, attempt: slot.Attempts, until: until, indexRef: e.indexRef}
		q.inflight.add(wheelEntry{deadline: until, msgID: slot.MsgID, indexRef: e.indexRef, nonce: nonce})

		ds = append(ds, Delivery{
			ID:           slot.MsgID,
			Priority:     slot.Priority,
			Attempt:      slot.Attempts,
			Receipt:      receipt,
			LeaseUntilMs: until,
		})
	}
	q.mu.Unlock()

	// Read payloads (outside lock — zero-copy mmap slice, trimmed to PayloadLen).
	for i := range ds {
		info := q.receipts[ds[i].ID]
		slot, ok := q.idx.GetByRef(info.indexRef)
		if !ok {
			continue
		}
		data, err := q.pages.Read(pagestore.SlotRef(slot.PayloadRef))
		if err == nil {
			cp := make([]byte, slot.PayloadLen)
			copy(cp, data[:slot.PayloadLen])
			ds[i].Payload = cp
		}
	}

	q.statDelivered.Add(uint64(len(ds)))
	return ds, nil
}

// LeaseWait is like Lease but blocks up to wait for a message to become ready.
func (q *Queue) LeaseWait(max int, visibilityMs int64, wait time.Duration) ([]Delivery, error) {
	ds, err := q.Lease(max, visibilityMs)
	if err != nil || len(ds) > 0 {
		return ds, err
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-q.notifyCh:
		return q.Lease(max, visibilityMs)
	case <-timer.C:
		return nil, nil
	}
}

// --- Ack / Nack --------------------------------------------------------------

// Ack marks the message as delivered. Receipt must match the current lease
// nonce; a stale receipt returns ErrBadReceipt.
func (q *Queue) Ack(receipt string) error {
	msgID, attempt, nonce, err := decodeReceipt(receipt)
	if err != nil {
		return ErrBadReceipt
	}

	q.mu.Lock()
	info, ok := q.receipts[msgID]
	if !ok || info.nonce != nonce || info.attempt != attempt {
		q.mu.Unlock()
		return ErrBadReceipt
	}
	delete(q.receipts, msgID)
	q.inflight.remove(msgID)
	indexRef := info.indexRef
	slot, _ := q.idx.GetByRef(indexRef)
	q.mu.Unlock()

	// WAL ACK is fsynced (durable: this removes the message from future delivery).
	if _, err := q.wal.Append(walPkg.Record{
		Op:   walPkg.OpAck,
		Body: walPkg.Ack{MsgID: msgID, Nonce: nonce},
	}); err != nil {
		return err
	}

	// Release index and payload slots now that WAL ACK is durable.
	if slot.MsgID != 0 {
		q.pages.Release(pagestore.SlotRef(slot.PayloadRef))
	}
	q.idx.FreeSlot(indexRef)

	q.statAcked.Add(1)
	return nil
}

// Nack returns the message for redelivery after delayMs.
// If attempts ≥ MaxAttempts, the message is dead-lettered instead.
func (q *Queue) Nack(receipt string, delayMs int64) error {
	msgID, attempt, nonce, err := decodeReceipt(receipt)
	if err != nil {
		return ErrBadReceipt
	}

	q.mu.Lock()
	info, ok := q.receipts[msgID]
	if !ok || info.nonce != nonce || info.attempt != attempt {
		q.mu.Unlock()
		return ErrBadReceipt
	}
	delete(q.receipts, msgID)
	q.inflight.remove(msgID)
	indexRef := info.indexRef
	slot, _ := q.idx.GetByRef(indexRef)

	now := q.nowMs()
	requeueAt := now
	if delayMs > 0 {
		requeueAt = now + delayMs
	}

	if q.cfg.MaxAttempts > 0 && slot.Attempts >= q.cfg.MaxAttempts {
		payload := q.readPayloadLocked(slot)
		q.deadLetterLocked(slot, indexRef, payload)
		q.mu.Unlock()
		q.wal.Append(walPkg.Record{Op: walPkg.OpDead, Body: walPkg.Dead{MsgID: msgID, Reason: 1}})
		q.statNacked.Add(1)
		return nil
	}

	slot.State = index.StateDelayed
	slot.LeaseUntilMs = 0
	q.idx.Put(indexRef, slot)
	q.delayed.add(wheelEntry{deadline: requeueAt, msgID: slot.MsgID, indexRef: indexRef})
	q.mu.Unlock()

	// NACK is volatile (no fsync).
	q.wal.Append(walPkg.Record{
		Op:   walPkg.OpNack,
		Body: walPkg.Nack{MsgID: msgID, Nonce: nonce, RequeueAtMs: requeueAt},
	})
	q.statNacked.Add(1)
	return nil
}

// --- Dead letters + Replay ---------------------------------------------------

// DeadLetters returns up to limit dead-lettered messages (0 = all).
func (q *Queue) DeadLetters(limit int) ([]DeadMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if limit <= 0 || limit > len(q.dead) {
		cp := make([]DeadMessage, len(q.dead))
		copy(cp, q.dead)
		return cp, nil
	}
	cp := make([]DeadMessage, limit)
	copy(cp, q.dead)
	return cp, nil
}

// Replay re-enqueues dead letters as fresh messages. If ids is empty, all are
// replayed. Returns the new message IDs.
func (q *Queue) Replay(ids []uint64, delayMs int64) ([]uint64, error) {
	q.mu.Lock()
	idSet := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}
	var toReplay []DeadMessage
	var remaining []DeadMessage
	for _, dm := range q.dead {
		if len(ids) == 0 || idSet[dm.ID] {
			toReplay = append(toReplay, dm)
		} else {
			remaining = append(remaining, dm)
		}
	}
	q.dead = remaining
	q.mu.Unlock()

	if len(toReplay) == 0 {
		return nil, nil
	}

	reqs := make([]EnqueueRequest, len(toReplay))
	for i, dm := range toReplay {
		reqs[i] = EnqueueRequest{
			Payload:  dm.Payload,
			Priority: dm.Priority,
			DelayMs:  delayMs,
		}
	}
	newIDs, err := q.Enqueue(reqs...)
	if err != nil {
		// Put them back.
		q.mu.Lock()
		q.dead = append(q.dead, toReplay...)
		q.mu.Unlock()
		return nil, err
	}
	q.statReplayed.Add(uint64(len(newIDs)))
	return newIDs, nil
}

// Checkpoint writes the current index to disk and truncates the WAL.
func (q *Queue) Checkpoint() error {
	lsn := q.wal.DurableLSN()
	if err := q.idx.Checkpoint(lsn); err != nil {
		return err
	}
	return q.wal.TruncatePrefix(lsn)
}

// Stats returns a point-in-time snapshot.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	r, d, i, dead := q.ready.Len(), q.delayed.Len(), len(q.receipts), len(q.dead)
	q.mu.Unlock()
	return Stats{
		Ready:     r,
		Delayed:   d,
		Inflight:  i,
		Dead:      dead,
		Enqueued:  q.statEnqueued.Load(),
		Delivered: q.statDelivered.Load(),
		Acked:     q.statAcked.Load(),
		Nacked:    q.statNacked.Load(),
		Expired:   q.statExpired.Load(),
		Replayed:  q.statReplayed.Load(),
	}
}

// Close stops the ticker and flushes state.
func (q *Queue) Close() error {
	close(q.stopCh)
	q.wg.Wait()
	q.Checkpoint()
	return q.wal.Close()
}

// --- Internal helpers --------------------------------------------------------

func (q *Queue) recover() error {
	from := q.idx.CheckpointLSN()
	now := q.nowMs()

	// Step 1: Replay WAL — only mutate the index, never the heaps.
	if err := q.wal.Replay(from, func(_ uint64, rec walPkg.Record) error {
		switch rec.Op {
		case walPkg.OpEnqueue:
			e := rec.Body.(walPkg.Enqueue)
			bumpMsgID(&q.nextMsgID, e.MsgID)
			st := index.StateReady
			if e.AvailableAtMs > now {
				st = index.StateDelayed
			}
			q.idx.Put(e.IndexRef, index.Slot{
				MsgID:         e.MsgID,
				EnqueuedAtMs:  e.EnqueuedAtMs,
				AvailableAtMs: e.AvailableAtMs,
				PayloadRef:    e.PayloadRef,
				PayloadLen:    e.PayloadLen,
				Priority:      e.Priority,
				State:         st,
			})

		case walPkg.OpAck:
			a := rec.Body.(walPkg.Ack)
			if ref, slot, ok := q.findSlot(a.MsgID); ok {
				q.pages.Release(pagestore.SlotRef(slot.PayloadRef))
				q.idx.FreeSlot(ref)
				q.statAcked.Add(1)
			}

		case walPkg.OpNack:
			n := rec.Body.(walPkg.Nack)
			if ref, slot, ok := q.findSlot(n.MsgID); ok {
				slot.State = index.StateDelayed
				slot.LeaseUntilMs = 0
				q.idx.Put(ref, slot)
			}

		case walPkg.OpDead:
			d := rec.Body.(walPkg.Dead)
			if ref, slot, ok := q.findSlot(d.MsgID); ok {
				payload := q.readPayloadLocked(slot)
				q.deadLetterLocked(slot, ref, payload)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Step 2: All INFLIGHT → READY (leases are volatile; §6.3).
	q.idx.Scan(func(ref uint64, s index.Slot) {
		if s.State == index.StateInflight {
			s.State = index.StateReady
			s.LeaseUntilMs = 0
			q.idx.Put(ref, s)
		}
	})

	// Step 3: Rebuild heaps and wheels from the final index state.
	q.idx.Scan(func(ref uint64, s index.Slot) {
		bumpMsgID(&q.nextMsgID, s.MsgID)
		switch s.State {
		case index.StateReady:
			q.ready.push(heapEntry{msgID: s.MsgID, priority: s.Priority, indexRef: ref, enqueuedAtMs: s.EnqueuedAtMs})
		case index.StateDelayed:
			q.delayed.add(wheelEntry{deadline: s.AvailableAtMs, msgID: s.MsgID, indexRef: ref})
		}
	})
	return nil
}

// bumpMsgID atomically advances nextMsgID to at least v.
func bumpMsgID(counter *atomic.Uint64, v uint64) {
	for {
		cur := counter.Load()
		if v <= cur {
			return
		}
		if counter.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (q *Queue) promoteLocked(now int64) {
	for _, e := range q.delayed.popExpired(now) {
		slot, ok := q.idx.GetByRef(e.indexRef)
		if !ok || slot.MsgID == 0 {
			continue
		}
		slot.State = index.StateReady
		q.idx.Put(e.indexRef, slot)
		q.ready.push(heapEntry{msgID: slot.MsgID, priority: slot.Priority, indexRef: e.indexRef, enqueuedAtMs: slot.EnqueuedAtMs})
	}
}

func (q *Queue) deadLetterLocked(slot index.Slot, indexRef uint64, payload []byte) {
	q.dead = append(q.dead, DeadMessage{
		ID:           slot.MsgID,
		Payload:      payload,
		Priority:     slot.Priority,
		Attempts:     slot.Attempts,
		EnqueuedAtMs: slot.EnqueuedAtMs,
	})
	q.idx.FreeSlot(indexRef)
}

func (q *Queue) readPayloadLocked(slot index.Slot) []byte {
	if slot.MsgID == 0 {
		return nil
	}
	data, err := q.pages.Read(pagestore.SlotRef(slot.PayloadRef))
	if err != nil {
		return nil
	}
	cp := make([]byte, slot.PayloadLen)
	copy(cp, data[:slot.PayloadLen])
	return cp
}

func (q *Queue) findSlot(msgID uint64) (uint64, index.Slot, bool) {
	var found index.Slot
	var foundRef uint64
	var ok bool
	q.idx.Scan(func(ref uint64, s index.Slot) {
		if s.MsgID == msgID {
			found = s
			foundRef = ref
			ok = true
		}
	})
	return foundRef, found, ok
}

// submitCommit enqueues a commit request and blocks until the committer goroutine
// has fsynced both the payload pages and the WAL record as part of a batch.
func (q *Queue) submitCommit(refs []pagestore.SlotRef, walRecs []walPkg.Record) error {
	done := make(chan error, 1)
	q.commitCh <- commitRequest{payloadRefs: refs, walRecs: walRecs, done: done}
	return <-done
}

// runCommitter is the batch-commit goroutine.  It drains commitCh and flushes
// when either the count threshold (commitBatchSize) or the lag timer fires.
//
// For every batch it does exactly:
//   1. fsync each unique payload page once   (§6.1 step 3)
//   2. WAL AppendBatch — one WAL fsync       (§6.1 step 5)
//   3. notify all waiting producers
//
// This converts N concurrent per-message fsyncs into 1 pair, giving ~N×
// throughput improvement for concurrent producers.
func (q *Queue) runCommitter() {
	defer q.wg.Done()

	batch := make([]commitRequest, 0, commitBatchSize)
	ticker := time.NewTicker(commitLagDuration)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Deduplicate payload page refs — one fsync per unique page is enough.
		seen := make(map[uint32]bool, len(batch))
		var syncRefs []pagestore.SlotRef
		totalRecs := 0
		for _, r := range batch {
			totalRecs += len(r.walRecs)
			for _, ref := range r.payloadRefs {
				if !seen[ref.PageID()] {
					seen[ref.PageID()] = true
					syncRefs = append(syncRefs, ref)
				}
			}
		}

		// Flatten WAL records from all producers in this batch.
		allWalRecs := make([]walPkg.Record, 0, totalRecs)
		for _, r := range batch {
			allWalRecs = append(allWalRecs, r.walRecs...)
		}

		// Step 1: fsync payload pages (one call per unique dirty page).
		err := q.pages.Sync(syncRefs)

		// Step 2: WAL AppendBatch — group commits its own fsync internally.
		if err == nil {
			_, err = q.wal.AppendBatch(allWalRecs)
		}

		// Step 3: wake every waiting producer.
		for _, r := range batch {
			r.done <- err
		}
		batch = batch[:0]
	}

	for {
		select {
		case req := <-q.commitCh:
			batch = append(batch, req)
			if len(batch) >= commitBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-q.stopCh:
			// Drain any remaining requests then flush once more.
			for {
				select {
				case req := <-q.commitCh:
					batch = append(batch, req)
				default:
					flush()
					return
				}
			}
		}
	}
}

// notify sends a non-blocking wakeup to LeaseWait goroutines.
func (q *Queue) notify() {
	select {
	case q.notifyCh <- struct{}{}:
	default:
	}
}

// ticker runs the background promotion + expiry loop.
func (q *Queue) ticker() {
	defer q.wg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			q.tick()
		case <-q.stopCh:
			return
		}
	}
}

func (q *Queue) tick() {
	now := q.nowMs()
	q.mu.Lock()

	// Promote delayed → ready.
	before := q.ready.Len()
	q.promoteLocked(now)
	promoted := q.ready.Len() > before

	// Expire overdue inflight leases.
	type expiredEntry struct {
		msgID  uint64
		nonce  uint64
		toDead bool
	}
	expiredLeases := q.inflight.popExpired(now)
	var toExpire []expiredEntry

	for _, e := range expiredLeases {
		info, ok := q.receipts[e.msgID]
		if !ok || info.nonce != e.nonce {
			continue // already acked/nacked
		}
		delete(q.receipts, e.msgID)

		slot, ok := q.idx.GetByRef(e.indexRef)
		if !ok {
			continue
		}
		q.statExpired.Add(1)

		if q.cfg.MaxAttempts > 0 && slot.Attempts >= q.cfg.MaxAttempts {
			payload := q.readPayloadLocked(slot)
			q.deadLetterLocked(slot, e.indexRef, payload)
			toExpire = append(toExpire, expiredEntry{msgID: e.msgID, nonce: e.nonce, toDead: true})
		} else {
			slot.State = index.StateReady
			slot.LeaseUntilMs = 0
			q.idx.Put(e.indexRef, slot)
			q.ready.push(heapEntry{msgID: slot.MsgID, priority: slot.Priority, indexRef: e.indexRef, enqueuedAtMs: slot.EnqueuedAtMs})
			toExpire = append(toExpire, expiredEntry{msgID: e.msgID, nonce: e.nonce})
			promoted = true
		}
	
	}
	q.mu.Unlock()

	// Async WAL writes (volatile — no fsync needed).
	for _, e := range toExpire {
		if e.toDead {
			q.wal.Append(walPkg.Record{Op: walPkg.OpDead, Body: walPkg.Dead{MsgID: e.msgID, Reason: 2}})
		} else {
			q.wal.Append(walPkg.Record{Op: walPkg.OpExpire, Body: walPkg.Expire{MsgID: e.msgID, Nonce: e.nonce}})
		}
	}

	if promoted {
		q.notify()
	}

	// Checkpoint trigger: size-based or time-based.
	if q.wal.Size() >= walCheckpointBytes || now-q.lastCheckpointMs >= checkpointIntervalMs {
		if err := q.Checkpoint(); err == nil {
			q.lastCheckpointMs = now
		}
	}
}

// --- Receipt encoding --------------------------------------------------------

func encodeReceipt(msgID uint64, attempt uint16, nonce uint64) string {
	return fmt.Sprintf("%d:%d:%016x", msgID, attempt, nonce)
}

func decodeReceipt(s string) (msgID uint64, attempt uint16, nonce uint64, err error) {
	var att uint64
	_, err = fmt.Sscanf(s, "%d:%d:%016x", &msgID, &att, &nonce)
	attempt = uint16(att)
	return
}

func newNonce() uint64 {
	var b [8]byte
	rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}
