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

	// reserved counts in-flight Enqueue calls that have passed MaxDepth
	// but are not yet on ready/delayed. Prevents a check-then-act overflow.
	reserved int

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

	apply        applyCursor
	checkpointMu sync.Mutex

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
	done        chan<- commitResult
}

type commitResult struct {
	firstLSN uint64
	lastLSN  uint64
	err      error
}

// applyCursor is a contiguous apply watermark. Checkpoint may only truncate
// through applied(), which does not advance past a durable LSN whose index
// mutation is still in flight. begin() runs in the committer before producers
// are woken; finish() runs after the matching index apply.
type applyCursor struct {
	mu      sync.Mutex
	pending map[uint64]struct{} // first LSN of each unapplied durable request
	high    uint64              // max last LSN begun
	contig  uint64              // max L such that every begun LSN ≤ L is applied
}

func (c *applyCursor) reset(lsn uint64) {
	c.mu.Lock()
	c.pending = make(map[uint64]struct{})
	c.high = lsn
	c.contig = lsn
	c.mu.Unlock()
}

func (c *applyCursor) begin(first, last uint64) {
	if last == 0 {
		return
	}
	c.mu.Lock()
	if c.pending == nil {
		c.pending = make(map[uint64]struct{})
	}
	c.pending[first] = struct{}{}
	if last > c.high {
		c.high = last
	}
	c.recompute()
	c.mu.Unlock()
}

func (c *applyCursor) finish(first, last uint64) {
	if last == 0 {
		return
	}
	c.mu.Lock()
	delete(c.pending, first)
	c.recompute()
	c.mu.Unlock()
}

func (c *applyCursor) recompute() {
	contig := c.high
	for first := range c.pending {
		if first == 0 {
			continue
		}
		if first-1 < contig {
			contig = first - 1
		}
	}
	c.contig = contig
}

func (c *applyCursor) applied() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.contig
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
		commitCh:         make(chan commitRequest), // unbuffered: send fails closed after committer exits
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
func (q *Queue) Enqueue(reqs ...EnqueueRequest) (ids []uint64, err error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	now := q.nowMs()
	maxDel := q.cfg.maxDelayMs()
	for _, r := range reqs {
		if r.DelayMs > maxDel {
			return nil, fmt.Errorf("%w: requested %d ms, max %d ms", ErrDelayTooLong, r.DelayMs, maxDel)
		}
	}

	n := len(reqs)
	q.mu.Lock()
	if q.cfg.MaxDepth > 0 && q.ready.Len()+q.delayed.Len()+q.reserved+n > q.cfg.MaxDepth {
		q.mu.Unlock()
		return nil, ErrFull
	}
	q.reserved += n
	q.mu.Unlock()
	unreserve := true
	defer func() {
		if unreserve {
			q.mu.Lock()
			q.reserved -= n
			q.mu.Unlock()
		}
	}()

	type item struct {
		ref      pagestore.SlotRef
		msgID    uint64
		indexRef uint64
		availMs  int64
		prio     uint8
	}
	items := make([]item, len(reqs))
	refs := make([]pagestore.SlotRef, 0, len(reqs))
	indexRefs := make([]uint64, 0, len(reqs))

	defer func() {
		if err == nil {
			return
		}
		for _, ref := range refs {
			_ = q.pages.Release(ref)
		}
		for _, ir := range indexRefs {
			q.idx.FreeSlot(ir)
		}
	}()

	// Phase 1: allocate payload slots and write payload bytes.
	for i, req := range reqs {
		sz := uint32(len(req.Payload))
		if sz == 0 {
			sz = 1
		}
		ref, buf, allocErr := q.pages.Alloc(sz)
		if allocErr != nil {
			return nil, allocErr
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
	walRecs := make([]walPkg.Record, len(reqs))
	for i, req := range reqs {
		msgID := q.nextMsgID.Add(1)
		indexRef, allocErr := q.idx.AllocSlot()
		if allocErr != nil {
			return nil, allocErr
		}
		indexRefs = append(indexRefs, indexRef)
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

	lsnFirst, lsnLast, commitErr := q.submitCommit(refs, walRecs)
	if commitErr != nil {
		return nil, commitErr
	}

	q.mu.Lock()
	ids = make([]uint64, len(reqs))
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
	q.reserved -= n
	unreserve = false
	q.apply.finish(lsnFirst, lsnLast)
	q.statEnqueued.Add(uint64(len(reqs)))
	q.mu.Unlock()

	refs = nil
	indexRefs = nil
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
	q.promoteLocked(now)

	type payloadRef struct {
		ref pagestore.SlotRef
		len uint32
	}
	var ds []Delivery
	payloads := make([]payloadRef, 0, max)
	leaseRecs := make([]walPkg.Record, 0, max)
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
		slot.LeaseNonce = nonce
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
		payloads = append(payloads, payloadRef{ref: pagestore.SlotRef(slot.PayloadRef), len: slot.PayloadLen})
		if q.cfg.DurableLeases {
			leaseRecs = append(leaseRecs, walPkg.Record{
				Op: walPkg.OpLease,
				Body: walPkg.Lease{
					MsgID:        slot.MsgID,
					Nonce:        nonce,
					Attempt:      slot.Attempts,
					LeaseUntilMs: until,
				},
			})
		}
	}
	q.mu.Unlock()

	if len(leaseRecs) > 0 {
		first, last, err := q.submitCommit(nil, leaseRecs)
		if err != nil {
			q.rollbackLeases(ds)
			return nil, err
		}
		q.apply.finish(first, last)
	}

	for i := range ds {
		data, err := q.pages.Read(payloads[i].ref)
		if err == nil && int(payloads[i].len) <= len(data) {
			cp := make([]byte, payloads[i].len)
			copy(cp, data[:payloads[i].len])
			ds[i].Payload = cp
		}
	}

	q.statDelivered.Add(uint64(len(ds)))
	return ds, nil
}

func (q *Queue) rollbackLeases(ds []Delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, d := range ds {
		_, attempt, nonce, err := decodeReceipt(d.Receipt)
		if err != nil {
			continue
		}
		info, ok := q.receipts[d.ID]
		if !ok || info.nonce != nonce || info.attempt != attempt {
			continue // a newer lease owns this message
		}
		delete(q.receipts, d.ID)
		q.inflight.remove(d.ID)
		slot, ok := q.idx.GetByRef(info.indexRef)
		if !ok {
			continue
		}
		slot.State = index.StateReady
		slot.LeaseUntilMs = 0
		slot.LeaseNonce = 0
		if slot.Attempts > 0 {
			slot.Attempts--
		}
		q.idx.Put(info.indexRef, slot)
		q.ready.push(heapEntry{msgID: slot.MsgID, priority: slot.Priority, indexRef: info.indexRef, enqueuedAtMs: slot.EnqueuedAtMs})
	}
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

	first, last, err := q.submitCommit(nil, []walPkg.Record{{
		Op:   walPkg.OpAck,
		Body: walPkg.Ack{MsgID: msgID, Nonce: nonce},
	}})
	if err != nil {
		q.restoreLease(info, slot)
		return err
	}

	if slot.MsgID != 0 {
		q.pages.Release(pagestore.SlotRef(slot.PayloadRef))
	}
	q.idx.FreeSlot(indexRef)
	q.apply.finish(first, last)

	q.statAcked.Add(1)
	return nil
}

func (q *Queue) restoreLease(info leaseInfo, slot index.Slot) {
	q.mu.Lock()
	q.restoreLeaseLocked(info, slot)
	q.mu.Unlock()
}

func (q *Queue) restoreLeaseLocked(info leaseInfo, slot index.Slot) {
	if slot.MsgID == 0 {
		return
	}
	if _, taken := q.receipts[slot.MsgID]; taken {
		return
	}
	slot.State = index.StateInflight
	slot.LeaseUntilMs = info.until
	slot.LeaseNonce = info.nonce
	q.idx.Put(info.indexRef, slot)
	q.receipts[slot.MsgID] = info
	q.inflight.add(wheelEntry{deadline: info.until, msgID: slot.MsgID, indexRef: info.indexRef, nonce: info.nonce})
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
	toDead := q.cfg.MaxAttempts > 0 && slot.Attempts >= q.cfg.MaxAttempts
	q.mu.Unlock()

	var rec walPkg.Record
	if toDead {
		rec = walPkg.Record{Op: walPkg.OpDead, Body: walPkg.Dead{MsgID: msgID, Reason: 1}}
	} else {
		rec = walPkg.Record{Op: walPkg.OpNack, Body: walPkg.Nack{MsgID: msgID, Nonce: nonce, RequeueAtMs: requeueAt}}
	}
	first, last, err := q.submitCommit(nil, []walPkg.Record{rec})
	if err != nil {
		q.restoreLease(info, slot)
		return err
	}

	q.mu.Lock()
	promoted := false
	if toDead {
		payload := q.readPayloadLocked(slot)
		q.deadLetterLocked(slot, indexRef, payload)
	} else if requeueAt <= q.nowMs() {
		slot.State = index.StateReady
		slot.LeaseUntilMs = 0
		slot.LeaseNonce = 0
		slot.AvailableAtMs = requeueAt
		q.idx.Put(indexRef, slot)
		q.ready.push(heapEntry{msgID: slot.MsgID, priority: slot.Priority, indexRef: indexRef, enqueuedAtMs: slot.EnqueuedAtMs})
		promoted = true
	} else {
		slot.State = index.StateDelayed
		slot.LeaseUntilMs = 0
		slot.LeaseNonce = 0
		slot.AvailableAtMs = requeueAt
		q.idx.Put(indexRef, slot)
		q.delayed.add(wheelEntry{deadline: requeueAt, msgID: slot.MsgID, indexRef: indexRef})
	}
	q.apply.finish(first, last)
	q.mu.Unlock()
	if promoted {
		q.notify()
	}
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
		q.mu.Lock()
		q.dead = append(q.dead, toReplay...)
		q.mu.Unlock()
		return nil, err
	}
	for _, dm := range toReplay {
		if dm.payloadRef != 0 {
			_ = q.pages.Release(pagestore.SlotRef(dm.payloadRef))
		}
		if dm.indexRef != 0 {
			q.idx.FreeSlot(dm.indexRef)
		}
	}
	q.statReplayed.Add(uint64(len(newIDs)))
	return newIDs, nil
}

// Checkpoint writes the current index to disk and truncates the WAL up to
// the highest LSN that is both durable and applied to the index. Using
// DurableLSN alone can drop records the snapshot does not yet contain.
func (q *Queue) Checkpoint() error {
	q.checkpointMu.Lock()
	defer q.checkpointMu.Unlock()

	lsn := q.wal.DurableLSN()
	if applied := q.apply.applied(); applied < lsn {
		lsn = applied
	}
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

// Close stops the ticker, checkpoints, and flushes the WAL.
func (q *Queue) Close() error {
	close(q.stopCh)
	q.wg.Wait()
	q.Checkpoint()
	return q.wal.Close()
}

// closeWithoutCheckpoint stops background work and closes the WAL without
// writing index.dat. Tests use this to simulate a crash after durable ops.
func (q *Queue) closeWithoutCheckpoint() error {
	close(q.stopCh)
	q.wg.Wait()
	return q.wal.Close()
}

// --- Internal helpers --------------------------------------------------------

func (q *Queue) recover() error {
	from := q.idx.CheckpointLSN()
	if from > 0 {
		from++ // records up to checkpointLSN are already in the index
	}
	now := q.nowMs()

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

		case walPkg.OpLease:
			l := rec.Body.(walPkg.Lease)
			if ref, slot, ok := q.findSlot(l.MsgID); ok {
				slot.State = index.StateInflight
				slot.Attempts = l.Attempt
				slot.LeaseUntilMs = l.LeaseUntilMs
				slot.LeaseNonce = l.Nonce
				q.idx.Put(ref, slot)
			}

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
				slot.LeaseNonce = 0
				slot.AvailableAtMs = n.RequeueAtMs
				q.idx.Put(ref, slot)
			}

		case walPkg.OpExpire:
			e := rec.Body.(walPkg.Expire)
			if ref, slot, ok := q.findSlot(e.MsgID); ok && slot.State == index.StateInflight {
				slot.State = index.StateReady
				slot.LeaseUntilMs = 0
				slot.LeaseNonce = 0
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

	type live struct {
		ref uint64
		s   index.Slot
	}
	snapshot := func() []live {
		var slots []live
		q.idx.Scan(func(ref uint64, s index.Slot) {
			slots = append(slots, live{ref: ref, s: s})
		})
		return slots
	}

	// Demote inflight slots that should not survive restart. Must not Put
	// inside Scan: Scan holds RLock and Put needs Lock (deadlock).
	for _, e := range snapshot() {
		s := e.s
		if s.State != index.StateInflight {
			continue
		}
		keep := q.cfg.DurableLeases && s.LeaseUntilMs > now && s.LeaseNonce != 0
		if keep {
			continue
		}
		s.State = index.StateReady
		s.LeaseUntilMs = 0
		s.LeaseNonce = 0
		q.idx.Put(e.ref, s)
	}

	for _, e := range snapshot() {
		s := e.s
		bumpMsgID(&q.nextMsgID, s.MsgID)
		switch s.State {
		case index.StateReady:
			q.ready.push(heapEntry{msgID: s.MsgID, priority: s.Priority, indexRef: e.ref, enqueuedAtMs: s.EnqueuedAtMs})
		case index.StateDelayed:
			q.delayed.add(wheelEntry{deadline: s.AvailableAtMs, msgID: s.MsgID, indexRef: e.ref})
		case index.StateInflight:
			q.receipts[s.MsgID] = leaseInfo{nonce: s.LeaseNonce, attempt: s.Attempts, until: s.LeaseUntilMs, indexRef: e.ref}
			q.inflight.add(wheelEntry{deadline: s.LeaseUntilMs, msgID: s.MsgID, indexRef: e.ref, nonce: s.LeaseNonce})
		case index.StateDead:
			payload := q.readPayloadLocked(s)
			q.deadLetterLocked(s, e.ref, payload)
		}
	}
	applied := q.wal.DurableLSN()
	if cp := q.idx.CheckpointLSN(); cp > applied {
		applied = cp
	}
	q.apply.reset(applied)
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
	for _, d := range q.dead {
		if d.ID == slot.MsgID {
			slot.State = index.StateDead
			slot.LeaseUntilMs = 0
			slot.LeaseNonce = 0
			q.idx.Put(indexRef, slot)
			return
		}
	}
	q.dead = append(q.dead, DeadMessage{
		ID:           slot.MsgID,
		Payload:      payload,
		Priority:     slot.Priority,
		Attempts:     slot.Attempts,
		EnqueuedAtMs: slot.EnqueuedAtMs,
		indexRef:     indexRef,
		payloadRef:   slot.PayloadRef,
	})
	slot.State = index.StateDead
	slot.LeaseUntilMs = 0
	slot.LeaseNonce = 0
	q.idx.Put(indexRef, slot)
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
func (q *Queue) submitCommit(refs []pagestore.SlotRef, walRecs []walPkg.Record) (uint64, uint64, error) {
	done := make(chan commitResult, 1)
	select {
	case q.commitCh <- commitRequest{payloadRefs: refs, walRecs: walRecs, done: done}:
	case <-q.stopCh:
		return 0, 0, walPkg.ErrClosed
	}
	res := <-done
	return res.firstLSN, res.lastLSN, res.err
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
		var lsn uint64
		if err == nil {
			lsn, err = q.wal.AppendBatch(allWalRecs)
		}

		// Step 3: register each request as unapplied BEFORE waking producers,
		// so Checkpoint cannot jump past a hole.
		assigned := lsn
		if err == nil && len(allWalRecs) > 0 {
			assigned = lsn - uint64(len(allWalRecs))
		}
		for _, r := range batch {
			first, last := uint64(0), uint64(0)
			if err == nil && len(r.walRecs) > 0 {
				first = assigned + 1
				last = assigned + uint64(len(r.walRecs))
				assigned = last
				q.apply.begin(first, last)
			}
			r.done <- commitResult{firstLSN: first, lastLSN: last, err: err}
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

	if q.cfg.AgeBoostMs > 0 {
		q.ready.refresh()
	}

	// Promote delayed → ready.
	before := q.ready.Len()
	q.promoteLocked(now)
	promoted := q.ready.Len() > before

	// Expire overdue inflight leases.
	type expiredEntry struct {
		msgID    uint64
		nonce    uint64
		toDead   bool
		indexRef uint64
		slot     index.Slot
		info     leaseInfo
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
			toExpire = append(toExpire, expiredEntry{msgID: e.msgID, nonce: e.nonce, toDead: true, indexRef: e.indexRef, slot: slot, info: info})
		} else {
			slot.State = index.StateReady
			slot.LeaseUntilMs = 0
			slot.LeaseNonce = 0
			q.idx.Put(e.indexRef, slot)
			q.ready.push(heapEntry{msgID: slot.MsgID, priority: slot.Priority, indexRef: e.indexRef, enqueuedAtMs: slot.EnqueuedAtMs})
			toExpire = append(toExpire, expiredEntry{msgID: e.msgID, nonce: e.nonce})
			promoted = true
		}
	}
	q.mu.Unlock()

	for _, e := range toExpire {
		if e.toDead {
			first, last, err := q.submitCommit(nil, []walPkg.Record{{
				Op:   walPkg.OpDead,
				Body: walPkg.Dead{MsgID: e.msgID, Reason: 2},
			}})
			if err != nil {
				q.restoreLease(e.info, e.slot)
				continue
			}
			q.mu.Lock()
			payload := q.readPayloadLocked(e.slot)
			q.deadLetterLocked(e.slot, e.indexRef, payload)
			q.apply.finish(first, last)
			q.mu.Unlock()
		} else {
			first, last, err := q.submitCommit(nil, []walPkg.Record{{
				Op:   walPkg.OpExpire,
				Body: walPkg.Expire{MsgID: e.msgID, Nonce: e.nonce},
			}})
			if err == nil {
				q.apply.finish(first, last)
			}
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
