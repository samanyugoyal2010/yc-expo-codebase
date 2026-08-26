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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/samanyugoyal2010/frankenqueue/internal/pagestore"
	"github.com/samanyugoyal2010/frankenqueue/internal/types"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func sharedPages(t *testing.T) *pagestore.Store {
	t.Helper()
	s, err := pagestore.Open(t.TempDir())
	if err != nil {
		t.Fatalf("pagestore.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newQ(t *testing.T, order types.Order, maxAttempts uint16) *Queue {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		Name:         "test",
		Order:        order,
		MaxAttempts:  maxAttempts,
		VisibilityMs: 30_000,
	}
	q, err := openQueue(dir, cfg, sharedPages(t))
	if err != nil {
		t.Fatalf("openQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func enq(t *testing.T, q *Queue, body string, priority uint8, delayMs int64) uint64 {
	t.Helper()
	ids, err := q.Enqueue(EnqueueRequest{Payload: []byte(body), Priority: priority, DelayMs: delayMs})
	if err != nil {
		t.Fatalf("Enqueue(%q): %v", body, err)
	}
	return ids[0]
}

func lease1(t *testing.T, q *Queue) Delivery {
	t.Helper()
	ds, err := q.Lease(1, 0)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("Lease returned %d messages, want 1", len(ds))
	}
	return ds[0]
}

func bodies(ds []Delivery) []string {
	s := make([]string, len(ds))
	for i, d := range ds {
		s[i] = string(d.Payload)
	}
	return s
}

// ── 1. FIFO ordering ─────────────────────────────────────────────────────────

func TestFIFO(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	for _, b := range []string{"a", "b", "c", "d"} {
		enq(t, q, b, 5, 0)
	}
	ds, _ := q.Lease(4, 0)
	got := bodies(ds)
	want := []string{"a", "b", "c", "d"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("FIFO order = %v, want %v", got, want)
		}
	}
}

// ── 2. LIFO ordering ─────────────────────────────────────────────────────────

func TestLIFO(t *testing.T) {
	q := newQ(t, types.LIFO, 0)
	for _, b := range []string{"a", "b", "c", "d"} {
		enq(t, q, b, 5, 0)
	}
	ds, _ := q.Lease(4, 0)
	got := bodies(ds)
	want := []string{"d", "c", "b", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("LIFO order = %v, want %v", got, want)
		}
	}
}

// ── 3. Priority + FIFO: higher priority first, then FIFO within same priority ─

func TestPriorityFIFO(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "lo-a", 1, 0)
	enq(t, q, "lo-b", 1, 0)
	enq(t, q, "hi-a", 9, 0)
	enq(t, q, "hi-b", 9, 0)

	ds, _ := q.Lease(4, 0)
	got := bodies(ds)
	want := []string{"hi-a", "hi-b", "lo-a", "lo-b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("priority+FIFO order = %v, want %v", got, want)
		}
	}
}

// ── 4. Priority + LIFO: higher priority first, then LIFO within same priority ─

func TestPriorityLIFO(t *testing.T) {
	q := newQ(t, types.LIFO, 0)
	enq(t, q, "lo-a", 1, 0)
	enq(t, q, "lo-b", 1, 0)
	enq(t, q, "hi-a", 9, 0)
	enq(t, q, "hi-b", 9, 0)

	ds, _ := q.Lease(4, 0)
	got := bodies(ds)
	want := []string{"hi-b", "hi-a", "lo-b", "lo-a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("priority+LIFO order = %v, want %v", got, want)
		}
	}
}

// ── 5. Delayed message does not appear until due ──────────────────────────────

func TestDelay(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "ready", 5, 0)
	enq(t, q, "delayed", 9, 60_000) // high priority but 60 s delay

	// Only the ready message should appear.
	ds, _ := q.Lease(10, 0)
	if len(ds) != 1 || string(ds[0].Payload) != "ready" {
		t.Fatalf("expected only 'ready', got %v", bodies(ds))
	}
}

// ── 6. Delayed message does not jump the ready queue ─────────────────────────

func TestDelayDoesNotJumpQueue(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "lo", 1, 0)
	enq(t, q, "delayed-hi", 200, 60_000)

	ds, _ := q.Lease(5, 0)
	if len(ds) != 1 || string(ds[0].Payload) != "lo" {
		t.Fatalf("delayed message jumped queue: got %v", bodies(ds))
	}
}

// ── 7. Ack removes the message ────────────────────────────────────────────────

func TestAck(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "msg", 5, 0)
	d := lease1(t, q)

	if err := q.Ack(d.Receipt); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	st := q.Stats()
	if st.Acked != 1 || st.Ready != 0 || st.Inflight != 0 {
		t.Fatalf("stats after Ack = %+v", st)
	}
}

// ── 8. Nack re-queues the message ────────────────────────────────────────────

func TestNackRequeue(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "msg", 5, 0)
	d := lease1(t, q)

	// delayMs = 0 puts the message back on ready immediately.
	if err := q.Nack(d.Receipt, 0); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if q.Stats().Ready != 1 {
		t.Fatalf("after Nack delayMs=0: ready=%d, want 1", q.Stats().Ready)
	}

	d2 := lease1(t, q)
	if string(d2.Payload) != "msg" {
		t.Fatalf("after Nack: got %q, want 'msg'", string(d2.Payload))
	}
	if d2.Attempt != 2 {
		t.Fatalf("after Nack attempt = %d, want 2", d2.Attempt)
	}
}

// ── 9. Stale receipt is rejected with ErrBadReceipt ──────────────────────────

func TestStaleReceipt(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "msg", 5, 0)
	d := lease1(t, q)

	// Ack once successfully.
	q.Ack(d.Receipt)

	// Second ack with same receipt must fail.
	err := q.Ack(d.Receipt)
	if err != ErrBadReceipt {
		t.Fatalf("stale ack = %v, want ErrBadReceipt", err)
	}

	// Nack with old receipt must also fail.
	if err := q.Nack(d.Receipt, 0); err != ErrBadReceipt {
		t.Fatalf("stale nack = %v, want ErrBadReceipt", err)
	}
}

// ── 10. Wrong-nonce receipt is rejected ──────────────────────────────────────

func TestWrongNonce(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "msg", 5, 0)
	d := lease1(t, q)

	// Tamper with the nonce field.
	badReceipt := d.Receipt[:len(d.Receipt)-4] + "0000"
	if err := q.Ack(badReceipt); err != ErrBadReceipt {
		t.Fatalf("bad nonce ack = %v, want ErrBadReceipt", err)
	}
}

// ── 11. Max-attempts → dead letter ───────────────────────────────────────────

func TestMaxAttemptsDeadLetter(t *testing.T) {
	q := newQ(t, types.FIFO, 1) // max 1 attempt

	enq(t, q, "bad", 5, 0)
	d := lease1(t, q)

	// Nack with max_attempts=1 → should dead-letter.
	if err := q.Nack(d.Receipt, 0); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// No more leases.
	q.tick()
	ds, _ := q.Lease(1, 0)
	if len(ds) != 0 {
		t.Fatalf("expected no leases after dead-letter, got %v", bodies(ds))
	}

	// Dead letter list should have the message.
	dead, _ := q.DeadLetters(0)
	if len(dead) != 1 || string(dead[0].Payload) != "bad" {
		t.Fatalf("dead letters = %+v", dead)
	}
}

// ── 12. Replay re-enqueues dead letters ───────────────────────────────────────

func TestDeadLetterReplay(t *testing.T) {
	q := newQ(t, types.FIFO, 1)

	enq(t, q, "bad", 5, 0)
	d := lease1(t, q)
	q.Nack(d.Receipt, 0) // → dead letter

	// Replay all.
	newIDs, err := q.Replay(nil, 0)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(newIDs) != 1 {
		t.Fatalf("Replay returned %d IDs, want 1", len(newIDs))
	}

	// Dead list should be empty.
	dead, _ := q.DeadLetters(0)
	if len(dead) != 0 {
		t.Fatalf("dead list not empty after replay: %v", dead)
	}

	// Message re-appears with a new ID.
	q.tick()
	d2 := lease1(t, q)
	if string(d2.Payload) != "bad" {
		t.Fatalf("replayed body = %q, want 'bad'", string(d2.Payload))
	}
	if d2.ID == d.ID {
		t.Fatalf("replayed message has same ID as original")
	}
}

// ── 13. Lease expiry returns message to ready ─────────────────────────────────

func TestLeaseExpiry(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	q.cfg.VisibilityMs = 50 // 50 ms visibility

	enq(t, q, "msg", 5, 0)
	d := lease1(t, q)
	_ = d

	// Simulate time passing past the lease deadline.
	original := q.nowFn
	q.nowFn = func() time.Time { return original().Add(200 * time.Millisecond) }
	q.tick()
	q.nowFn = original

	// Message should be back in ready.
	st := q.Stats()
	if st.Ready != 1 || st.Inflight != 0 {
		t.Fatalf("after expiry: ready=%d inflight=%d, want ready=1 inflight=0", st.Ready, st.Inflight)
	}
}

// ── 14. Stats are correct ────────────────────────────────────────────────────

func TestStats(t *testing.T) {
	q := newQ(t, types.FIFO, 0)

	enq(t, q, "a", 5, 0)
	enq(t, q, "b", 5, 0)
	enq(t, q, "c", 5, 60_000) // delayed

	st := q.Stats()
	if st.Ready != 2 || st.Delayed != 1 || st.Enqueued != 3 {
		t.Fatalf("after enqueue: %+v", st)
	}

	d := lease1(t, q)
	st = q.Stats()
	if st.Ready != 1 || st.Inflight != 1 || st.Delivered != 1 {
		t.Fatalf("after lease: %+v", st)
	}

	q.Ack(d.Receipt)
	st = q.Stats()
	if st.Acked != 1 || st.Inflight != 0 {
		t.Fatalf("after ack: %+v", st)
	}
}

// ── 15. Save & restore: messages survive a close + reopen ────────────────────

func TestSaveRestore(t *testing.T) {
	dir := t.TempDir()
	pDir := t.TempDir()
	pages, _ := pagestore.Open(pDir)
	defer pages.Close()

	cfg := Config{Name: "q", Order: types.FIFO, VisibilityMs: 30_000}

	// Write two messages.
	{
		q, _ := openQueue(dir, cfg, pages)
		enq(t, q, "one", 5, 0)
		enq(t, q, "two", 5, 0)
		q.Checkpoint()
		q.wal.Close()
		// Don't close pages (shared)
	}

	// Reopen, rebuild PageStore volatile state, lease.
	{
		q, err := openQueue(dir, cfg, pages)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer q.Close()

		// Collect refs for RebuildVolatile.
		// (PageStore volatile state is rebuilt by Broker; here we skip it for
		//  the unit test since we reuse the same open Store instance.)

		ds, _ := q.Lease(5, 0)
		got := bodies(ds)
		if len(got) != 2 {
			t.Fatalf("after restore: got %d messages, want 2", len(got))
		}
		if got[0] != "one" || got[1] != "two" {
			t.Fatalf("after restore: order = %v, want [one two]", got)
		}
	}
}

// ── 16. Long-poll: LeaseWait wakes when a message arrives ────────────────────

func TestLongPoll(t *testing.T) {
	q := newQ(t, types.FIFO, 0)

	done := make(chan []Delivery, 1)
	go func() {
		ds, _ := q.LeaseWait(1, 0, 5*time.Second)
		done <- ds
	}()

	time.Sleep(50 * time.Millisecond)
	enq(t, q, "late", 5, 0)

	select {
	case ds := <-done:
		if len(ds) != 1 || string(ds[0].Payload) != "late" {
			t.Fatalf("long-poll returned %v", bodies(ds))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LeaseWait did not wake up")
	}
}

// ── 17. ErrDelayTooLong is returned for excessive delay ──────────────────────

func TestDelayTooLong(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	_, err := q.Enqueue(EnqueueRequest{Payload: []byte("x"), DelayMs: 99_999_999_999})
	if !errors.Is(err, ErrDelayTooLong) {
		t.Fatalf("expected ErrDelayTooLong, got %v", err)
	}
}

// ── 18. Batch enqueue returns IDs in order ────────────────────────────────────

func TestBatchEnqueue(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	reqs := []EnqueueRequest{
		{Payload: []byte("a"), Priority: 5},
		{Payload: []byte("b"), Priority: 5},
		{Payload: []byte("c"), Priority: 5},
	}
	ids, err := q.Enqueue(reqs...)
	if err != nil {
		t.Fatalf("batch Enqueue: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("got %d IDs, want 3", len(ids))
	}
	// IDs must be strictly increasing.
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs not monotonic: %v", ids)
		}
	}
}

// ── 19. Concurrent Enqueue + Lease + Ack (race detector) ─────────────────────

func TestConcurrent(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	const goroutines = 8
	const msgs = 20

	var wg sync.WaitGroup
	// Enqueue concurrently.
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < msgs; i++ {
				q.Enqueue(EnqueueRequest{
					Payload: []byte(fmt.Sprintf("g%d-m%d", g, i)),
				})
			}
		}()
	}
	wg.Wait()

	// Lease and ack concurrently.
	total := goroutines * msgs
	acked := 0
	var mu sync.Mutex
	for acked < total {
		ds, _ := q.Lease(10, 0)
		if len(ds) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		var wg2 sync.WaitGroup
		for _, d := range ds {
			d := d
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				q.Ack(d.Receipt)
				mu.Lock()
				acked++
				mu.Unlock()
			}()
		}
		wg2.Wait()
	}

	st := q.Stats()
	if st.Acked != uint64(total) {
		t.Fatalf("acked = %d, want %d", st.Acked, total)
	}
}

// ── 20. Replay selective dead letters by ID ───────────────────────────────────

func TestReplaySelectiveByID(t *testing.T) {
	q := newQ(t, types.FIFO, 1)

	id1 := enq(t, q, "keep-dead", 5, 0)
	enq(t, q, "replay-me", 5, 0)

	// Dead-letter both.
	for i := 0; i < 2; i++ {
		d := lease1(t, q)
		q.Nack(d.Receipt, 0)
	}

	dead, _ := q.DeadLetters(0)
	if len(dead) != 2 {
		t.Fatalf("expected 2 dead, got %d", len(dead))
	}

	// Replay only the second one (find its ID in the dead list).
	var replayID uint64
	for _, dm := range dead {
		if dm.ID != id1 {
			replayID = dm.ID
		}
	}

	q.Replay([]uint64{replayID}, 0)

	dead2, _ := q.DeadLetters(0)
	if len(dead2) != 1 || dead2[0].ID != id1 {
		t.Fatalf("after selective replay dead = %+v", dead2)
	}
}

func newQWith(t *testing.T, cfg Config) *Queue {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "test"
	}
	if cfg.VisibilityMs == 0 {
		cfg.VisibilityMs = 30_000
	}
	q, err := openQueue(t.TempDir(), cfg, sharedPages(t))
	if err != nil {
		t.Fatalf("openQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func TestCrashWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Create(Config{Name: "jobs", Order: types.FIFO, VisibilityMs: 30_000}); err != nil {
		t.Fatal(err)
	}
	q, _ := b.Get("jobs")
	enq(t, q, "survive", 5, 0)
	if err := b.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}

	b2, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b2.Close() })
	q2, err := b2.Get("jobs")
	if err != nil {
		t.Fatal(err)
	}
	d := lease1(t, q2)
	if string(d.Payload) != "survive" {
		t.Fatalf("after crash: got %q", d.Payload)
	}
}

func TestDeadLettersSurviveCheckpoint(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Create(Config{Name: "jobs", Order: types.FIFO, MaxAttempts: 1, VisibilityMs: 30_000}); err != nil {
		t.Fatal(err)
	}
	q, _ := b.Get("jobs")
	enq(t, q, "poison", 5, 0)
	d := lease1(t, q)
	if err := q.Nack(d.Receipt, 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := b.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}

	b2, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b2.Close() })
	q2, _ := b2.Get("jobs")
	dead, _ := q2.DeadLetters(0)
	if len(dead) != 1 || string(dead[0].Payload) != "poison" {
		t.Fatalf("dead after restart = %+v", dead)
	}
}

func TestConcurrentLeaseExclusive(t *testing.T) {
	q := newQ(t, types.FIFO, 0)
	enq(t, q, "only", 5, 0)

	got := make(chan Delivery, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ds, err := q.Lease(1, 0)
			if err != nil {
				t.Errorf("Lease: %v", err)
				return
			}
			if len(ds) == 1 {
				got <- ds[0]
			}
		}()
	}
	wg.Wait()
	close(got)
	n := 0
	for range got {
		n++
	}
	if n != 1 {
		t.Fatalf("concurrent Lease delivered %d copies, want 1", n)
	}
}

func TestAgeBoostReordersHeap(t *testing.T) {
	q := newQWith(t, Config{Order: types.FIFO, AgeBoostMs: 100, VisibilityMs: 30_000})
	base := time.Unix(1_700_000_000, 0)
	q.nowFn = func() time.Time { return base }
	enq(t, q, "old-low", 1, 0)

	q.nowFn = func() time.Time { return base.Add(500 * time.Millisecond) }
	enq(t, q, "new-high", 5, 0)
	q.tick()

	d := lease1(t, q)
	if string(d.Payload) != "old-low" {
		t.Fatalf("age boost order: got %q, want old-low (prio 1 + 5 boost > 5)", d.Payload)
	}
}

func TestMaxDepthConcurrent(t *testing.T) {
	q := newQWith(t, Config{Order: types.FIFO, MaxDepth: 10, VisibilityMs: 30_000})
	const goroutines = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, failed := 0, 0
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := q.Enqueue(EnqueueRequest{Payload: []byte(fmt.Sprintf("m%d", i))})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok++
			} else if errors.Is(err, ErrFull) {
				failed++
			} else {
				t.Errorf("unexpected: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if ok != 10 || failed != 10 {
		t.Fatalf("ok=%d failed=%d, want 10/10", ok, failed)
	}
	if q.Stats().Ready != 10 {
		t.Fatalf("ready=%d, want 10", q.Stats().Ready)
	}
}

func TestDurableLeaseSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Create(Config{Name: "jobs", Order: types.FIFO, DurableLeases: true, VisibilityMs: 60_000}); err != nil {
		t.Fatal(err)
	}
	q, _ := b.Get("jobs")
	enq(t, q, "leased", 5, 0)
	d := lease1(t, q)
	if err := b.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}

	b2, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b2.Close() })
	q2, _ := b2.Get("jobs")
	st := q2.Stats()
	if st.Inflight != 1 || st.Ready != 0 {
		t.Fatalf("durable lease after crash: %+v", st)
	}
	ds, _ := q2.Lease(1, 0)
	if len(ds) != 0 {
		t.Fatalf("leased again: %v", bodies(ds))
	}
	if err := q2.Ack(d.Receipt); err != nil {
		t.Fatalf("ack recovered lease: %v", err)
	}
}

func TestApplyCursorDoesNotAdvancePastHole(t *testing.T) {
	var c applyCursor
	c.reset(9)
	c.begin(10, 10)
	c.begin(11, 12)
	c.finish(11, 12)
	if got := c.applied(); got != 9 {
		t.Fatalf("applied=%d, want 9 while LSN 10 is unapplied", got)
	}
	c.finish(10, 10)
	if got := c.applied(); got != 12 {
		t.Fatalf("applied=%d, want 12 after hole filled", got)
	}
}

func TestExpireDeadSurvivesUncleanClose(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Create(Config{Name: "jobs", Order: types.FIFO, MaxAttempts: 1, VisibilityMs: 50}); err != nil {
		t.Fatal(err)
	}
	q, _ := b.Get("jobs")
	base := time.Unix(1_700_000_000, 0)
	q.nowFn = func() time.Time { return base }
	enq(t, q, "poison", 5, 0)
	_ = lease1(t, q)

	q.nowFn = func() time.Time { return base.Add(time.Second) }
	q.tick()
	dead, _ := q.DeadLetters(0)
	if len(dead) != 1 {
		t.Fatalf("dead after expire = %d, want 1", len(dead))
	}
	if err := b.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}

	b2, err := OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b2.Close() })
	q2, _ := b2.Get("jobs")
	dead, _ = q2.DeadLetters(0)
	if len(dead) != 1 || string(dead[0].Payload) != "poison" {
		t.Fatalf("dead after crash = %+v", dead)
	}
	ds, _ := q2.Lease(1, 0)
	if len(ds) != 0 {
		t.Fatalf("dead letter became ready: %v", bodies(ds))
	}
}
