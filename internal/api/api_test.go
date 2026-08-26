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

package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/samanyugoyal2010/frankenqueue/internal/pagestore"
	"github.com/samanyugoyal2010/frankenqueue/internal/queue"
)

func newServer(t *testing.T) (*httptest.Server, *queue.Broker) {
	t.Helper()
	b, err := queue.OpenBroker(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(b, nil))
	t.Cleanup(func() { srv.Close(); b.Close() })
	return srv, b
}

func do(t *testing.T, method, url string, body any, out any) int {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if out != nil {
		if err := json.NewDecoder(res.Body).Decode(out); err != nil && res.StatusCode < 300 {
			t.Fatalf("decode %s %s: %v", method, url, err)
		}
	}
	return res.StatusCode
}

type leaseResp struct {
	Messages []deliveryOut `json:"messages"`
}

func TestEndToEndPriorityLIFOWithDelayAndAck(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"

	if code := do(t, http.MethodPost, base, map[string]any{"name": "jobs", "order": "lifo"}, nil); code != http.StatusCreated {
		t.Fatalf("create queue: %d", code)
	}
	body := map[string]any{"messages": []map[string]any{
		{"body": "lo-a", "priority": 1},
		{"body": "lo-b", "priority": 1},
		{"body": "hi-a", "priority": 9},
		{"body": "hi-b", "priority": 9},
		{"body": "delayed-top", "priority": 200, "delay_ms": 60000},
	}}
	if code := do(t, http.MethodPost, base+"/jobs/messages", body, nil); code != http.StatusCreated {
		t.Fatalf("enqueue: %d", code)
	}

	var got []string
	var receipts []string
	for i := 0; i < 4; i++ {
		var lr leaseResp
		if code := do(t, http.MethodPost, base+"/jobs/lease", map[string]any{"max": 1}, &lr); code != http.StatusOK {
			t.Fatalf("lease: %d", code)
		}
		if len(lr.Messages) != 1 {
			t.Fatalf("lease %d returned %d messages", i, len(lr.Messages))
		}
		got = append(got, lr.Messages[0].Body)
		receipts = append(receipts, lr.Messages[0].Receipt)
	}
	want := []string{"hi-b", "hi-a", "lo-b", "lo-a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
	var empty leaseResp
	do(t, http.MethodPost, base+"/jobs/lease", map[string]any{"max": 5}, &empty)
	if len(empty.Messages) != 0 {
		t.Fatalf("delayed message was delivered early: %v", empty.Messages)
	}

	for _, r := range receipts {
		if code := do(t, http.MethodPost, base+"/jobs/ack", map[string]any{"receipt": r}, nil); code != http.StatusOK {
			t.Fatalf("ack: %d", code)
		}
	}
	if code := do(t, http.MethodPost, base+"/jobs/ack", map[string]any{"receipt": receipts[0]}, nil); code != http.StatusNotFound && code != http.StatusConflict {
		t.Fatalf("stale ack = %d, want 404/409", code)
	}

	var stats queue.Stats
	do(t, http.MethodGet, base+"/jobs/stats", nil, &stats)
	if stats.Acked != 4 || stats.Delayed != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestNackDeadLetterAndReplayOverHTTP(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	do(t, http.MethodPost, base, map[string]any{"name": "poison", "order": "fifo", "max_attempts": 1}, nil)
	do(t, http.MethodPost, base+"/poison/messages", map[string]any{"body": "bad"}, nil)

	for i := 0; i < 2; i++ {
		var lr leaseResp
		do(t, http.MethodPost, base+"/poison/lease", map[string]any{"max": 1}, &lr)
		if len(lr.Messages) == 0 {
			break
		}
		do(t, http.MethodPost, base+"/poison/nack", map[string]any{"receipt": lr.Messages[0].Receipt}, nil)
	}

	var dead struct {
		Messages []deadOut `json:"messages"`
	}
	do(t, http.MethodGet, base+"/poison/dead", nil, &dead)
	if len(dead.Messages) != 1 || dead.Messages[0].Body != "bad" {
		t.Fatalf("dead letters = %+v", dead.Messages)
	}

	do(t, http.MethodPost, base+"/poison/replay", map[string]any{}, nil)
	var lr leaseResp
	do(t, http.MethodPost, base+"/poison/lease", map[string]any{"max": 1}, &lr)
	if len(lr.Messages) != 1 || lr.Messages[0].Body != "bad" {
		t.Fatalf("replayed message = %+v", lr.Messages)
	}
}

func TestUnknownQueueAndBadReceiptStatusCodes(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	if code := do(t, http.MethodGet, base+"/nope/stats", nil, nil); code != http.StatusNotFound {
		t.Fatalf("unknown queue = %d, want 404", code)
	}
	do(t, http.MethodPost, base, map[string]any{"name": "q"}, nil)
	if code := do(t, http.MethodPost, base+"/q/ack", map[string]any{"receipt": "garbage"}, nil); code != http.StatusConflict {
		t.Fatalf("bad receipt = %d, want 409", code)
	}
	if code := do(t, http.MethodPost, base+"/q/messages", map[string]any{"body": "x", "delay_ms": 99999999999}, nil); code != http.StatusBadRequest {
		t.Fatalf("over-horizon delay = %d, want 400", code)
	}
}

func TestLongPollLeaseWakesOnEnqueue(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	do(t, http.MethodPost, base, map[string]any{"name": "poll"}, nil)

	done := make(chan leaseResp, 1)
	go func() {
		var lr leaseResp
		do(t, http.MethodPost, base+"/poll/lease", map[string]any{"max": 1, "wait_ms": 5000}, &lr)
		done <- lr
	}()
	do(t, http.MethodPost, base+"/poll/messages", map[string]any{"body": "late"}, nil)
	lr := <-done
	if len(lr.Messages) != 1 || lr.Messages[0].Body != "late" {
		t.Fatalf("long poll returned %+v", lr.Messages)
	}
}

// --- new integration tests ---------------------------------------------------

// TestFIFOLifecycle verifies the simplest happy path: create → enqueue →
// lease → ack, checking body, ordering and stats at each step.
func TestFIFOLifecycle(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"

	if code := do(t, "POST", base, map[string]any{"name": "fifo", "order": "fifo"}, nil); code != 201 {
		t.Fatalf("create: %d", code)
	}

	messages := []map[string]any{
		{"body": "first"},
		{"body": "second"},
		{"body": "third"},
	}
	var enqResp struct{ IDs []uint64 `json:"ids"` }
	if code := do(t, "POST", base+"/fifo/messages",
		map[string]any{"messages": messages}, &enqResp); code != 201 {
		t.Fatalf("enqueue: %d", code)
	}
	if len(enqResp.IDs) != 3 {
		t.Fatalf("want 3 ids, got %v", enqResp.IDs)
	}

	var receipts []string
	order := []string{"first", "second", "third"}
	for i, want := range order {
		var lr leaseResp
		if code := do(t, "POST", base+"/fifo/lease", map[string]any{"max": 1}, &lr); code != 200 {
			t.Fatalf("lease %d: %d", i, code)
		}
		if len(lr.Messages) != 1 || lr.Messages[0].Body != want {
			t.Fatalf("lease %d: got %+v, want body=%q", i, lr.Messages, want)
		}
		receipts = append(receipts, lr.Messages[0].Receipt)
	}

	for i, r := range receipts {
		if code := do(t, "POST", base+"/fifo/ack", map[string]any{"receipt": r}, nil); code != 200 {
			t.Fatalf("ack %d: %d", i, code)
		}
	}

	var stats queue.Stats
	do(t, "GET", base+"/fifo/stats", nil, &stats)
	if stats.Acked != 3 || stats.Ready != 0 || stats.Inflight != 0 {
		t.Fatalf("final stats = %+v", stats)
	}
}

// TestBatchEnqueueAndAck enqueues a large batch in one HTTP call, leases all,
// acks all, and verifies the page store shrinks to zero live bytes.
func TestBatchEnqueueAndAck(t *testing.T) {
	dir := t.TempDir()
	b, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(b, nil))
	base := srv.URL + "/v1/queues"

	do(t, "POST", base, map[string]any{"name": "batch"}, nil)

	const N = 100
	msgs := make([]map[string]any, N)
	for i := range msgs {
		msgs[i] = map[string]any{"body": "msg"}
	}
	if code := do(t, "POST", base+"/batch/messages", map[string]any{"messages": msgs}, nil); code != 201 {
		t.Fatalf("enqueue: %d", code)
	}

	var stBefore pagestore.Stats
	do(t, "GET", srv.URL+"/v1/stats", nil, &stBefore)
	if stBefore.LiveBytes == 0 {
		t.Fatal("expected LiveBytes > 0 after enqueue")
	}
	t.Logf("after enqueue: %+v", stBefore)

	var receipts []string
	for len(receipts) < N {
		var lr leaseResp
		do(t, "POST", base+"/batch/lease", map[string]any{"max": 20}, &lr)
		for _, m := range lr.Messages {
			receipts = append(receipts, m.Receipt)
		}
	}
	for _, r := range receipts {
		do(t, "POST", base+"/batch/ack", map[string]any{"receipt": r}, nil)
	}

	var stAfter pagestore.Stats
	do(t, "GET", srv.URL+"/v1/stats", nil, &stAfter)
	t.Logf("after ack: %+v", stAfter)
	if stAfter.Pages != 0 {
		t.Errorf("expected 0 pages after ack, got %d", stAfter.Pages)
	}
	if stAfter.LiveBytes != 0 {
		t.Errorf("expected 0 live bytes after ack, got %d", stAfter.LiveBytes)
	}

	srv.Close()
	b.Close()
}

// TestRestartWithPageStoreVerification is the hardest test: enqueue messages,
// close (simulating a crash-free shutdown), reopen, and verify that:
//  1. Messages are still readable after restart.
//  2. The page store has the same number of live pages (index + payload
//     slots survived RebuildVolatile).
//  3. Acking after restart frees all memory.
func TestRestartWithPageStoreVerification(t *testing.T) {
	dir := t.TempDir()
	const N = 50

	// --- Session 1: enqueue N messages, checkpoint, close ---
	func() {
		b, err := queue.OpenBroker(dir)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(New(b, nil))
		base := srv.URL + "/v1/queues"
		do(t, "POST", base, map[string]any{"name": "durable", "order": "fifo"}, nil)
		msgs := make([]map[string]any, N)
		for i := range msgs {
			msgs[i] = map[string]any{"body": "hello"}
		}
		do(t, "POST", base+"/durable/messages", map[string]any{"messages": msgs}, nil)
		do(t, "POST", base+"/durable/checkpoint", nil, nil) // flush index.dat
		var st pagestore.Stats
		do(t, "GET", srv.URL+"/v1/stats", nil, &st)
		t.Logf("before close: pages=%d live=%d", st.Pages, st.LiveBytes)
		srv.Close()
		b.Close()
	}()

	// --- Session 2: reopen, verify messages survive, ack all ---
	b2, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv2 := httptest.NewServer(New(b2, nil))
	defer srv2.Close()
	defer b2.Close()
	base2 := srv2.URL + "/v1/queues"

	var stAfterReopen pagestore.Stats
	do(t, "GET", srv2.URL+"/v1/stats", nil, &stAfterReopen)
	t.Logf("after reopen: pages=%d live=%d", stAfterReopen.Pages, stAfterReopen.LiveBytes)
	if stAfterReopen.Pages == 0 {
		t.Error("expected pages > 0 after reopen (RebuildVolatile should keep live slots)")
	}

	var receipts []string
	for len(receipts) < N {
		var lr leaseResp
		do(t, "POST", base2+"/durable/lease", map[string]any{"max": 20}, &lr)
		if len(lr.Messages) == 0 {
			break
		}
		for _, m := range lr.Messages {
			if m.Body != "hello" {
				t.Fatalf("body mismatch: %q", m.Body)
			}
			receipts = append(receipts, m.Receipt)
		}
	}
	if len(receipts) != N {
		t.Fatalf("after restart: got %d messages, want %d", len(receipts), N)
	}

	for _, r := range receipts {
		do(t, "POST", base2+"/durable/ack", map[string]any{"receipt": r}, nil)
	}

	var stFinal pagestore.Stats
	do(t, "GET", srv2.URL+"/v1/stats", nil, &stFinal)
	t.Logf("after ack: pages=%d live=%d", stFinal.Pages, stFinal.LiveBytes)
	if stFinal.Pages != 0 {
		t.Errorf("pages=%d after full ack, want 0", stFinal.Pages)
	}
}

// TestMaxDepthEnforced checks that enqueue past MaxDepth returns 429.
func TestMaxDepthEnforced(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	do(t, "POST", base, map[string]any{"name": "capped", "max_depth": 2}, nil)
	do(t, "POST", base+"/capped/messages", map[string]any{"body": "a"}, nil)
	do(t, "POST", base+"/capped/messages", map[string]any{"body": "b"}, nil)
	if code := do(t, "POST", base+"/capped/messages", map[string]any{"body": "c"}, nil); code != http.StatusTooManyRequests {
		t.Fatalf("third enqueue: %d, want 429", code)
	}
}

func TestQueueSurvivesServerRestart(t *testing.T) {
	dir := t.TempDir()
	b, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(b, nil))
	base := srv.URL + "/v1/queues"
	do(t, http.MethodPost, base, map[string]any{"name": "durable", "order": "fifo"}, nil)
	do(t, http.MethodPost, base+"/durable/messages", map[string]any{"messages": []map[string]any{
		{"body": "one"}, {"body": "two"},
	}}, nil)
	srv.Close()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	srv2 := httptest.NewServer(New(b2, nil))
	defer srv2.Close()
	var lr leaseResp
	do(t, http.MethodPost, srv2.URL+"/v1/queues/durable/lease", map[string]any{"max": 5}, &lr)
	if len(lr.Messages) != 2 || lr.Messages[0].Body != "one" {
		t.Fatalf("after restart = %+v", lr.Messages)
	}
}

func TestQueueSurvivesUncleanRestart(t *testing.T) {
	dir := t.TempDir()
	b, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(b, nil))
	base := srv.URL + "/v1/queues"
	do(t, http.MethodPost, base, map[string]any{"name": "crash", "order": "fifo"}, nil)
	do(t, http.MethodPost, base+"/crash/messages", map[string]any{"messages": []map[string]any{
		{"body": "one"}, {"body": "two"},
	}}, nil)
	srv.Close()
	if err := b.CloseWithoutCheckpoint(); err != nil {
		t.Fatal(err)
	}

	b2, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	srv2 := httptest.NewServer(New(b2, nil))
	defer srv2.Close()
	var lr leaseResp
	do(t, http.MethodPost, srv2.URL+"/v1/queues/crash/lease", map[string]any{"max": 5}, &lr)
	if len(lr.Messages) != 2 || lr.Messages[0].Body != "one" || lr.Messages[1].Body != "two" {
		t.Fatalf("after unclean restart = %+v", lr.Messages)
	}
}
