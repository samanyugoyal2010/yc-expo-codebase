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

//go:build ignore

// Moved to internal/api/api_test.go
package api
 
import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
 
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
	// The highest-priority message is delayed, so it must not have jumped the
	// queue: eligibility is not a sort key.
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
	// Re-acking a spent receipt is a conflict, not a server error.
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
 
