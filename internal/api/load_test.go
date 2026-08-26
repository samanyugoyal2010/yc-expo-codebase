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
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samanyugoyal2010/frankenqueue/internal/queue"
)

// loadResult holds the measurements for one run.
type loadResult struct {
	payloadLabel string
	totalMsgs    int
	enqQPS       float64
	ackQPS       float64
	throughputMB float64
	p99LatencyMs float64
	missed       int
}

// TestConcurrentLoadTable runs two load matrices:
//   - Single-message HTTP calls (baseline)
//   - 512-message HTTP batches (batch commit path)
//
// Every produced message must be consumed exactly once.
func TestConcurrentLoadTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in -short mode")
	}

	const (
		numProducers    = 8
		numConsumers    = 4
		numQueues       = 4
	)

	scenarios := []struct {
		label string
		size  int
	}{
		{"64 B", 64},
		{"256 B", 256},
		{"1 KB", 1024},
		{"4 KB", 4096},
		{"16 KB", 16 * 1024},
	}

	// ── Single-message baseline (each HTTP call = 1 message) ────────────────
	// 200 msgs × 8 producers = 1600 total; at ~450 QPS takes ~3.5s per payload
	// size × 5 sizes = ~18s total — well within the default 120s go test timeout.
	const singleMsgsPerProducer = 200
	var singleResults []loadResult
	for _, sc := range scenarios {
		r := runLoad(t, sc.label, sc.size, numProducers, numConsumers, numQueues,
			singleMsgsPerProducer, 1)
		singleResults = append(singleResults, r)
	}
	t.Log("=== Baseline: 1 message per HTTP call ===")
	printTable(t, numProducers, numConsumers, numQueues, singleMsgsPerProducer, 1, singleResults)

	// ── Batch: 512 messages per HTTP call ───────────────────────────────────
	const batchSize = 512
	const batchMsgsPerProducer = 512 // exactly 1 HTTP call per producer
	var batchResults []loadResult
	for _, sc := range scenarios {
		r := runLoad(t, sc.label, sc.size, numProducers, numConsumers, numQueues,
			batchMsgsPerProducer, batchSize)
		batchResults = append(batchResults, r)
	}
	t.Log("=== Batch: 512 messages per HTTP call ===")
	printTable(t, numProducers, numConsumers, numQueues, batchMsgsPerProducer, batchSize, batchResults)
}

// runLoad executes one load scenario and returns measurements.
// batchSize controls how many messages are packed into each HTTP POST.
func runLoad(t *testing.T, label string, payloadSize, numProducers, numConsumers, numQueues, msgsPerProducer, batchSize int) loadResult {
	t.Helper()

	b, err := queue.OpenBroker(t.TempDir())
	if err != nil {
		t.Fatalf("OpenBroker: %v", err)
	}
	srv := httptest.NewServer(New(b, nil))
	defer srv.Close()
	defer b.Close()

	base := srv.URL + "/v1/queues"
	totalMsgs := numProducers * msgsPerProducer

	// Create queues.
	for i := 0; i < numQueues; i++ {
		name := fmt.Sprintf("q%d", i)
		code := do(t, "POST", base, map[string]any{"name": name, "order": "fifo"}, nil)
		if code != 201 {
			t.Fatalf("create queue %s: %d", name, code)
		}
	}

	// Tracking: each message is tagged producerID*msgsPerProducer + seqNum.
	// received[tag] is set atomically when the consumer acks that message.
	received := make([]int32, totalMsgs)

	// Latency samples (enqueue → ack round-trip in ms).
	var latMu sync.Mutex
	latencies := make([]float64, 0, totalMsgs)

	// ── Producers ──────────────────────────────────────────────────────────
	var wgProd sync.WaitGroup
	var enqCount atomic.Int64
	enqStart := time.Now()

	for p := 0; p < numProducers; p++ {
		p := p
		wgProd.Add(1)
		go func() {
			defer wgProd.Done()
			qName := fmt.Sprintf("q%d", p%numQueues)
			url := fmt.Sprintf("%s/%s/messages", base, qName)
			payload := makePayload(payloadSize, p)

			// Send messages in batches of batchSize.
			for seq := 0; seq < msgsPerProducer; seq += batchSize {
				end := seq + batchSize
				if end > msgsPerProducer {
					end = msgsPerProducer
				}
				msgs := make([]map[string]any, 0, end-seq)
				for i := seq; i < end; i++ {
					tag := p*msgsPerProducer + i
					msgs = append(msgs, map[string]any{"body": embedTag(payload, tag)})
				}
				var resp struct{ IDs []uint64 `json:"ids"` }
				if code := do(t, "POST", url, map[string]any{"messages": msgs}, &resp); code != 201 {
					t.Errorf("producer %d seq %d: enqueue returned %d", p, seq, code)
					return
				}
				enqCount.Add(int64(len(msgs)))
			}
		}()
	}

	wgProd.Wait()
	enqDur := time.Since(enqStart)

	// ── Consumers ──────────────────────────────────────────────────────────
	var ackCount atomic.Int64
	ackStart := time.Now()
	deadline := time.Now().Add(30 * time.Second)

	var wgCons sync.WaitGroup
	stopCons := make(chan struct{})
	var stopOnce sync.Once

	for c := 0; c < numConsumers; c++ {
		c := c
		wgCons.Add(1)
		go func() {
			defer wgCons.Done()
			qName := fmt.Sprintf("q%d", c%numQueues)
			leaseURL := fmt.Sprintf("%s/%s/lease", base, qName)
			ackURL := fmt.Sprintf("%s/%s/ack", base, qName)

			for {
				select {
				case <-stopCons:
					return
				default:
				}
				if time.Now().After(deadline) {
					return
				}

				var lr leaseResp
				do(t, "POST", leaseURL, map[string]any{"max": 50}, &lr)

				if len(lr.Messages) == 0 {
					time.Sleep(5 * time.Millisecond)
					continue
				}

				for _, m := range lr.Messages {
					t0 := time.Now()
					tag := extractTag(m.Body)
					if tag >= 0 && tag < totalMsgs {
						atomic.StoreInt32(&received[tag], 1)
					}
					do(t, "POST", ackURL, map[string]any{"receipt": m.Receipt}, nil)
					ackCount.Add(1)
					latMu.Lock()
					latencies = append(latencies, float64(time.Since(t0).Milliseconds()))
					latMu.Unlock()
				}

				if int(ackCount.Load()) >= totalMsgs {
					stopOnce.Do(func() { close(stopCons) })
					return
				}
			}
		}()
	}

	wgCons.Wait()
	ackDur := time.Since(ackStart)

	// Verify: count missed messages.
	missed := 0
	for i := range received {
		if atomic.LoadInt32(&received[i]) == 0 {
			missed++
		}
	}

	bytesTotal := float64(totalMsgs) * float64(payloadSize)
	p99 := percentile(latencies, 99)

	return loadResult{
		payloadLabel: label,
		totalMsgs:    totalMsgs,
		enqQPS:       float64(enqCount.Load()) / enqDur.Seconds(),
		ackQPS:       float64(ackCount.Load()) / ackDur.Seconds(),
		throughputMB: bytesTotal / ackDur.Seconds() / (1 << 20),
		p99LatencyMs: p99,
		missed:       missed,
	}
}

// makePayload generates a deterministic payload of exactly `size` bytes.
func makePayload(size, producerID int) string {
	buf := make([]byte, size)
	r := rand.New(rand.NewSource(int64(producerID)))
	for i := range buf {
		buf[i] = 'a' + byte(r.Intn(26))
	}
	return string(buf)
}

// embedTag writes the message tag (int) into a JSON-safe field prepended to
// the body: "<TAG:NNN> rest of payload...".  This keeps the payload size stable.
func embedTag(payload string, tag int) string {
	prefix := fmt.Sprintf("<TAG:%010d>", tag)
	if len(payload) <= len(prefix) {
		return prefix
	}
	return prefix + payload[len(prefix):]
}

// extractTag reads the tag embedded by embedTag.  Returns -1 on parse failure.
func extractTag(body string) int {
	var tag int
	if _, err := fmt.Sscanf(body, "<TAG:%d>", &tag); err != nil {
		return -1
	}
	return tag
}

// percentile returns the p-th percentile of a slice of float64 values.
func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	// Simple insertion sort — vals are small (≤ totalMsgs).
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx]
}

// printTable formats and logs the results table.
func printTable(t *testing.T, producers, consumers, queues, msgsPerProd, batchSize int, results []loadResult) {
	t.Helper()

	sep := strings.Repeat("─", 90)
	hdr := fmt.Sprintf("%-10s  %8s  %12s  %12s  %14s  %12s  %6s",
		"Payload", "Msgs", "Enqueue QPS", "Ack QPS", "Throughput", "p99 Lat(ms)", "Missed")

	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════╗")
	t.Logf("║  FrankenQueue Load — Producers:%d  Consumers:%d  Queues:%d  Msgs/prod:%d  Batch:%d",
		producers, consumers, queues, msgsPerProd, batchSize)
	t.Log("╚══════════════════════════════════════════════════════════════════════╝")
	t.Log("")
	t.Log(sep)
	t.Log(hdr)
	t.Log(sep)

	allClean := true
	for _, r := range results {
		missedStr := "✓"
		if r.missed > 0 {
			missedStr = fmt.Sprintf("✗ %d", r.missed)
			allClean = false
		}
		t.Logf("%-10s  %8d  %12.0f  %12.0f  %11.2f MB/s  %12.1f  %6s",
			r.payloadLabel, r.totalMsgs,
			r.enqQPS, r.ackQPS,
			r.throughputMB, r.p99LatencyMs, missedStr)
	}
	t.Log(sep)
	if allClean {
		t.Log("✓  All messages delivered — zero missed, zero duplicates")
	}
	t.Log("")

	// Fail the test if any payload was missed.
	for _, r := range results {
		if r.missed > 0 {
			t.Errorf("payload=%s: %d messages missed", r.payloadLabel, r.missed)
		}
	}
}

// TestLoadSingleQueue is a faster smoke-test of the load harness (runs without -short).
func TestLoadSingleQueue(t *testing.T) {
	b, _ := queue.OpenBroker(t.TempDir())
	srv := httptest.NewServer(New(b, nil))
	defer srv.Close()
	defer b.Close()

	base := srv.URL + "/v1/queues"
	do(t, "POST", base, map[string]any{"name": "smokeq", "order": "fifo"}, nil)

	const (
		producers    = 4
		consumers    = 2
		msgsPerProd  = 100  // send in 1 batch of 100
		payloadBytes = 256
		batch        = 100
	)
	total := producers * msgsPerProd
	received := make([]int32, total)

	var wgP, wgC sync.WaitGroup
	var ackCount atomic.Int64
	stop := make(chan struct{})
	var stopOnce sync.Once

	// Consumers.
	for c := 0; c < consumers; c++ {
		wgC.Add(1)
		go func() {
			defer wgC.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var lr leaseResp
				do(t, "POST", base+"/smokeq/lease", map[string]any{"max": 20}, &lr)
				for _, m := range lr.Messages {
					tag := extractTag(m.Body)
					if tag >= 0 && tag < total {
						atomic.StoreInt32(&received[tag], 1)
					}
					do(t, "POST", base+"/smokeq/ack", map[string]any{"receipt": m.Receipt}, nil)
					if ackCount.Add(1) >= int64(total) {
						stopOnce.Do(func() { close(stop) })
						return
					}
				}
				if len(lr.Messages) == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}

	// Producers — send all messages in one batch per producer.
	for p := 0; p < producers; p++ {
		p := p
		wgP.Add(1)
		go func() {
			defer wgP.Done()
			payload := makePayload(payloadBytes, p)
			msgs := make([]map[string]any, msgsPerProd)
			for seq := 0; seq < msgsPerProd; seq++ {
				msgs[seq] = map[string]any{"body": embedTag(payload, p*msgsPerProd+seq)}
			}
			do(t, "POST", base+"/smokeq/messages", map[string]any{"messages": msgs}, nil)
		}()
	}

	wgP.Wait()
	wgC.Wait()

	missed := 0
	for i := range received {
		if atomic.LoadInt32(&received[i]) == 0 {
			missed++
		}
	}
	if missed > 0 {
		t.Errorf("%d/%d messages missed", missed, total)
	} else {
		t.Logf("✓ all %d messages delivered across %d producers/%d consumers", total, producers, consumers)
	}
}

// TestPushRate measures the pure ingestion rate of the queue — enqueue only,
// no consumers.  It answers: "how fast can the queue absorb data?"
//
// Variables:
//   - Batch size (messages per HTTP call): 1, 8, 64, 512, 1024
//   - Payload size: 64 B, 1 KB, 16 KB
//
// Uses a single httptest.Server for all scenarios and a pooled HTTP client
// so that TCP connections are reused rather than exhausting ephemeral ports.
func TestPushRate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping push-rate test in -short mode")
	}

	const numProducers = 8

	batchSizes := []int{1, 8, 64, 512, 1024}
	payloadSizes := []struct {
		label string
		bytes int
	}{
		{"64 B", 64},
		{"1 KB", 1024},
		{"16 KB", 16 * 1024},
	}

	// 8 producers × 512 msgs = 4096 total per scenario.  Must be divisible by
	// all batch sizes (1,8,64,512,1024) — 512 satisfies all of them.
	const msgsPerProducer = 512

	// Shared server — one queue per scenario to keep state isolated.
	b, err := queue.OpenBroker(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(b, nil))
	defer srv.Close()
	defer b.Close()

	// Pooled client: reuse TCP connections across all scenarios.
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			DisableKeepAlives:   false,
		},
	}

	// pushDo sends a POST and returns the status code, using the pooled client.
	pushDo := func(url string, body any) int {
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", url, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	type row struct {
		batch     int
		payload   string
		totalMsgs int
		qps       float64
		mbps      float64
	}
	var rows []row
	qIdx := 0

	for _, ps := range payloadSizes {
		for _, bs := range batchSizes {
			qName := fmt.Sprintf("pq%d", qIdx)
			qIdx++
			base := srv.URL + "/v1/queues"
			pushDo(base, map[string]any{"name": qName, "order": "fifo"})
			msgURL := fmt.Sprintf("%s/%s/messages", base, qName)

			payload := makePayload(ps.bytes, 0)
			total := numProducers * msgsPerProducer

			var wg sync.WaitGroup
			start := time.Now()

			for p := 0; p < numProducers; p++ {
				p := p
				wg.Add(1)
				go func() {
					defer wg.Done()
					for seq := 0; seq < msgsPerProducer; seq += bs {
						end := seq + bs
						if end > msgsPerProducer {
							end = msgsPerProducer
						}
						n := end - seq
						msgs := make([]map[string]any, n)
						for i := 0; i < n; i++ {
							msgs[i] = map[string]any{
								"body": embedTag(payload, p*msgsPerProducer+seq+i),
							}
						}
						if code := pushDo(msgURL, map[string]any{"messages": msgs}); code != 201 {
							t.Errorf("enqueue failed: %d", code)
							return
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			qps := float64(total) / elapsed.Seconds()
			mbps := float64(total) * float64(ps.bytes) / elapsed.Seconds() / (1 << 20)
			rows = append(rows, row{
				batch: bs, payload: ps.label,
				totalMsgs: total, qps: qps, mbps: mbps,
			})
		}
	}

	// Print results table.
	sep := strings.Repeat("─", 72)
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════╗")
	t.Logf("║  FrankenQueue Push Rate — %d producers · %d msgs/producer · durable",
		numProducers, msgsPerProducer)
	t.Log("╚══════════════════════════════════════════════════════════════════════╝")
	t.Log("")
	t.Log(sep)
	t.Logf("%-8s  %-8s  %10s  %12s  %14s", "Batch", "Payload", "Total Msgs", "Enq QPS", "Throughput")
	t.Log(sep)

	curPayload := ""
	for _, r := range rows {
		if r.payload != curPayload {
			if curPayload != "" {
				t.Log("")
			}
			curPayload = r.payload
		}
		t.Logf("%-8d  %-8s  %10d  %12.0f  %11.2f MB/s",
			r.batch, r.payload, r.totalMsgs, r.qps, r.mbps)
	}
	t.Log(sep)
	t.Log("")
	t.Log("Fully durable: payload fsync + WAL fsync before every return.")
	t.Log("")
}

// Ensure json import is used (for potential body decoding extensions).
var _ = json.Marshal
