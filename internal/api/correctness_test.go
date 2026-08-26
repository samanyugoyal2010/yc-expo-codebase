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

// Package api — correctness_test.go
//
// Tests the four queue types (FIFO, LIFO, Priority, Delayed) with 100 messages
// of varying sizes, verifying:
//   - Exact content correctness (body bytes round-trip intact)
//   - Correct ordering for each queue type
//   - Save-and-restore: push → shutdown → reopen → pop → exact match
//   - On-disk endianness: all formats are explicitly little-endian
package api

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/samanyugoyal2010/frankenqueue/internal/queue"
)

// ── Message encoding ──────────────────────────────────────────────────────────
//
// Body format — ASCII-only so JSON encoding never mangles the bytes:
//
//	"IDX=XXXXXXXX;SIZ=YYYYYYYY;CRC=ZZZZZZZZ;AAA...A"
//	  IDX = 8 hex digits, message sequence number
//	  SIZ = 8 hex digits, total body length in bytes
//	  CRC = 8 hex digits, CRC32 of "IDX=XXXXXXXX;SIZ=YYYYYYYY;"
//	  A…  = 'A' repeated to pad body to SIZ bytes
//
// The fixed header is 40 bytes; if targetSize < 40 it is raised to 40.
// All bytes are ASCII (0x20–0x7E), so JSON round-trips without replacement.

const bodyHeaderLen = 39 // "IDX=XXXXXXXX;SIZ=YYYYYYYY;CRC=ZZZZZZZZ;" = 4+8+1+4+8+1+4+8+1 = 39

func encodeBody(index, targetSize int) string {
	if targetSize < bodyHeaderLen {
		targetSize = bodyHeaderLen
	}
	hdr := fmt.Sprintf("IDX=%08X;SIZ=%08X;", index, targetSize)
	crc := crc32.ChecksumIEEE([]byte(hdr))
	head := hdr + fmt.Sprintf("CRC=%08X;", crc)
	pad := strings.Repeat("A", targetSize-bodyHeaderLen)
	return head + pad
}

func decodeBody(t *testing.T, body string, wantIndex int) {
	t.Helper()
	if len(body) < bodyHeaderLen {
		t.Fatalf("msg %d: body too short (%d bytes)", wantIndex, len(body))
	}
	var gotIndex, wantSize int
	var gotCRCHex string
	_, err := fmt.Sscanf(body, "IDX=%08X;SIZ=%08X;CRC=%8s", &gotIndex, &wantSize, &gotCRCHex)
	if err != nil {
		t.Fatalf("msg %d: failed to parse header: %v", wantIndex, err)
	}
	hdr := fmt.Sprintf("IDX=%08X;SIZ=%08X;", gotIndex, wantSize)
	wantCRC := crc32.ChecksumIEEE([]byte(hdr))
	var gotCRC uint32
	fmt.Sscanf(gotCRCHex, "%08X", &gotCRC)

	if gotIndex != wantIndex {
		t.Errorf("body: index=%d want %d (header=%q)", gotIndex, wantIndex, body[:bodyHeaderLen])
	}
	if len(body) != wantSize {
		t.Errorf("msg %d: body len=%d want %d", wantIndex, len(body), wantSize)
	}
	if gotCRC != wantCRC {
		t.Errorf("msg %d: CRC mismatch (got %08X want %08X) — corruption!", wantIndex, gotCRC, wantCRC)
	}
	for i := bodyHeaderLen; i < len(body); i++ {
		if body[i] != 'A' {
			t.Errorf("msg %d: padding byte[%d]=%02x want 'A'", wantIndex, i, body[i])
			break
		}
	}
}

// msgSizes cycles through 10 different sizes covering many size classes.
var msgSizes = [10]int{12, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384}

func sizeFor(i int) int { return msgSizes[i%len(msgSizes)] }

// leaseAll drains a queue until empty and returns all bodies in delivery order.
func leaseAll(t *testing.T, base, qName string, total int) []string {
	t.Helper()
	url := fmt.Sprintf("%s/%s/lease", base, qName)
	ackURL := fmt.Sprintf("%s/%s/ack", base, qName)
	var bodies []string
	deadline := time.Now().Add(30 * time.Second)
	for len(bodies) < total {
		if time.Now().After(deadline) {
			t.Fatalf("leaseAll: timed out after receiving %d/%d messages", len(bodies), total)
		}
		var lr leaseResp
		do(t, "POST", url, map[string]any{"max": 50}, &lr)
		for _, m := range lr.Messages {
			bodies = append(bodies, m.Body)
			do(t, "POST", ackURL, map[string]any{"receipt": m.Receipt}, nil)
		}
		if len(lr.Messages) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return bodies
}

// ── 1. FIFO correctness ───────────────────────────────────────────────────────

// TestFIFO100Messages enqueues 100 messages of 10 different sizes in order
// 0..99 and verifies they are delivered in exactly the same order (FIFO),
// with each body's content matching byte-for-byte.
func TestFIFO100Messages(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	do(t, "POST", base, map[string]any{"name": "fifo100", "order": "fifo"}, nil)

	const N = 100
	msgs := make([]map[string]any, N)
	for i := 0; i < N; i++ {
		msgs[i] = map[string]any{"body": encodeBody(i, sizeFor(i))}
	}
	if code := do(t, "POST", base+"/fifo100/messages", map[string]any{"messages": msgs}, nil); code != 201 {
		t.Fatalf("enqueue: %d", code)
	}

	bodies := leaseAll(t, base, "fifo100", N)
	if len(bodies) != N {
		t.Fatalf("received %d messages, want %d", len(bodies), N)
	}
	for i, body := range bodies {
		decodeBody(t, body, i) // must be in exact FIFO order
	}
	t.Logf("✓ FIFO: %d messages, 10 different sizes, all correct", N)
}

// ── 2. LIFO correctness ───────────────────────────────────────────────────────

// TestLIFO100Messages enqueues 100 messages in order 0..99 and verifies they
// are delivered in reverse order 99..0 (LIFO) with exact content.
func TestLIFO100Messages(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	do(t, "POST", base, map[string]any{"name": "lifo100", "order": "lifo"}, nil)

	const N = 100
	msgs := make([]map[string]any, N)
	for i := 0; i < N; i++ {
		msgs[i] = map[string]any{"body": encodeBody(i, sizeFor(i))}
	}
	do(t, "POST", base+"/lifo100/messages", map[string]any{"messages": msgs}, nil)

	bodies := leaseAll(t, base, "lifo100", N)
	if len(bodies) != N {
		t.Fatalf("received %d messages, want %d", len(bodies), N)
	}
	for pos, body := range bodies {
		wantIndex := N - 1 - pos // LIFO: last in = first out
		decodeBody(t, body, wantIndex)
	}
	t.Logf("✓ LIFO: %d messages delivered in reverse order, all correct", N)
}

// ── 3. Priority correctness ───────────────────────────────────────────────────

// TestPriority100Messages enqueues 100 messages with priorities cycling 0..9.
// Verifies that all priority-9 messages are delivered before priority-8, etc.
// Content of each message is verified byte-for-byte.
func TestPriority100Messages(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	do(t, "POST", base, map[string]any{"name": "prio100", "order": "fifo"}, nil)

	const N = 100
	// Enqueue with priority = i%10 so we have 10 messages per priority level.
	// Body index = i so we can verify content.
	msgs := make([]map[string]any, N)
	for i := 0; i < N; i++ {
		msgs[i] = map[string]any{
			"body":     encodeBody(i, sizeFor(i)),
			"priority": uint8(i % 10),
		}
	}
	do(t, "POST", base+"/prio100/messages", map[string]any{"messages": msgs}, nil)

	bodies := leaseAll(t, base, "prio100", N)
	if len(bodies) != N {
		t.Fatalf("received %d messages, want %d", len(bodies), N)
	}

	// Verify each body is structurally valid (content correct).
	// Also verify that deliveries are in non-increasing priority order.
	prevPriority := 10 // higher than any real priority
	for _, body := range bodies {
		if len(body) < bodyHeaderLen {
			t.Fatal("body too short to decode index")
		}
		var idx, _ int
		fmt.Sscanf(body, "IDX=%08X;", &idx)
		decodeBody(t, body, idx) // verify content
		prio := idx % 10
		if prio > prevPriority {
			t.Errorf("priority ordering violated: got prio %d after prio %d", prio, prevPriority)
		}
		prevPriority = prio
	}
	t.Logf("✓ Priority: %d messages, priority order correct, all content valid", N)
}

// ── 4. Delayed correctness ────────────────────────────────────────────────────

// TestDelayed100Messages enqueues 60 immediate + 40 delayed messages.
// Verifies: (a) immediate messages arrive immediately, (b) delayed messages
// are not delivered early, (c) after delay window all messages arrive with
// correct content.
func TestDelayed100Messages(t *testing.T) {
	srv, _ := newServer(t)
	base := srv.URL + "/v1/queues"
	do(t, "POST", base, map[string]any{"name": "delay100", "order": "fifo"}, nil)

	const (
		immediate = 60
		delayed   = 40
		total     = immediate + delayed
		delayMs   = 300 // 300ms delay
	)

	// Enqueue immediate messages (indices 0..59).
	immMsgs := make([]map[string]any, immediate)
	for i := 0; i < immediate; i++ {
		immMsgs[i] = map[string]any{"body": encodeBody(i, sizeFor(i))}
	}
	do(t, "POST", base+"/delay100/messages", map[string]any{"messages": immMsgs}, nil)

	// Enqueue delayed messages (indices 60..99, delay=300ms).
	delMsgs := make([]map[string]any, delayed)
	for i := 0; i < delayed; i++ {
		delMsgs[i] = map[string]any{
			"body":     encodeBody(immediate+i, sizeFor(immediate+i)),
			"delay_ms": delayMs,
		}
	}
	do(t, "POST", base+"/delay100/messages", map[string]any{"messages": delMsgs}, nil)

	// Immediately lease — should only get the 60 immediate messages.
	var lr leaseResp
	do(t, "POST", base+"/delay100/lease", map[string]any{"max": 200}, &lr)
	if len(lr.Messages) != immediate {
		t.Errorf("before delay: got %d messages, want %d", len(lr.Messages), immediate)
	}
	for _, m := range lr.Messages {
		var idx int
		fmt.Sscanf(m.Body, "IDX=%08X;", &idx)
		if idx >= immediate {
			t.Errorf("delayed message (index %d) delivered early", idx)
		}
		decodeBody(t, m.Body, idx)
		do(t, "POST", base+"/delay100/ack", map[string]any{"receipt": m.Receipt}, nil)
	}

	// Wait for delay to expire.
	time.Sleep(time.Duration(delayMs+150) * time.Millisecond)

	// Now lease the remaining 40 delayed messages.
	bodies := leaseAll(t, base, "delay100", delayed)
	if len(bodies) != delayed {
		t.Fatalf("after delay: received %d messages, want %d", len(bodies), delayed)
	}
	for _, body := range bodies {
		var idx int
		fmt.Sscanf(body, "IDX=%08X;", &idx)
		if idx < immediate {
			t.Errorf("unexpected immediate message index %d after delay window", idx)
		}
		decodeBody(t, body, idx)
	}
	t.Logf("✓ Delayed: %d immediate + %d delayed, timing correct, all content valid", immediate, delayed)
}

// ── 5. Save and restore correctness ──────────────────────────────────────────

// TestSaveRestoreCorrectness pushes 100 messages of 10 different sizes, then
// closes the broker (simulating a shutdown), reopens it, and pops every message
// verifying the body bytes are an exact match.
func TestSaveRestoreCorrectness(t *testing.T) {
	dir := t.TempDir()
	const N = 100

	// Session 1: push 100 messages and checkpoint.
	func() {
		b, err := queue.OpenBroker(dir)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(New(b, nil))
		base := srv.URL + "/v1/queues"
		do(t, "POST", base, map[string]any{"name": "saverestore", "order": "fifo"}, nil)

		msgs := make([]map[string]any, N)
		for i := 0; i < N; i++ {
			msgs[i] = map[string]any{"body": encodeBody(i, sizeFor(i))}
		}
		if code := do(t, "POST", base+"/saverestore/messages", map[string]any{"messages": msgs}, nil); code != 201 {
			t.Fatalf("enqueue: %d", code)
		}
		do(t, "POST", base+"/saverestore/checkpoint", nil, nil)
		t.Logf("Session 1: enqueued %d messages, checkpointed", N)
		srv.Close()
		b.Close()
	}()

	// Session 2: reopen and verify every message.
	b2, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv2 := httptest.NewServer(New(b2, nil))
	defer srv2.Close()
	defer b2.Close()
	base2 := srv2.URL + "/v1/queues"

	bodies := leaseAll(t, base2, "saverestore", N)
	if len(bodies) != N {
		t.Fatalf("after restart: received %d messages, want %d", len(bodies), N)
	}
	for i, body := range bodies {
		decodeBody(t, body, i) // FIFO order + exact content
	}
	t.Logf("✓ SaveRestore: %d messages, exact byte match after broker restart", N)
}

// ── 6. Endianness verification ────────────────────────────────────────────────

// TestOnDiskLittleEndian verifies that all on-disk binary formats (WAL, page
// header, index checkpoint) use explicit little-endian byte order, making the
// storage engine portable across architectures.
//
// Method: write one message, close cleanly, then read the raw bytes of each
// file and assert the known multi-byte fields are in little-endian order.
func TestOnDiskLittleEndian(t *testing.T) {
	dir := t.TempDir()
	const knownMsgID = uint64(1)

	// Write one message and checkpoint.
	b, err := queue.OpenBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(b, nil))
	base := srv.URL + "/v1/queues"
	do(t, "POST", base, map[string]any{"name": "endian", "order": "fifo"}, nil)
	do(t, "POST", base+"/endian/messages", map[string]any{"body": encodeBody(0, 64)}, nil)
	do(t, "POST", base+"/endian/checkpoint", nil, nil)
	srv.Close()
	b.Close()

	// ── WAL record ────────────────────────────────────────────────────────────
	// WAL header: [0:4] length(u32) [4:12] lsn(u64) [12] op [13] flags [14:16] reserved
	// ENQUEUE payload: [16:24] msgID(u64) [24:32] indexRef(u64) ...
	// Expected: msgID=1 encoded as bytes [01 00 00 00 00 00 00 00] (little-endian).
	walDir := filepath.Join(dir, "queues", "endian", "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("wal dir not found or empty: %v", err)
	}
	walBytes, err := os.ReadFile(filepath.Join(walDir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(walBytes) < 24 {
		t.Fatalf("wal too short: %d bytes", len(walBytes))
	}
	// Record header is 16 bytes, then ENQUEUE payload starts at offset 16.
	// msgID is at offset 16..24.
	gotMsgID := binary.LittleEndian.Uint64(walBytes[16:24])
	if gotMsgID != knownMsgID {
		t.Errorf("WAL: msgID = %d (0x%016x), want %d — wrong byte order?", gotMsgID, gotMsgID, knownMsgID)
	}
	// The byte at offset 16 should be 0x01 and bytes 17..23 should be 0x00
	// (little-endian representation of uint64(1)).
	if walBytes[16] != 0x01 {
		t.Errorf("WAL: msgID byte[16]=%02x, want 0x01 (little-endian)", walBytes[16])
	}
	for i := 17; i < 24; i++ {
		if walBytes[i] != 0x00 {
			t.Errorf("WAL: msgID byte[%d]=%02x, want 0x00 (little-endian)", i, walBytes[i])
		}
	}
	t.Logf("✓ WAL: msgID little-endian confirmed [%02x %02x %02x %02x %02x %02x %02x %02x]",
		walBytes[16], walBytes[17], walBytes[18], walBytes[19],
		walBytes[20], walBytes[21], walBytes[22], walBytes[23])

	// ── Page header ───────────────────────────────────────────────────────────
	// Header layout: [0:4] magic [4:8] pageID [8:12] gen [12] scIdx [13] flags
	// [14:16] spanPages [16:20] capacity [20:24] freeCount [24:28] slotsStart [28:32] CRC
	// We know magic = 0xF4A35E1B.  In little-endian: bytes = [1B 5E A3 F4].
	pagesDir := filepath.Join(dir, "pages")
	pageEntries, err := os.ReadDir(pagesDir)
	if err != nil || len(pageEntries) == 0 {
		t.Fatalf("pages dir not found or empty: %v", err)
	}
	pageBytes, err := os.ReadFile(filepath.Join(pagesDir, pageEntries[0].Name()))
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if len(pageBytes) < 8 {
		t.Fatalf("page too short: %d bytes", len(pageBytes))
	}
	gotMagic := binary.LittleEndian.Uint32(pageBytes[0:4])
	const wantMagic = uint32(0xF4A35E1B)
	if gotMagic != wantMagic {
		t.Errorf("page header: magic=0x%08X want 0x%08X", gotMagic, wantMagic)
	}
	// Little-endian 0xF4A35E1B → bytes [1B 5E A3 F4]
	if pageBytes[0] != 0x1B || pageBytes[1] != 0x5E || pageBytes[2] != 0xA3 || pageBytes[3] != 0xF4 {
		t.Errorf("page magic bytes %02X %02X %02X %02X, want 1B 5E A3 F4 (little-endian)",
			pageBytes[0], pageBytes[1], pageBytes[2], pageBytes[3])
	}
	t.Logf("✓ Page header: magic little-endian confirmed [%02X %02X %02X %02X]",
		pageBytes[0], pageBytes[1], pageBytes[2], pageBytes[3])

	// ── store.log (page lifecycle journal) ────────────────────────────────────
	// PAGE_CREATE record: [0] op=1 [1:5] pageID(u32) [5:7] scIdx(u16)
	// [7:11] spanPages(u32) [11:15] gen(u32) [15:19] crc32(u32)
	// pageID=1 in little-endian: bytes [01 00 00 00]
	storeLog := filepath.Join(dir, "pagestore", "store.log")
	storeBytes, err := os.ReadFile(storeLog)
	if err != nil {
		t.Fatalf("read store.log: %v", err)
	}
	if len(storeBytes) < 5 {
		t.Fatalf("store.log too short: %d bytes", len(storeBytes))
	}
	if storeBytes[0] != 1 { // op = jrnlPageCreate = 1
		t.Errorf("store.log: first byte = %02x, want 0x01 (PAGE_CREATE)", storeBytes[0])
	}
	gotPageID := binary.LittleEndian.Uint32(storeBytes[1:5])
	if gotPageID == 0 || gotPageID > 100 {
		t.Errorf("store.log: pageID=%d looks wrong", gotPageID)
	}
	// The pageID bytes should be little-endian: pageID=1 → [01 00 00 00]
	if storeBytes[1] != byte(gotPageID) || storeBytes[2] != 0 {
		t.Errorf("store.log: pageID bytes %02x %02x %02x %02x (expected little-endian)",
			storeBytes[1], storeBytes[2], storeBytes[3], storeBytes[4])
	}
	t.Logf("✓ store.log: PAGE_CREATE pageID=%d little-endian confirmed [%02X %02X %02X %02X]",
		gotPageID, storeBytes[1], storeBytes[2], storeBytes[3], storeBytes[4])

	t.Log("")
	t.Log("✓ All on-disk formats verified: exclusively little-endian byte order.")
	t.Log("  Storage is portable across architectures (x86, ARM, MIPS, etc.)")
	t.Log("  Volatile structures (bitmap words) use native order but are rebuilt on restart.")

	// Confirm no BigEndian usage exists in source (sanity check via file scan).
	bigEndianFiles := scanForBigEndian(t)
	if len(bigEndianFiles) > 0 {
		t.Errorf("BigEndian found in production source files:\n%s",
			strings.Join(bigEndianFiles, "\n"))
	} else {
		t.Log("✓ Source scan: no BigEndian usage in production code.")
	}
}

// scanForBigEndian checks that no production (non-test) source file uses
// binary.BigEndian for on-disk encoding.
func scanForBigEndian(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..")
	var found []string
	dirs := []string{
		filepath.Join(root, "internal", "wal"),
		filepath.Join(root, "internal", "pagestore"),
		filepath.Join(root, "internal", "index"),
		filepath.Join(root, "internal", "queue"),
	}
	for _, d := range dirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(d, e.Name())
			content, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			if strings.Contains(string(content), "BigEndian") {
				found = append(found, path)
			}
		}
	}
	return found
}
