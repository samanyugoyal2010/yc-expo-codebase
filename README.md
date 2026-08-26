## Copyright & Legal Notice

**Copyright © 2026 Samanyu Goyal. All Rights Reserved.**

This software, including all source code, documentation, design documents, algorithms, and related materials ("Software"), is the exclusive intellectual property of Samanyu Goyal.

**No part of this Software may be used, copied, reproduced, modified, merged, published, distributed, sublicensed, sold, or otherwise exploited — in whole or in part — without the prior explicit written permission of Samanyu Goyal.**

Any unauthorized use, reproduction, or distribution of this Software constitutes a violation of copyright law and may result in severe civil and criminal penalties under applicable law. All rights are reserved worldwide.

For licensing inquiries or written permission requests, contact the copyright holder directly.

*This notice applies to all files, components, and derivatives of this project without exception.*

# FrankenQueue

**Copyright © 2026 Samanyu Goyal — Proprietary & Confidential. No use, reproduction, or distribution without written permission.**

A self-contained, durable HTTP message queue that composes three orthogonal behaviours — **FIFO / LIFO**, **priority**, and **delayed delivery** — in any combination. No external database, broker, or dependency. One Go binary.

## Quick Start

```bash
# Build and run
go build -o frankenqueue .
./frankenqueue -addr :8080 -data ./data

# Or run directly
go run . -addr :8080 -data ./data

# Demo UI
open http://localhost:8080
```

**Dependencies:** Go 1.21+ only. Zero external packages.

## Code Architecture

```
frankenqueue/
├── main.go                    Entry point, signal handling, graceful shutdown
├── api.go / web.go            Top-level wiring (build-ignored copies)
│
├── internal/
│   ├── api/                   HTTP layer
│   │   └── api.go             REST handler: routes every request to broker/queue methods
│   │
│   ├── queue/                 Ordering engine + durability
│   │   ├── broker.go          Manages named queues, shared page store, recovery phases
│   │   ├── queue.go           Per-queue: enqueue, lease, ack, nack, expiry, recovery
│   │   ├── heap.go            Max-heap: primary=priority, secondary=msgID×dir (FIFO/LIFO)
│   │   ├── wheel.go           Timing wheel: delayed messages + inflight lease expiry
│   │   └── config.go          Queue config, delivery/dead-letter types, stats
│   │
│   ├── pagestore/             Payload storage
│   │   ├── pagestore.go       SlotRef: compressed 8-byte address (pageID<<20 | offset)
│   │   ├── store.go           Allocator: slab pages + jumbo spans, global lock
│   │   ├── page.go            Bitmap-based slot allocation (lock-free atomic CAS)
│   │   ├── header.go          Uniform 32-byte page header, size classes, CRC
│   │   └── journal.go         store.log: PAGE_CREATE / PAGE_RETIRE lifecycle journal
│   │
│   ├── index/                 Queue message index
│   │   └── index.go           Page-store-backed index: SlotRef = index slot address
│   │
│   ├── wal/                   Write-ahead log
│   │   ├── wal.go             Append-only, segmented, group-commit committer goroutine
│   │   └── record.go          8 op types: ENQUEUE, LEASE, ACK, NACK, EXPIRE, DEAD, MOVE_REF, CONFIG
│   │
│   └── types/
│       └── types.go           Order enum (FIFO/LIFO), shared constants
│
└── web/
    └── web.go                 Embedded static UI (served at /)
```

### Key design decisions

**SlotRef — compressed 8-byte page reference**

`uint64 = pageID (44 bits) << 20 | offset (20 bits)`

Pages are 1 MiB-aligned so any slot address encodes its own page. The page header is found by masking the bottom 20 bits — no global table, no lock. Size is not in the ref; callers track `payloadLen` separately.

**Batch commit (group commit for enqueue).** N concurrent producers each write their payloads then submit to a single `runCommitter` goroutine. When `commitBatchSize=64` messages accumulate, or `commitLagDuration=5ms` elapses, the committer does:

1. One fsync per unique dirty payload page
2. One WAL `AppendBatch` (WAL's own group commit adds a second fsync)
3. Wakes all N waiting producers

Result: N messages share 2 fsyncs instead of N×2.

**Index on page store.** The message index (metadata per message) lives in the same page store as payloads — 64-byte slab slots. `AllocSlot()` → `pages.Alloc(64)`. `checkpointLSN` is snapshotted to `index.dat` for fast restarts.

**WAL segments + checkpoint.** WAL rotates to a new segment file every 64 MiB. Checkpoints fire automatically when WAL grows past **32 MiB** or **5 minutes** elapse (whichever comes first), writing `index.dat` and truncating fully-superseded WAL segments.

## How to Run

### Binary flags

| **Flag** | **Default** | **Description** |
|:---------|:------------|:----------------|
| `-addr`  | `:8080`     | Listen address |
| `-data`  | `./data`    | Data directory (created if absent) |

### Data directory layout

```
data/
├── pagestore/
│   └── store.log              Page lifecycle journal (PAGE_CREATE / PAGE_RETIRE)
├── pages/
│   ├── 000000001.page         1 MiB slab page (64-byte slots)
│   └── 000000007.page         8 MiB jumbo span
└── queues/
    └── jobs/
        ├── queue.meta         Durable queue config (JSON)
        ├── wal/
        │   └── 0000000000000000001.wal
        └── index.dat          Checkpoint of current message state
```

### Example session

```bash
# Create a priority FIFO queue
curl -XPOST localhost:8080/v1/queues \
  -d '{"name":"jobs","order":"fifo","max_attempts":3,"visibility_ms":30000}'

# Enqueue a batch of 512 messages (one HTTP call)
curl -XPOST localhost:8080/v1/queues/jobs/messages \
  -d '{"messages":[{"body":"task-1","priority":9},{"body":"task-2","priority":1}]}'

# Lease up to 20 messages (long-poll 5s if empty)
curl -XPOST localhost:8080/v1/queues/jobs/lease \
  -d '{"max":20,"wait_ms":5000}'

# Ack by receipt
curl -XPOST localhost:8080/v1/queues/jobs/ack \
  -d '{"receipt":"1:1:a3f9c2b1..."}'

# Nack (requeue after 10s)
curl -XPOST localhost:8080/v1/queues/jobs/nack \
  -d '{"receipt":"...","delay_ms":10000}'

# Force checkpoint
curl -XPOST localhost:8080/v1/queues/jobs/checkpoint

# Page store stats
curl localhost:8080/v1/stats
```

## HTTP API Reference

| **Method** | **Path** | **Description** |
|:---|:---|:---|
| POST | `/v1/queues` | Create queue (`name`, `order`, `max_attempts`, `visibility_ms`, `max_delay_ms`, `age_boost_ms`, `max_depth`, `durable_leases`) |
| GET | `/v1/queues` | List all queues with config + stats |
| DELETE | `/v1/queues/{q}` | Delete queue and reclaim all pages |
| POST | `/v1/queues/{q}/messages` | Enqueue one or a batch (`messages:[...]`) |
| POST | `/v1/queues/{q}/lease` | Lease up to `max` messages; `wait_ms` for long poll |
| POST | `/v1/queues/{q}/ack` | Acknowledge by receipt |
| POST | `/v1/queues/{q}/nack` | Return for redelivery (`delay_ms` optional) |
| GET | `/v1/queues/{q}/dead` | List dead-letter messages |
| POST | `/v1/queues/{q}/replay` | Re-enqueue dead letters (all or by ids) |
| POST | `/v1/queues/{q}/checkpoint` | Force checkpoint + WAL truncation |
| GET | `/v1/queues/{q}/stats` | Queue depths and lifetime counters |
| GET | `/v1/stats` | Page store pages, live bytes, total bytes |

## FAQ

**How do you handle replay messages?** Replay takes dead-lettered messages off the DLQ, re-enqueues them as brand-new messages (same payload and priority, optional delay, fresh IDs and attempt counts), and only frees the old slots if that enqueue succeeds — if it fails, they go right back on the dead list.

**How would you refactor your queue into a Pub/Sub?** I'd keep the WAL, page store, and lease/ack machinery, then fan a published message out into per-subscriber queues (or per-subscriber cursors on a shared log) so each consumer still gets its own delivery, retry, and dead-letter story instead of competing for one pop.

**If you had more time, what other features would you add?** Kill-9 crash tests, poison-pill inspection in the UI, per-queue metrics, idempotent producer keys, and a simple consumer group so you can scale readers without inventing a whole new broker.

**Why would users choose this over Amazon SQS, RabbitMQ, or Apache Pulsar?** They wouldn't pick it for a company-wide bus — those already won on ops, fan-out, and scale — they'd pick it when they want one Go binary, a store they actually own, and FIFO/LIFO plus priority plus delay composed in one queue without standing up a cluster.

## Performance

Measured on Apple M-series (single process, httptest.Server, macOS SSD). All enqueue results are **fully durable** (payload fsync + WAL fsync before returning to the caller). All delivery results verified **zero missed messages**.

### Push Rate — pure ingestion speed

8 producers · 512 msgs/producer · `TestPushRate`

| **Batch size** | **Payload** | **Enqueue QPS** | **Throughput** |
|:---------------|:------------|:----------------|:---------------|
| 1 (baseline)   | 64 B        | 778             | 0.05 MB/s      |
| 1 (baseline)   | 1 KB        | 782             | 0.76 MB/s      |
| 1 (baseline)   | 16 KB       | 735             | 11.5 MB/s      |
| 8              | 64 B        | 6,212           | 0.38 MB/s      |
| 8              | 1 KB        | 5,994           | 5.85 MB/s      |
| 8              | 16 KB       | 3,342           | 52.2 MB/s      |
| 64             | 64 B        | 43,227          | 2.64 MB/s      |
| 64             | 1 KB        | 32,933          | 32.2 MB/s      |
| 64             | 16 KB       | 6,067           | **94.8 MB/s**  |
| 512            | 64 B        | 208,913         | 12.8 MB/s      |
| 512            | 1 KB        | 77,141          | 75.3 MB/s      |
| 512            | 16 KB       | 6,442           | **100.7 MB/s** |
| 1024           | 64 B        | **245,940**     | 15.0 MB/s      |
| 1024           | 1 KB        | 70,880          | 69.2 MB/s      |
| 1024           | 16 KB       | 7,277           | **113.7 MB/s** |

**How to read this:**

- **Batch=1 (baseline):** ~750 QPS regardless of payload size — pure fsync limit. Each HTTP call triggers one batch-commit cycle (payload fsync + WAL fsync).
- **Batch=8:** 8 messages share the same 2 fsyncs → ~8× improvement.
- **Batch=64:** 43K QPS for small messages — group commit at full efficiency.
- **Batch=512–1024:** 246K msgs/sec for 64-byte payloads; **113 MB/s** for 16 KB. At large payloads the ceiling shifts from fsync count to disk bandwidth.

**The invariant never breaks:** payload is always on disk before the WAL record is written, regardless of batch size.

### Enqueue + Ack (round-trip, with consumers)

8 producers · 4 consumers · 4 queues · `TestConcurrentLoadTable`

#### Baseline — 1 message per HTTP call (500 msgs/producer)

| **Payload** | **Enqueue QPS** | **Ack QPS** | **Throughput** | **p99 Latency** |
|:------------|:----------------|:------------|:---------------|:----------------|
| 64 B        | 476             | 357         | 0.02 MB/s      | 19 ms           |
| 256 B       | 461             | 358         | 0.09 MB/s      | 19 ms           |
| 1 KB        | 457             | 358         | 0.35 MB/s      | 18 ms           |
| 4 KB        | 461             | 349         | 1.36 MB/s      | 19 ms           |
| 16 KB       | 401             | 345         | 5.39 MB/s      | 18 ms           |

#### Batch — 512 messages per HTTP call (512 msgs/producer, 1 HTTP call each)

| **Payload** | **Enqueue QPS** | **Ack QPS** | **Throughput** | **p99 Latency** |
|:------------|:----------------|:------------|:---------------|:----------------|
| 64 B        | **118,833**     | 345         | 0.02 MB/s      | 18 ms           |
| 256 B       | **110,989**     | 354         | 0.09 MB/s      | 18 ms           |
| 1 KB        | **63,325**      | 351         | 0.34 MB/s      | 18 ms           |
| 4 KB        | **21,047**      | 349         | 1.36 MB/s      | 18 ms           |
| 16 KB       | **6,353**       | 341         | 5.32 MB/s      | 18 ms           |

**Why enqueue jumps 250× but ack stays at ~350 QPS:**

- Batch commit amortises 2 fsyncs across N messages → enqueue scales with N
- Each ack requires its own WAL fsync (durable removal) → independently limited
- Batch ack API would apply the same pattern; not yet implemented

**p99 = 18–19 ms** — the group-commit window: committer waits up to 5 ms for a full batch, then two fsyncs (~1–2 ms each on NVMe).

## Durability Guarantees

| **Operation** | **Guarantee** |
|:---|:---|
| **Enqueue** | Payload fsynced → WAL fsynced → client told 201. Crash before either: message never existed. |
| **Ack** | WAL fsynced before 200. Crash before: message redelivered (at-least-once). |
| **Dead-letter** | WAL fsynced. Crash before: message retried again. |
| **Lease** | Async WAL append (no fsync). Crash: all inflight → ready. |
| **Nack / expire** | Async WAL append. Crash: recovered as ready. |
| **Restart** | WAL replayed from `checkpointLSN`; index rebuilt; page store volatile state rebuilt from live message refs. |

## Test Infrastructure

### Run tests

```bash
# All packages, race detector on
go test -race ./...

# Full load test (takes ~5 min)
go test -v -timeout 300s -run TestConcurrentLoadTable ./internal/api/

# Smoke load test
go test -v -run TestLoadSingleQueue ./internal/api/

# Specific package
go test -race -v ./internal/pagestore/
```

### Test count by package

| **Package** | **Tests** | **What is covered** |
|:---|:---|:---|
| `internal/pagestore` | 31 | Header encode/decode/CRC, bitmap alloc/free, slab+jumbo alloc, auto-reclaim, stale ref, save+restore, rebuild volatile, concurrent alloc/release (`-race`), 1M-record stress, random sizes shrink-to-zero, sliding-window peak memory, fragmentation recovery |
| `internal/wal` | 11 | All 8 op types encode/decode round-trip, CRC mismatch detection, single + batch append, replay with LSN filter, segment rotation, torn-tail truncation, WAL truncate prefix, close+reopen |
| `internal/index` | 12 | Entry wire size, alloc+put+get, free removes slot, len, `ScanPayloadPage` filter, scan all, checkpoint save+restore, corrupt fallback, slot field round-trip, CRC mismatch rejects checkpoint, concurrent alloc/put/free (`-race`), checkpoint atomicity |
| `internal/queue` | ~8 | (from `queue_test.go`) |
| `internal/api` | 11 | FIFO lifecycle, priority+LIFO+delay ordering, batch enqueue+ack, nack→dead-letter→replay, long-poll wakeup, bad receipt 409, over-horizon delay 400, max-depth 429, server restart (messages survive), restart+page-store rebuild verification |
| **Load tests** | 2 | 8 concurrent producers × 4 queues: baseline (1 msg/call) and batch-512, 5 payload sizes each, zero-missed-message assertion |
| **Total** | **~65+** | |

### Test infrastructure features

- **Race detector** (`-race`) on all packages — no races in any test
- **httptest.Server** — full HTTP stack, no mocking, real TCP
- **t.TempDir()** — isolated data directory per test, auto-cleaned
- **Integrity verification** — load tests tag every message and assert 100% delivery
- **Crash simulation** — `TestRestartWithPageStoreVerification` and `TestQueueSurvivesServerRestart` close broker without clean shutdown and reopen
- **Concurrent stress** — `TestConcurrentAllocRelease`, `TestBitmapConcurrent`, `TestConcurrentAllocPutFree` all run with `-race`

## Layout

```
.
├── main.go              Server entry, signal handling, graceful shutdown
├── api.go               Top-level HTTP server wiring
├── web.go               Embedded static UI assets
├── internal/
│   ├── api/             HTTP handlers (thin translation layer)
│   ├── queue/           Ordering engine, broker, recovery, checkpoints
│   ├── pagestore/       1 MiB-aligned payload storage, SlotRef, journal
│   ├── index/           Page-store-backed message index
│   ├── wal/             Append-only log, group commit, segment rotation
│   └── types/           Shared enums and constants
└── web/                 Demo UI (served at /)
```
