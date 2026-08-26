# FrankenQueue — Complete Design (rev 3)

A single self-contained HTTP service implementing a *composable* queue: **FIFO | LIFO** × **priority** × **delay**, in any combination, with its own durable storage engine. No external database, no external broker.

| **Requirement** | **How** |
|:---|:---|
| FIFO / LIFO | one comparator with a direction sign (§5) |
| Priority | primary comparator key (§5) |
| Delay | eligibility predicate via timing wheel, orthogonal to ordering (§5) |
| Durable across restarts | per-operation durability points (§6), WAL replay (§7) |
| Storage not delegated | own WAL + own index + own page allocator, plain files (§3) |
| Concurrency | per-queue sharded locks + atomics + group commit (§9) |

### Revision history

**Rev 2** reworked persistence: WAL is the sole durable truth, the index is a rebuildable acceleration structure, the heap/wheels are volatile.

**Rev 3** fixes twelve defects found auditing rev 2:

| **#** | **Defect in rev 2** | **Fix** |
|:---|:---|:---|
| 1 | "sealed immutable + compressed" pages contradicted in-place slot reuse; compression also broke offset reads and hole punching | pages mutable while live; compression opt-in, off by default, mutually exclusive with reuse/punch (§3.5) |
| 2 | slot = msg_id % capacity collides — one long-lived message pins a slot forever | slot ids allocated from a freelist, recorded in ENQUEUE (§3.4) |
| 3 | shared page store × per-queue WAL: cross-queue compaction, refcount rebuild ordering, and queue deletion all undefined | global page-store journal + phased global recovery + journaled deletion (§3.6, §7, §8.5) |
| 4 | checkpoint-gated slot reuse was over-strict | gate on the durable killing record; checkpoint only governs WAL truncation (§8) |
| 5 | generation mismatch during recovery silently dropped committed messages | it is a "cannot happen" state → fail loudly (§7) |
| 6 | page generation was mutable durable state with no journal; global watermark stalled on idle queues | generations journaled; idle queues forced to checkpoint (§3.6, §7.2) |
| 7 | receipt validation unspecified → stale receipt could ack another consumer's work | receipt = (msg_id, attempt, nonce), must match the current lease (§6.4) |
| 8 | u32 ms deltas gave a 49.7-day horizon that rebasing could not extend | absolute i64 ms in the index; delay capped by policy, not by representation (§3.4, §5.3) |
| 9 | comparator referenced seq, which rev 2 had deleted | seq ≡ msg_id, stated explicitly (§5) |
| 10 | starvation under LIFO/priority never mentioned | acknowledged + optional age boost (§5.4) |
| 11 | max_attempts implied exact, but LEASE/NACK are volatile | documented as best-effort with bounds (§6.5) |
| 12 | producer-side enqueue duplicates unstated | stated next to the at-least-once claim (§6.6) |

## 1. The persistence model, stated once

Three tiers with strictly different roles. Everything else follows.

```
┌───────────────────────────────────────────────────────────────────────────┐
│ VOLATILE ready heap · delayed wheel · inflight wheel · receipts │
│ page refcounts · slab freelists · slot freelist │
│ rebuilt from scratch on every start. Never persisted. │
├───────────────────────────────────────────────────────────────────────────┤
│ REBUILDABLE index.dat — fixed-slot current-state array. │
│ A checkpoint. Deleting it costs replay time, never │
│ correctness. │
├───────────────────────────────────────────────────────────────────────────┤
│ DURABLE TRUTH queues/*/wal.log — message state transitions │
│ pagestore/store.log — page lifecycle transitions │
│ pages/*.page — payload bytes │
│ append-only, immutable, never updated in place │
└───────────────────────────────────────────────────────────────────────────┘
```

Rev 2 kept one piece of durable *mutable* state outside the log — the page generation counter in each page header — which is the same contradiction the WAL split was meant to remove. Rev 3 journals it (§3.6). The rule is now absolute: **every durable state transition is an append to some log; nothing durable is mutated in place except payload bytes, whose lifecycle is itself journaled.**

## 2. Architecture

HTTP clients (producers / consumers / demo app)

```
│
╔══════════════════════════════════════════════════════════════════════════╗
║ HTTP API (net/http, JSON) ║
║ POST /queues · /queues/{q}/messages · /lease · /ack · /nack · /replay ║
╚══════════════════════════════════════════════════════════════════════════╝
│
╔══════════════════════════════════════════════════════════════════════════╗
║ BROKER map[queueName] *Queue ║
║ one mutex per queue → queues never contend ║
╚══════════════════════════════════════════════════════════════════════════╝
│ │
┌──────┴───────────────┐ ┌──────┴───────────────┐
│ Queue "jobs" │ │ Queue "emails" │ … one per queue
│ ── volatile ── │ │ ── volatile ── │
│ ready heap │ │ ready heap │
│ (−priority,seq×dir)│ │ │
│ delayed wheel │ │ delayed wheel │
│ inflight wheel │ │ inflight wheel │
│ receipts map │ │ receipts map │
│ slot freelist │ │ slot freelist │
│ ── durable ── │ │ ── durable ── │
│ queue.meta │ │ queue.meta │
│ wal.log (truth) │ │ wal.log (truth) │
│ index.dat (cache) │ │ index.dat (cache) │
└──────────┬───────────┘ └──────────┬───────────┘
│ │
└────────────┬─────────────┘
▼
╔═════════════════════════════════════════════════════╗
║ shared PAGE STORE ║
║ pages/*.page 1 MiB-aligned slabs, jumbo spans║
║ pagestore/store.log journal: CREATE/RETIRE/SWEEP ║
║ volatile: refcounts · freelists · owner-queue sets║
╚═════════════════════════════════════════════════════╝
```

Nothing in the hot path crosses a process boundary. **The storage engine *is* the service** — the "storage cannot be delegated" requirement, met by construction.

## 3. On-disk layout

data/

manifest # format version, next page id, next queue id

pagestore/

store.log # journal of page lifecycle transitions

pages/

000000001.page # 1 MiB, size class 64 B

000000007.page # 8 MiB JUMBO span, still 1 MiB-aligned

queues/

jobs/

queue.meta # durable queue configuration

wal.log # append-only truth for this queue

wal.log.1 # older segments, retained until checkpointed

index.dat # current-state checkpoint + checkpoint_lsn

emails/

…

Per-queue directories (rather than a queue_id column) keep recovery, deletion, retention and corruption isolation independent per queue. The page store is shared for slab efficiency, and §3.6/§7/§8.5 pay the coordination cost that sharing implies — rev 2 asserted the benefit without paying it.

### 3.1 queue.meta

Written and fsynced at creation; rewritten atomically (temp → fsync → rename → fsync dir) on reconfiguration, always preceded by a CONFIG record in the WAL so ordering against message operations is defined.

{ name, order: fifo|lifo, max_attempts, default_visibility_ms,

max_delay_ms (default 30 d, hard cap — see §5.3),

retention_ms, max_payload_bytes, dlq_target,

durable_leases: bool (default false),

aging_ms: 0 = disabled (see §5.4),

compress_sealed: bool (default false — see §3.5),

created_at, format_version }

### 3.2 wal.log — the only durable truth for messages

Append-only, immutable, segmented.

off sz field

0 4 length u32

4 8 lsn u64 monotonic per queue

```
12 1 op u8 ENQUEUE│LEASE│ACK│NACK│EXPIRE│DEAD│MOVE_REF│CONFIG
```

13 1 flags u8

14 2 reserved

16 … op-specific payload

… 4 crc32 u32 over the whole record

ENQUEUE { msg_id u64, slot_id u32, page_id u32, page_gen u32, offset u32,

size u32, priority u8, enqueued_at_ms i64, available_at_ms i64 }

LEASE { msg_id u64, nonce u64, attempt u16, lease_until_ms i64 }

ACK { msg_id u64, nonce u64 }

NACK { msg_id u64, nonce u64, requeue_at_ms i64 }

EXPIRE { msg_id u64, nonce u64 }

DEAD { msg_id u64, reason u8 }

MOVE_REF { msg_id u64, old{page_id,gen,offset}, new{page_id,gen,offset} }

CONFIG { … }

msg_id comes from a per-queue atomic counter and is monotonic — which is also what supplies the arrival sequence for FIFO/LIFO ordering (§5). slot_id is the *internal* index position and is journaled so replay is deterministic (§3.4). Recovery is a pure fold of these transitions over an empty state.

### 3.3 Index slot — 56 B, absolute timestamps

IndexSlot (56 B, fixed stride, array position == slot_id)

msg_id u64 0 = free slot

enqueued_at_ms i64 absolute

available_at_ms i64 absolute

lease_until_ms i64 absolute, 0 = not leased

page_id u32

page_gen u32

offset u32

size u32

priority u8

```
state u8 DELAYED│READY│INFLIGHT│ACKED│DEAD
```

attempts u16

crc32 u32

Rev 2 packed this into 32 B using u32 millisecond deltas from a rebased epoch. That was a false economy: the horizon is 49.7 days, and rebasing moves the base *forward*, so it extends the past, not the future — a long delay was unrepresentable at enqueue no matter when you rebased. Absolute i64 costs 24 B per message (56 MB per million) and removes an entire class of bug. enqueued_at_ms is also now kept, so age metrics and retention_ms still work after a checkpoint has superseded the WAL records.

### 3.4 Slot allocation

Rev 2 used slot = msg_id % capacity, which collides: msg_id and msg_id + capacity share a slot, and a single long-lived low-priority message — exactly what priority and LIFO queues produce — pins its slot while ids march past. Growth by rewrite relieves capacity pressure, not collisions.

Instead, slot_id is allocated from an in-memory freelist (rebuilt at recovery by scanning for msg_id == 0) and **written into the ENQUEUE record**, so replay reconstructs exactly the same layout. msg_id remains the external identity; slot_id is purely internal. The array doubles by rewrite when occupancy exceeds 75 %.

### 3.5 Payload pages

Pages are 1 MiB-aligned — jumbo spans included — so the header is found by masking. One AND, no back-pointers, no global page table, therefore no lock on one:

hdr := (*PageHeader)(unsafe.Pointer(uintptr(p) &^ (PageSize - 1)))

type PageHeader struct { // durable fields are write-once per generation

magic uint32

pageID uint32

generation uint32 // journaled (§3.6); never bumped silently

sizeClass uint16

```
flags uint16 // JUMBO │ COMPRESSED
```

spanPages uint32

headerCRC uint32

}

Note what is **absent** versus rev 2: refcount, liveBytes and freeHead are no longer in the durable header. They are volatile, rebuilt at recovery from the union of all queues' live sets (§7). Keeping mutable counters in a durable header was the rev-2 mistake that made crash semantics ambiguous.

The payload region is pure bytes — no inline record header, no owner tag, no per-record CRC. Owners are found on the rare compaction path by index scan (§8.5). Paying 4–16 B per record forever to accelerate a cold path is the wrong trade; at the 64 B size class it would have been 6–25 % overhead. Free slots thread the CAS freelist through the dead slot's own bytes, costing nothing while occupied.

**Compression, corrected.** Rev 2 claimed sealed pages were immutable *and* LZ4-compressed while simultaneously reusing freed slots in place — mutually exclusive — and compression additionally breaks offset addressing (a ref points into *uncompressed* bytes) and hole punching. Rev 3:

- A page is **mutable and uncompressed while it has any live slot.** Slot reuse in place (§8.1) and hole punching (§8.2) apply only to such pages.

- compress_sealed is **opt-in per queue and off by default**. When enabled, a page is compressed only once compaction has emptied it of reusable capacity — i.e. compression is an archival path, reached via §8.4, and a compressed page is only ever reclaimed whole. Reads inflate the page into a bounded LRU cache.

That keeps compression available for retention/replay workloads without letting it contradict the allocator.

### 3.6 pagestore/store.log — page lifecycle journal

Page *generations* must survive restart and cannot be rebuilt, so they are journaled rather than mutated in place:

PAGE_CREATE { page_id u32, size_class u16, span_pages u32, generation u32 }

PAGE_RETIRE { page_id u32, old_gen u32, new_gen u32 }

QUEUE_DROP_BEGIN { queue_id u32 }

QUEUE_DROP_END { queue_id u32 }

Written and fsynced before the corresponding physical action (create, retire, delete). Everything else about the page store — refcounts, freelists, occupancy, owner-queue sets — is volatile and reconstructed (§7).

## 4. Go type skeleton

Go has no classes; behavior composes from structs and implicitly-satisfied interfaces. The WAL is append-only *in its type signature* — there is no Update.

type WAL interface {

Append(rec Record) (lsn uint64, err error)

AppendBatch(recs []Record) (lsn uint64, err error) // one fsync, N records

Replay(from uint64, fn func(lsn uint64, rec Record) error) error

TruncatePrefix(upTo uint64) error // post-checkpoint only

DurableLSN() uint64 // gates slot reuse (§8)

Sync() error

}

type Index interface { // in-memory; persisted only at checkpoint

Get(msgID uint64) (IndexSlot, bool)

Put(slot IndexSlot) // mutates memory, never the file

AllocSlot() uint32

FreeSlot(id uint32)

Checkpoint(lsn uint64) error // temp → fsync → rename → fsync dir

CheckpointLSN() uint64

ScanPage(pageID uint32, fn func(IndexSlot))

}

type PageStore interface { // shared across queues

Alloc(size uint32) (Ref, []byte, error)

Read(ref Ref) ([]byte, error)

Release(ref Ref, killedAtLSN uint64, q *Queue) // → reclaimable

SyncBatch(refs []Ref) error

}

type Order uint8; const (FIFO Order = iota; LIFO)

type Queue struct {

meta QueueMeta

mu sync.Mutex // guards the volatile tier only

ready *readyHeap

delayed *timingWheel

inflight *timingWheel

receipts map[uint64]lease // msg_id → {nonce, attempt, until}

wal WAL

index Index

pages PageStore

}

## 5. The composable ordering engine

**Ordering** and **eligibility** are separate concerns. Keeping them separate is the core of the design, and it is the one part unchanged since rev 1.

Ordering is one comparator producing all four behaviors. The arrival sequence is msg_id itself, which is monotonic per queue — rev 2 deleted the separate seq field but left the comparator referring to it:

// seq ≡ msg_id: monotonic per queue, so it *is* the arrival sequence.

func (h *readyHeap) Less(i, j int) bool {

a, b := h.at(i), h.at(j)

pa, pb := h.effPriority(a), h.effPriority(b) // §5.4 aging, if enabled

if pa != pb {

return pa > pb // priority is primary

}

return int64(a.msgID)*h.dir < int64(b.msgID)*h.dir // +1 FIFO, −1 LIFO

}

Eligibility is *not* a sort key. A delayed message is simply **not in ready yet**; it sits in a timing wheel keyed by available_at_ms and is promoted when due.

> Why not fold delay into the priority key? They are independent dimensions. Sorting by available_at destroys priority. Sorting by (priority, available_at) lets the heap head be a high-priority message not due for ten minutes while a ready low-priority message starves behind it — dispatch would scan past every ineligible entry, O(n) per pop, head-of-line blocked. Collapsing a *filter* into a *sort* is the bug. Orthogonality is what makes "delayed priority LIFO" fall out for free.

### 5.1 Timing wheels for the time-keyed sets

delayed and inflight are keyed by monotonically advancing time, so a hierarchical hashed timing wheel (Kafka/Netty style) gives **O(1) amortised** insert and promote against O(log n) for a heap, and its buckets align with lifetime-bucketed page allocation (§8.3). Cascades are performed in bounded slices so a coarse-bucket cascade cannot hold the queue mutex for long.

### 5.2 Lifecycle

enqueue

```
│
```

delay_ms > 0 ?

```
┌─────┴─────┐
▼ ▼
┌──────────┐ ┌────────┐ lease ┌───────────┐ ack ┌─────────┐
│ DELAYED │──▶│ READY │─────────▶│ INFLIGHT │───────▶ │ ACKED │
│ wheel │due│ heap │ │ wheel │ │ rc−− │
└──────────┘ └────────┘ └───────────┘ └─────────┘
▲ │ │
│ nack / expire │ ▼
└────────────────────┤ reclaim (§8)
│ attempts > max_attempts
▼
┌─────────────┐
│ DEAD → DLQ │
└─────────────┘
```

### 5.3 Time resolution

The API speaks delay_ms / visibility_ms; the WAL and the index both store absolute i64 milliseconds, so there is no resolution or horizon mismatch anywhere (§3.3). max_delay_ms (default 30 days) is a **policy** cap enforced at the API with a 400, not a limit imposed by the encoding — which is the distinction rev 2 got wrong.

### 5.4 Starvation is real, and named

LIFO and priority starve by construction, and nack makes it worse: a nacked message re-entering a LIFO queue sorts to the very back and may never be redelivered while traffic continues. This is inherent to the semantics the user asked for, so the design does not pretend otherwise. Mitigation is opt-in:

func (h *readyHeap) effPriority(s IndexSlot) int {

if h.agingMs == 0 { return int(s.priority) } // disabled by default

age := h.now - s.enqueuedAtMs

return min(int(s.priority)+int(age/h.agingMs), 255) // bounded boost

}

aging_ms is per queue. Because the effective key drifts with time, promotion is applied on the wheel tick (messages are re-sifted in bounded batches) rather than continuously, which keeps the comparator cheap and the heap valid.

## 6. Durability points — one per operation

| **Op** | **WAL record** | **fsync before success?** | **Crash before durability** | **Crash after** |
|:---|:---|:---|:---|:---|
| **Enqueue** | ENQUEUE | **Yes** (group-committed) | message never existed; producer retries (§6.6) | recovers as READY/DELAYED |
| **Lease** | LEASE | No (async, batched) | recovers as READY → redelivered | recovers as READY anyway (§6.3) |
| **Ack** | ACK | **Yes** | recovers as READY → redelivered (at-least-once) | acked; payload reclaimable |
| **Nack** | NACK | No | recovers as READY, attempts may lose an increment | requeued at requeue_at_ms |
| **Expire** | EXPIRE | No | recovers as READY — identical outcome | same |
| **Dead-letter** | DEAD | **Yes** | recovers as READY → retried again | stays dead |
| **Config** | CONFIG + meta rewrite | **Yes** | old config in force | new config in force |
| **Compaction** | MOVE_REF | **Yes** | old location still referenced — harmless | new location referenced |

Two rules generate the table:

1.  **Anything that removes a message from future delivery must be durable before the client is told it happened** — ACK, DEAD. Otherwise the system forgets a decision the client believes is final.

2.  **Anything whose loss merely causes redelivery may be volatile** — LEASE, NACK, EXPIRE. At-least-once already permits redelivery, so an fsync buys nothing.

### 6.1 The commit-batch protocol

1\. allocate payload locations for the batch (CAS, lock-free)

2\. write payload bytes into pages

3\. flush + fsync every dirty payload file in the batch

4\. append ENQUEUE records to wal.log

```
5\. fsync wal.log ◀── COMMIT POINT
```

6\. publish msg_ids into ready heap / delayed wheel (per-queue mutex)

7\. reply 200 to all producers in the batch

> **Invariant: a durable WAL record can never reference payload bytes that were not already durable.** Step 3 strictly precedes step 5.

Payload allocation (step 1) is deliberately *not* journaled. If the process dies between 1 and 5, no ENQUEUE record exists, recovery never learns of the slot, and the refcount/freelist rebuild (§7) reclaims it automatically. Unjournaled allocation is safe precisely because allocator state is volatile and derived.

Steps 3 and 5 are group-committed: a committer goroutine drains a pending channel, issues one fsync per dirty file plus one for the WAL, then releases every waiter.

```
producers ──▶ [ pending ] ──▶ committer ──▶ fsync payload files
│ fsync wal.log
└────▶ close(waiter.done) × N
```

An uncontended Go mutex is ~20 ns; an fsync is 0.1–10 ms — five orders of magnitude. Batching syncs is worth far more than lock-free data structures, and that is why §9 looks the way it does.

### 6.2 What is *not* claimed

Rev 1 asserted a 32 B aligned record is atomic at the device level. No mainstream storage stack contracts to that; the claim is withdrawn. **CRC32 on every record is the sole torn-write and corruption detection mechanism.**

### 6.3 Lease durability

Leases are **not** durable by default and **do not survive restart**. A broker that died was holding messages no consumer will ack, so recovery returns every INFLIGHT message to READY. This is correct at-least-once behavior, it makes recovery a pure fold, and it removes an fsync from the hottest consumer path. queue.meta.durable_leases opts in per queue.

### 6.4 Receipts

A receipt is (msg_id, attempt, nonce) where nonce is 64 random bits minted per lease and journaled in the LEASE record. **Ack/nack are valid only if the nonce matches the message's current lease.** Without this, the sequence "consumer A's lease expires → B leases the message → A acks late" would ack B's in-flight work; with it, A's stale receipt is rejected with 409 Conflict.

Because leases are volatile, *every* receipt issued before a restart is stale afterwards, and those acks also return 409 — a defined, documented outcome rather than a 500.

### 6.5 max_attempts is best-effort

LEASE and NACK are volatile, so attempts can lose increments across a crash. A poison message may therefore be retried more than max_attempts times before dead-lettering. The bound that *is* guaranteed: attempts never exceed max_attempts within any single uninterrupted broker lifetime, and a message is never dead-lettered *early*. Turning on durable_leases makes the count exact at the cost of an fsync per lease.

### 6.6 Delivery semantics, stated plainly

**At-least-once, in both directions.** Consumers may see a message more than once (redelivery after crash or lease expiry). Producers may *create* a message more than once: a crash between the client's request and the commit point forces a retry the broker cannot distinguish from a new enqueue. Exactly-once is not offered — it is unachievable without idempotency at both ends, and claiming it would be dishonest. Idempotency keys with a dedup window are the natural fix and are listed under future work (§12).

## 7. Recovery

Because the page store is shared but WALs are per-queue, recovery is **phased globally**. Rev 2 described refcount rebuild inside per-queue recovery, which cannot work — no queue knows the whole live set.

PHASE 0 — global

· read manifest; replay pagestore/store.log

PAGE_CREATE → page inventory + authoritative generation

PAGE_RETIRE → generation bump (this is why generations are journaled)

QUEUE_DROP_BEGIN without END → resume the sweep (§8.5)

· no allocation is permitted yet

PHASE 1 — per queue, in parallel

1\. load queue.meta (fail closed on CRC/format error)

2\. load index.dat → state + checkpoint_lsn

unreadable/corrupt → discard, checkpoint_lsn = 0

3\. replay wal.log from checkpoint_lsn:

CRC mismatch / short record at the tail

→ truncate; it was never acknowledged

CRC mismatch mid-log → HARD FAIL, surface to the operator

fold each op:

ENQUEUE → DELAYED | READY (slot_id taken from the record)

LEASE → INFLIGHT

ACK → ACKED

NACK → DELAYED | READY

EXPIRE → READY

DEAD → DEAD

MOVE_REF → rewrite the page reference (ignore if msg already dead)

4\. INFLIGHT → READY (leases do not survive, §6.3)

5\. validate every surviving page ref against Phase-0 generations:

mismatch → HARD FAIL, not a silent drop

PHASE 2 — global

· union all live sets → rebuild page refcounts, slab freelists,

per-page owner-queue sets, occupancy stats

· enable allocation

PHASE 3 — per queue

· rebuild ready heap + delayed wheel; receipts map starts empty

· rebuild the slot freelist (slots with msg_id == 0)

· write a fresh checkpoint, then serve

**Phase 1 step 5 is a deliberate change.** Rev 2 dropped generation-mismatched messages with a log line — silently deleting data a producer was told was durable. Under §8's reuse rule that state is unreachable, so encountering it means an invariant is already broken and the honest response is to stop, not to guess. A "cannot happen" branch must never be wired to data loss.

Recovery is a **pure fold of journal transitions over an empty state**; the index only lets the fold start later than LSN 0.

### 7.1 Checkpointing and WAL truncation

1\. snapshot the index (copy-on-write) at current LSN = L; no quiescing

2\. write index.dat.tmp with header.checkpoint_lsn = L

3\. fsync index.dat.tmp

```
4\. rename → index.dat ; fsync the directory ◀── CHECKPOINT DURABLE
```

5\. wal.TruncatePrefix(L): delete only fully-superseded segments

The rename is the atomic commit. A crash at any earlier step leaves the previous checkpoint and a longer replay — never an inconsistency.

### 7.2 Idle queues must still checkpoint

A queue that never checkpoints keeps its WAL segments forever. Checkpoints therefore fire on a size *or* time trigger even when a queue is idle, and a checkpoint with no change since the last one is a cheap header rewrite. (In rev 2 this also stalled global page reclamation; §8 removes that coupling entirely, but unbounded WAL growth alone justifies the rule.)

## 8. Payload slot lifecycle and reclamation

```
LIVE ─────────────────────▶ LOGICALLY DEAD ──────────────▶ REUSABLE
```

the killing record refcount reaches 0

(ACK / DEAD / MOVE_REF)

is durable in the WAL

**Correction to rev 2.** Rev 2 gated reuse on checkpoint_lsn ≥ LSN of the killing record. That is stricter than necessary and stalls reclamation for up to a full checkpoint interval: recovery replays the *entire* WAL forward from the checkpoint, so it always observes a durable ACK, and truncation only ever removes records at or below checkpoint_lsn. Therefore

> once wal.DurableLSN() ≥ LSN(killing record), no recoverable state can reference the old location.

which is exactly the property required — proven at fsync time rather than at checkpoint time. Generations remain as a *safety check* that turns any violation into a loud failure (§7 Phase 1 step 5) rather than a silent misread.

Freed-but-not-yet-durable slots sit on a pending list and migrate to the CAS freelist when the durable LSN passes them — typically within one group commit.

### 8.1 Size-classed slabs — the common case

Freeing pushes the slot onto the page's pending list; the next same-class record reuses it in place once durable. O(1), no copying. Out-of-order death — which priority, LIFO and delay all cause — stops mattering. Applies only to uncompressed pages (§3.5).

### 8.2 Hole punching — large records, no copying

fallocate(FALLOC_FL_PUNCH_HOLE) returns physical blocks for freed records ≥ 4 KiB while every other offset stays valid. One syscall; the file goes sparse. Gated on the same durable-LSN rule, and likewise only for uncompressed pages.

### 8.3 Lifetime bucketing — make pages die together

Pop order is predictable: delayed messages leave in available_at order, priorities drain top-down. So co-locate by expected death — delayed messages allocate from pages bucketed by timing-wheel slot, each priority band gets its own active page. Generational GC's insight applied to a queue whose future is genuinely known.

### 8.4 Compaction across a shared page store

This is the coordination rev 2 skipped. A page may hold payloads owned by several queues, while MOVE_REF must be appended to *each owning queue's* WAL.

1\. compactor takes the page's compaction lock (excludes alloc + other compactors)

2\. owners := union over queues of index.ScanPage(page_id)

narrowed by the per-page owner-queue set (volatile, from §7 Phase 2),

so only queues that actually hold payloads here are scanned

3\. copy survivor bytes into destination pages; fsync destinations

4\. per owning queue: append MOVE_REF; fsync that queue's WAL

5\. update each queue's in-memory index to the new ref

6\. old page is retired only when, for every owning queue,

wal.DurableLSN() ≥ LSN(its MOVE_REF)

7\. append PAGE_RETIRE{page_id, old_gen, new_gen} to pagestore/store.log; fsync

8\. bump generation, return the page to the free pool

Crash analysis: between 3 and 4 there are two durable copies and the WAL still points at the old one — harmless, the destination is simply garbage collected by the refcount rebuild. Between 4 and 7 replay applies MOVE_REF, so the new location wins and the old page is retired on the next compaction pass. A message acked concurrently makes its MOVE_REF a no-op at replay (§7 Phase 1). Owners are located by index scan, so no per-record back-pointer is needed anywhere.

Trigger: liveBytes/capacity < 25 %. Sections 8.1 and 8.3 keep it cold in practice, but the pathological case — one long-lived low-priority message pinning an otherwise-empty page — is real, so it is built regardless. page_occupancy is exported so the claim stays measurable rather than asserted.

### 8.5 Queue deletion is journaled

rm -rf plus a refcount sweep is not crash-safe on a shared page store: a crash mid-delete leaks pages or, worse, double-frees them.

1\. append QUEUE_DROP_BEGIN{queue_id}; fsync store.log

2\. scan the queue's index; release every page ref it holds

3\. delete queues/<name>/ ; fsync the parent directory

4\. append QUEUE_DROP_END{queue_id}; fsync store.log

A BEGIN without a matching END at Phase 0 resumes the sweep. Releases are idempotent because refcounts are rebuilt from live sets rather than decremented from durable state, so replaying a partial sweep cannot double-free.

## 9. Concurrency model

| **Concern** | **Mechanism** | **Cost** |
|:---|:---|:---|
| between queues | one mutex per queue; separate WAL per queue | zero cross-queue contention |
| msg ids, stats | atomic counters | lock-free |
| page refcount | atomic.Add (volatile) | lock-free |
| slab freelist | CAS push/pop | lock-free |
| ready heap + wheels + receipts | per-queue mutex | few hundred ns |
| WAL append | per-queue append lock + group commit | one fsync per batch |
| page compaction | per-page compaction lock | excludes alloc on that page only |
| consumer wakeup | channel handoff (long-poll) | no busy-wait, no lock held |

Lock ordering, to make deadlock a static property rather than a hope: broker.mu → queue.mu → page compaction lock → wal append lock. No path takes two queue mutexes; compaction touches multiple queues but takes each in msg_id order and never while holding another.

**Why the heaps are not lock-free, deliberately:** one dequeue must mutate the ready heap, the inflight wheel and the receipt map together — a multi-word CAS problem (hazard pointers or DCAS) for no measurable payoff when every durable write already waits on an fsync three orders of magnitude larger. Atomics are used where they are sound; a short per-queue critical section is used where correctness is the harder problem. A benchmark demonstrating this is part of the deliverable, not an assertion.

## 10. HTTP API

POST /queues {"name":"jobs","order":"lifo","max_attempts":5,

"aging_ms":0,"durable_leases":false}

POST /queues/jobs/messages {"body":"…","priority":200,"delay_ms":5000}

→ {"id":8412}

POST /queues/jobs/lease {"max":10,"visibility_ms":30000,"wait_ms":20000}

→ [{"receipt":"8412:1:9f3c…","id":8412,

"body":"…","attempts":1}]

```
POST /queues/jobs/ack {"receipt":"…"} → 204 │ 409 stale receipt
```

POST /queues/jobs/nack {"receipt":"…","requeue_delay_ms":1000}

POST /queues/jobs/replay {"from_id":…,"to_id":…,"filter":{…}}

GET /queues/jobs/stats → {"ready":12,"delayed":3,"inflight":2,

"acked":901,"dead":1,"page_occupancy":0.87,

"checkpoint_lsn":99213,"durable_lsn":99871,

"wal_bytes":4194304,"oldest_ready_age_ms":812}

lease long-polls via channel handoff, so idle consumers neither busy-poll nor hold the queue lock. delay_ms > max_delay_ms and len(body) > max_payload_bytes are 400 and 413 respectively.

## 11. Demo application

A small web UI over the same HTTP API: create queues in any combination of order × priority × delay, publish messages, watch them move delayed → ready → inflight → acked live, then kill the process mid-flight and watch the same state come back — including in-flight messages correctly returning to READY and their pre-restart receipts correctly rejected with 409. It demonstrates the durability and composability claims rather than describing them.

## 12. Answers to the four questions

**Replay.** The WAL is already an immutable transition log, so replay is first-class rather than bolted on: ACK tombstones a message, it does not erase history, so POST /queues/{q}/replay {from_id,to_id,filter} re-reads ENQUEUE records in that range and re-inserts them as new messages (new ids, replay_of provenance, original payload). The binding constraint is payload retention, not log retention: retention_ms holds acked pages past refcount zero, making replay availability a per-queue policy that trades disk for recoverability, and it is exactly the case where compress_sealed (§3.5) earns its keep. This is the log-versus-queue tension, and refcount + durable-LSN gating is what lets one engine serve both.

**Refactor to Pub/Sub.** The rev-2/3 split makes this nearly mechanical, because durable truth is already separate from consumption state. A subscription becomes its own volatile tier plus its own cursor over the *shared* WAL: payload and ENQUEUE are stored once; each subscription keeps an independent ready heap, delayed wheel, inflight wheel and attempt counters; per-message transitions gain a sub_id. Page refcount increments once per subscription instead of once per message, and the page frees when the last subscription's ACK is durable. Fan-out becomes a refcount change rather than a copy, and the write path does not change at all. The one genuinely new piece is subscription lifecycle — creating a subscription mid-stream needs a defined start cursor (tail, or an LSN), and deleting one needs the journaled sweep of §8.5.

**With more time.** Replication via Raft over the WAL (the WAL is already the replicated state machine — an extension, not a rewrite); dead-letter queues with replay-from-DLQ; idempotency keys with a dedup window to close the producer-side duplicate in §6.6; consumer groups with partition affinity; batch enqueue/ack; a binary protocol beside HTTP; io_uring for the commit path; Prometheus histograms for fsync latency, page occupancy and replay lag; and — most valuable — a crash-injection harness that kills the process at every durability point in §6 and asserts the table's stated outcome, plus a deterministic simulation test (FoundationDB-style) driving concurrent producers and consumers against a mocked clock and filesystem.

**Why choose this over SQS / RabbitMQ / Pulsar.** Not on scale — it loses to all three there, and pretending otherwise would be silly. It wins on three specific axes. (1) **Composability**: SQS makes you choose standard *or* FIFO and offers no priority at all; RabbitMQ implements priority as per-priority sub-queues that interact badly with the delayed-message plugin; here priority × order × delay are orthogonal by construction, so "delayed priority LIFO" is a configuration rather than an architecture. (2) **Operational footprint**: one static binary and a data directory — no cluster, no ZooKeeper, no BookKeeper — which makes it viable at the edge, embedded, in CI, and in local development where standing up Pulsar is absurd. (3) **Transparency**: a few thousand readable lines with an explicitly documented durability point for every operation (§6), so "what happens if we crash *here*" has a precise answer — harder with a managed service whose internals you cannot inspect and whose bill scales with request count. Honest positioning: choose this when you want SQS-like semantics *plus* priority and LIFO, with Redis-like operational simplicity and real disk durability; choose Pulsar when you need multi-tenant geo-replicated scale.

## Copyright & Legal Notice

**Copyright © 2026 Samanyu Goyal. All Rights Reserved.**

This design document, including all architectural descriptions, algorithms, diagrams, data structures, wire formats, and implementation specifications contained herein ("Document"), is the exclusive intellectual property of Samanyu Goyal.

**No part of this Document may be used, copied, reproduced, modified, disclosed, transmitted, or distributed in any form or by any means — electronic, mechanical, or otherwise — without the prior explicit written permission of Samanyu Goyal.**

Any unauthorized use, reproduction, or distribution of this Document constitutes a violation of copyright law and may result in severe civil and criminal penalties under applicable law. All rights reserved worldwide.

For licensing inquiries or written permission requests, contact the copyright holder directly.

> *This notice supersedes any other notice and applies to all versions, revisions, and derivatives of this document without exception.*
