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
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/samanyugoyal2010/frankenqueue/internal/index"
	"github.com/samanyugoyal2010/frankenqueue/internal/pagestore"
)

// Broker manages a collection of named queues backed by a shared PageStore.
type Broker struct {
	dir   string
	pages *pagestore.Store

	mu     sync.RWMutex
	queues map[string]*Queue
}

// OpenBroker opens or creates a broker at dir.  It recovers all queues found
// in dir/queues/ and runs the four recovery phases from the design doc.
func OpenBroker(dir string) (*Broker, error) {
	if err := os.MkdirAll(filepath.Join(dir, "queues"), 0o755); err != nil {
		return nil, err
	}

	// Phase 0: open the shared PageStore (replays store.log).
	pages, err := pagestore.Open(dir)
	if err != nil {
		return nil, err
	}

	b := &Broker{
		dir:    dir,
		pages:  pages,
		queues: make(map[string]*Queue),
	}

	// Phase 1 + 3: recover each queue found in dir/queues/.
	entries, err := os.ReadDir(filepath.Join(dir, "queues"))
	if err != nil && !os.IsNotExist(err) {
		pages.Close()
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		qDir := filepath.Join(dir, "queues", e.Name())
		cfg, err := loadMeta(qDir)
		if err != nil {
			continue // skip unreadable queues (operator must intervene)
		}
		q, err := openQueue(qDir, cfg, pages)
		if err != nil {
			pages.Close()
			return nil, err
		}
		b.queues[cfg.Name] = q
	}
	// Phase 2: rebuild PageStore volatile state from live refs.
	if err := b.rebuildPageStore(); err != nil {
		pages.Close()
		return nil, err
	}

	return b, nil
}

// rebuildPageStore collects live Refs from all queues and calls RebuildVolatile.
func (b *Broker) rebuildPageStore() error {
	return b.pages.RebuildVolatile(b.collectRefs())
}

// collectRefs gathers all live SlotRefs from every queue's index.
// Both the index slot itself (backed by a 64-byte slab in the page store)
// and the payload slot it references must be marked live so that
// RebuildVolatile does not reclaim either as an orphan.
func (b *Broker) collectRefs() []pagestore.SlotRef {
	var refs []pagestore.SlotRef
	for _, q := range b.queues {
		q.idx.Scan(func(indexRef uint64, s index.Slot) {
			if s.MsgID != 0 {
				refs = append(refs, pagestore.SlotRef(indexRef))      // index slot
				refs = append(refs, pagestore.SlotRef(s.PayloadRef))   // payload slot
			}
		})
	}
	return refs
}

// Store returns the shared PageStore (used by the /v1/stats endpoint).
func (b *Broker) Store() *pagestore.Store { return b.pages }

// Names returns all queue names in sorted order.
func (b *Broker) Names() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.queues))
	for n := range b.queues {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Get returns the named queue.
func (b *Broker) Get(name string) (*Queue, error) {
	b.mu.RLock()
	q, ok := b.queues[name]
	b.mu.RUnlock()
	if !ok {
		return nil, ErrNoQueue
	}
	return q, nil
}

// Create creates a new queue with cfg.  Returns ErrExists if the name is taken.
func (b *Broker) Create(cfg Config) (*Queue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.queues[cfg.Name]; exists {
		return nil, ErrExists
	}

	qDir := filepath.Join(b.dir, "queues", cfg.Name)
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		return nil, err
	}
	if err := saveMeta(qDir, cfg); err != nil {
		return nil, err
	}
	q, err := openQueue(qDir, cfg, b.pages)
	if err != nil {
		return nil, err
	}
	b.queues[cfg.Name] = q
	return q, nil
}

// Delete removes a queue and its data.
func (b *Broker) Delete(name string) error {
	b.mu.Lock()
	q, ok := b.queues[name]
	if !ok {
		b.mu.Unlock()
		return ErrNoQueue
	}
	delete(b.queues, name)
	b.mu.Unlock()

	q.Close()
	return os.RemoveAll(filepath.Join(b.dir, "queues", name))
}

// Close shuts down all queues and the page store.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, q := range b.queues {
		q.Close()
	}
	b.queues = make(map[string]*Queue)
	return b.pages.Close()
}
