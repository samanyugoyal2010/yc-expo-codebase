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

import "github.com/samanyugoyal2010/frankenqueue/internal/types"

const (
	defaultVisibilityMs = 30_000          // 30 s
	defaultMaxDelayMs   = 30 * 24 * 3600 * 1000 // 30 days
)

// Config is the durable configuration for a queue. It is written to
// queue.meta at creation and rewritten atomically on reconfiguration.
type Config struct {
	Name          string      `json:"name"`
	Order         types.Order `json:"order"`
	MaxAttempts   uint16      `json:"max_attempts"`   // 0 = unlimited
	VisibilityMs  int64       `json:"visibility_ms"`  // 0 → default 30 s
	MaxDelayMs    int64       `json:"max_delay_ms"`   // 0 → default 30 d
	AgeBoostMs    int64       `json:"age_boost_ms"`   // 0 = disabled
	MaxDepth      int         `json:"max_depth"`      // 0 = unlimited
	DurableLeases bool        `json:"durable_leases"`
}

func (c *Config) visibilityMs() int64 {
	if c.VisibilityMs > 0 {
		return c.VisibilityMs
	}
	return defaultVisibilityMs
}

func (c *Config) maxDelayMs() int64 {
	if c.MaxDelayMs > 0 {
		return c.MaxDelayMs
	}
	return defaultMaxDelayMs
}

// EnqueueRequest is the caller-supplied description of one message to enqueue.
type EnqueueRequest struct {
	Payload  []byte
	Priority uint8
	DelayMs  int64
}

// Delivery is returned by Lease and carries everything a consumer needs.
type Delivery struct {
	ID           uint64
	Payload      []byte
	Priority     uint8
	Attempt      uint16
	Receipt      string
	LeaseUntilMs int64
}

// DeadMessage is a message that has exceeded its max_attempts.
type DeadMessage struct {
	ID           uint64
	Payload      []byte
	Priority     uint8
	Attempts     uint16
	EnqueuedAtMs int64
}

// Stats is a point-in-time snapshot of queue utilisation.
type Stats struct {
	// Current depths.
	Ready    int `json:"ready"`
	Delayed  int `json:"delayed"`
	Inflight int `json:"inflight"`
	Dead     int `json:"dead"`

	// Lifetime totals.
	Enqueued  uint64 `json:"enqueued_total"`
	Delivered uint64 `json:"delivered_total"`
	Acked     uint64 `json:"acked_total"`
	Nacked    uint64 `json:"nacked_total"`
	Expired   uint64 `json:"expired_total"`
	Replayed  uint64 `json:"replayed_total"`
}
