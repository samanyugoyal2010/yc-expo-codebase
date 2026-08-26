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

// Package types holds shared vocabulary types used by both the queue and api
// packages, keeping them free of import cycles.
package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Order controls the ordering of messages within the same priority level.
type Order uint8

const (
	FIFO Order = iota // arrival order (lower msg_id first)
	LIFO              // reverse arrival order (higher msg_id first)
)

// ParseOrder converts a JSON/HTTP string to an Order value.
func ParseOrder(s string) (Order, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "fifo", "":
		return FIFO, nil
	case "lifo":
		return LIFO, nil
	default:
		return 0, fmt.Errorf("unknown order %q (want \"fifo\" or \"lifo\")", s)
	}
}

func (o Order) String() string {
	if o == LIFO {
		return "lifo"
	}
	return "fifo"
}

func (o Order) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.String())
}

func (o *Order) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := ParseOrder(s)
	if err != nil {
		return err
	}
	*o = v
	return nil
}
