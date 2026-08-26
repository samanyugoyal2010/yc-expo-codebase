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

import "errors"

var (
	ErrNoQueue     = errors.New("queue not found")
	ErrNotFound    = errors.New("message not found")
	ErrBadReceipt  = errors.New("stale or invalid receipt")
	ErrDelayTooLong = errors.New("delay exceeds queue maximum")
	ErrFull        = errors.New("queue is full")
	ErrExists      = errors.New("queue already exists")
)
