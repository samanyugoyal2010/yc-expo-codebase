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

// Package wal implements the per-queue write-ahead log.
// Every message state transition is an append to this log. It is the sole
// durable truth for message state; the index is a rebuildable cache on top.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Op identifies the kind of state transition stored in a record.
type Op uint8

const (
	OpEnqueue Op = 1 // message arrives (ready or delayed)
	OpLease   Op = 2 // message leased to a consumer
	OpAck     Op = 3 // consumer confirmed delivery
	OpNack    Op = 4 // consumer returned the message for redelivery
	OpExpire  Op = 5 // lease timed out; message re-enters ready
	OpDead    Op = 6 // max attempts exceeded; message dead-lettered
	OpMoveRef Op = 7 // compaction relocated the payload
	OpConfig  Op = 8 // queue configuration changed
)

// Wire format of a WAL record (all little-endian):
//
//	[0:4]   length   u32   total record bytes (including this field and CRC)
//	[4:12]  lsn      u64   monotonic per queue
//	[12]    op       u8
//	[13]    flags    u8
//	[14:16] reserved u16
//	[16 …]  payload  —     op-specific (sizes below)
//	[…:+4]  crc32    u32   over all preceding bytes in this record
//
// Payload sizes:
//
//	ENQUEUE  45 B  msg_id(8) index_ref(8) payload_ref(8) payload_len(4)
//	                priority(1) enqueued_at_ms(8) available_at_ms(8)
//	LEASE    26 B  msg_id(8) nonce(8) attempt(2) lease_until_ms(8)
//	ACK      16 B  msg_id(8) nonce(8)
//	NACK     24 B  msg_id(8) nonce(8) requeue_at_ms(8)
//	EXPIRE   16 B  msg_id(8) nonce(8)
//	DEAD      9 B  msg_id(8) reason(1)
//	MOVE_REF 24 B  msg_id(8) old_ref(8) new_ref(8)

const recordHeaderSize = 16 // length(4)+lsn(8)+op(1)+flags(1)+reserved(2)
const recordCRCSize    = 4

// Enqueue is the payload for OpEnqueue.
// IndexRef and PayloadRef are pagestore.SlotRef values encoded as uint64
// (pageID<<20 | offset).  PayloadLen is the exact byte count written into
// the payload slot (≤ slot size class).
type Enqueue struct {
	MsgID         uint64
	IndexRef      uint64 // pagestore.SlotRef: index slot address
	PayloadRef    uint64 // pagestore.SlotRef: payload slot address
	PayloadLen    uint32 // exact bytes of caller payload
	Priority      uint8
	EnqueuedAtMs  int64
	AvailableAtMs int64
}

// Lease is the payload for OpLease.
type Lease struct {
	MsgID        uint64
	Nonce        uint64
	Attempt      uint16
	LeaseUntilMs int64
}

// Ack is the payload for OpAck.
type Ack struct {
	MsgID uint64
	Nonce uint64
}

// Nack is the payload for OpNack.
type Nack struct {
	MsgID       uint64
	Nonce       uint64
	RequeueAtMs int64
}

// Expire is the payload for OpExpire.
type Expire struct {
	MsgID uint64
	Nonce uint64
}

// Dead is the payload for OpDead.
type Dead struct {
	MsgID  uint64
	Reason uint8
}

// MoveRef is the payload for OpMoveRef (compaction).
// OldRef and NewRef are pagestore.SlotRef values encoded as uint64.
type MoveRef struct {
	MsgID  uint64
	OldRef uint64 // old payload SlotRef
	NewRef uint64 // new payload SlotRef
}

// Record is a decoded WAL entry.
type Record struct {
	LSN   uint64
	Op    Op
	Flags uint8
	Body  interface{} // one of the typed payloads above
}

// Encode serialises rec (with the given lsn) into a self-describing byte slice
// ready to be appended to the log file.
func Encode(lsn uint64, rec Record) ([]byte, error) {
	payload, err := encodePayload(rec)
	if err != nil {
		return nil, err
	}

	total := recordHeaderSize + len(payload) + recordCRCSize
	buf := make([]byte, total)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(total))
	binary.LittleEndian.PutUint64(buf[4:12], lsn)
	buf[12] = uint8(rec.Op)
	buf[13] = rec.Flags
	// [14:16] reserved = 0
	copy(buf[16:], payload)

	crc := crc32.ChecksumIEEE(buf[:recordHeaderSize+len(payload)])
	binary.LittleEndian.PutUint32(buf[recordHeaderSize+len(payload):], crc)
	return buf, nil
}

// Decode parses one complete record from buf (which must be exactly one record).
func Decode(buf []byte) (Record, error) {
	if len(buf) < recordHeaderSize+recordCRCSize {
		return Record{}, fmt.Errorf("wal: record too short (%d bytes)", len(buf))
	}
	total := int(binary.LittleEndian.Uint32(buf[0:4]))
	if total != len(buf) {
		return Record{}, fmt.Errorf("wal: length field %d != buf len %d", total, len(buf))
	}
	payloadEnd := total - recordCRCSize
	computed := crc32.ChecksumIEEE(buf[:payloadEnd])
	stored := binary.LittleEndian.Uint32(buf[payloadEnd:])
	if computed != stored {
		return Record{}, fmt.Errorf("wal: CRC mismatch (computed %08x stored %08x)", computed, stored)
	}

	rec := Record{
		LSN:   binary.LittleEndian.Uint64(buf[4:12]),
		Op:    Op(buf[12]),
		Flags: buf[13],
	}
	payload := buf[recordHeaderSize:payloadEnd]
	body, err := decodePayload(rec.Op, payload)
	if err != nil {
		return Record{}, err
	}
	rec.Body = body
	return rec, nil
}

func encodePayload(rec Record) ([]byte, error) {
	switch rec.Op {
	case OpEnqueue:
		e, ok := rec.Body.(Enqueue)
		if !ok {
			return nil, fmt.Errorf("wal: ENQUEUE body type %T", rec.Body)
		}
		b := make([]byte, 45)
		binary.LittleEndian.PutUint64(b[0:8], e.MsgID)
		binary.LittleEndian.PutUint64(b[8:16], e.IndexRef)
		binary.LittleEndian.PutUint64(b[16:24], e.PayloadRef)
		binary.LittleEndian.PutUint32(b[24:28], e.PayloadLen)
		b[28] = e.Priority
		binary.LittleEndian.PutUint64(b[29:37], uint64(e.EnqueuedAtMs))
		binary.LittleEndian.PutUint64(b[37:45], uint64(e.AvailableAtMs))
		return b, nil

	case OpLease:
		l, ok := rec.Body.(Lease)
		if !ok {
			return nil, fmt.Errorf("wal: LEASE body type %T", rec.Body)
		}
		b := make([]byte, 26)
		binary.LittleEndian.PutUint64(b[0:8], l.MsgID)
		binary.LittleEndian.PutUint64(b[8:16], l.Nonce)
		binary.LittleEndian.PutUint16(b[16:18], l.Attempt)
		binary.LittleEndian.PutUint64(b[18:26], uint64(l.LeaseUntilMs))
		return b, nil

	case OpAck:
		a, ok := rec.Body.(Ack)
		if !ok {
			return nil, fmt.Errorf("wal: ACK body type %T", rec.Body)
		}
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b[0:8], a.MsgID)
		binary.LittleEndian.PutUint64(b[8:16], a.Nonce)
		return b, nil

	case OpNack:
		n, ok := rec.Body.(Nack)
		if !ok {
			return nil, fmt.Errorf("wal: NACK body type %T", rec.Body)
		}
		b := make([]byte, 24)
		binary.LittleEndian.PutUint64(b[0:8], n.MsgID)
		binary.LittleEndian.PutUint64(b[8:16], n.Nonce)
		binary.LittleEndian.PutUint64(b[16:24], uint64(n.RequeueAtMs))
		return b, nil

	case OpExpire:
		e, ok := rec.Body.(Expire)
		if !ok {
			return nil, fmt.Errorf("wal: EXPIRE body type %T", rec.Body)
		}
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b[0:8], e.MsgID)
		binary.LittleEndian.PutUint64(b[8:16], e.Nonce)
		return b, nil

	case OpDead:
		d, ok := rec.Body.(Dead)
		if !ok {
			return nil, fmt.Errorf("wal: DEAD body type %T", rec.Body)
		}
		b := make([]byte, 9)
		binary.LittleEndian.PutUint64(b[0:8], d.MsgID)
		b[8] = d.Reason
		return b, nil

	case OpMoveRef:
		m, ok := rec.Body.(MoveRef)
		if !ok {
			return nil, fmt.Errorf("wal: MOVE_REF body type %T", rec.Body)
		}
		b := make([]byte, 24)
		binary.LittleEndian.PutUint64(b[0:8], m.MsgID)
		binary.LittleEndian.PutUint64(b[8:16], m.OldRef)
		binary.LittleEndian.PutUint64(b[16:24], m.NewRef)
		return b, nil

	default:
		return nil, fmt.Errorf("wal: unknown op %d", rec.Op)
	}
}

func decodePayload(op Op, b []byte) (interface{}, error) {
	switch op {
	case OpEnqueue:
		if len(b) < 45 {
			return nil, fmt.Errorf("wal: ENQUEUE payload %d < 45", len(b))
		}
		return Enqueue{
			MsgID:         binary.LittleEndian.Uint64(b[0:8]),
			IndexRef:      binary.LittleEndian.Uint64(b[8:16]),
			PayloadRef:    binary.LittleEndian.Uint64(b[16:24]),
			PayloadLen:    binary.LittleEndian.Uint32(b[24:28]),
			Priority:      b[28],
			EnqueuedAtMs:  int64(binary.LittleEndian.Uint64(b[29:37])),
			AvailableAtMs: int64(binary.LittleEndian.Uint64(b[37:45])),
		}, nil

	case OpLease:
		if len(b) < 26 {
			return nil, fmt.Errorf("wal: LEASE payload %d < 26", len(b))
		}
		return Lease{
			MsgID:        binary.LittleEndian.Uint64(b[0:8]),
			Nonce:        binary.LittleEndian.Uint64(b[8:16]),
			Attempt:      binary.LittleEndian.Uint16(b[16:18]),
			LeaseUntilMs: int64(binary.LittleEndian.Uint64(b[18:26])),
		}, nil

	case OpAck:
		if len(b) < 16 {
			return nil, fmt.Errorf("wal: ACK payload %d < 16", len(b))
		}
		return Ack{
			MsgID: binary.LittleEndian.Uint64(b[0:8]),
			Nonce: binary.LittleEndian.Uint64(b[8:16]),
		}, nil

	case OpNack:
		if len(b) < 24 {
			return nil, fmt.Errorf("wal: NACK payload %d < 24", len(b))
		}
		return Nack{
			MsgID:       binary.LittleEndian.Uint64(b[0:8]),
			Nonce:       binary.LittleEndian.Uint64(b[8:16]),
			RequeueAtMs: int64(binary.LittleEndian.Uint64(b[16:24])),
		}, nil

	case OpExpire:
		if len(b) < 16 {
			return nil, fmt.Errorf("wal: EXPIRE payload %d < 16", len(b))
		}
		return Expire{
			MsgID: binary.LittleEndian.Uint64(b[0:8]),
			Nonce: binary.LittleEndian.Uint64(b[8:16]),
		}, nil

	case OpDead:
		if len(b) < 9 {
			return nil, fmt.Errorf("wal: DEAD payload %d < 9", len(b))
		}
		return Dead{
			MsgID:  binary.LittleEndian.Uint64(b[0:8]),
			Reason: b[8],
		}, nil

	case OpMoveRef:
		if len(b) < 24 {
			return nil, fmt.Errorf("wal: MOVE_REF payload %d < 24", len(b))
		}
		return MoveRef{
			MsgID:  binary.LittleEndian.Uint64(b[0:8]),
			OldRef: binary.LittleEndian.Uint64(b[8:16]),
			NewRef: binary.LittleEndian.Uint64(b[16:24]),
		}, nil

	default:
		return nil, fmt.Errorf("wal: unknown op %d", op)
	}
}
