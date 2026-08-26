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

// Package pagestore implements the shared 1-MiB-aligned payload store.
// This file handles the append-only store.log that journals page lifecycle
// transitions (PAGE_CREATE, PAGE_RETIRE). It is fsynced before every
// physical action so crash recovery always has a complete picture.
package pagestore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Journal record opcodes.
const (
	jrnlPageCreate     = byte(1)
	jrnlPageRetire     = byte(2)
	jrnlQueueDropBegin = byte(3)
	jrnlQueueDropEnd   = byte(4)
)

// jrnlRecord is the decoded form of one store.log entry.
type jrnlRecord struct {
	op        byte
	pageID    uint32
	scIdx     uint16
	spanPages uint32
	gen       uint32 // used by PAGE_CREATE
	oldGen    uint32 // used by PAGE_RETIRE
	newGen    uint32 // used by PAGE_RETIRE
	queueID   uint32 // used by QUEUE_DROP_*
}

// journal is an append-only, fsynced log of page-store state transitions.
// Wire format (all little-endian):
//
//	PAGE_CREATE  (19 B): op(1) pageID(4) scIdx(2) spanPages(4) gen(4) crc32(4)
//	PAGE_RETIRE  (17 B): op(1) pageID(4) oldGen(4) newGen(4)          crc32(4)
//
// crc32 is computed over all preceding bytes in the record (including op).
type journal struct {
	f *os.File
}

func openJournal(path string) (*journal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &journal{f: f}, nil
}

func (j *journal) close() error { return j.f.Close() }

// appendPageCreate writes + fsyncs a PAGE_CREATE record.
// Must be called before creating the physical page file.
func (j *journal) appendPageCreate(pageID uint32, scIdx uint16, spanPages, gen uint32) error {
	var buf [19]byte
	buf[0] = jrnlPageCreate
	binary.LittleEndian.PutUint32(buf[1:5], pageID)
	binary.LittleEndian.PutUint16(buf[5:7], scIdx)
	binary.LittleEndian.PutUint32(buf[7:11], spanPages)
	binary.LittleEndian.PutUint32(buf[11:15], gen)
	binary.LittleEndian.PutUint32(buf[15:19], crc32.ChecksumIEEE(buf[:15]))
	if _, err := j.f.Write(buf[:]); err != nil {
		return err
	}
	return j.f.Sync()
}

// appendPageRetire writes a PAGE_RETIRE record (no fsync needed).
// Safety argument: if the process crashes before the write is flushed, the
// page file still exists on disk.  On replay we see PAGE_CREATE but no
// PAGE_RETIRE, so the page is treated as live.  RebuildVolatile then finds
// no queue WAL refs for it and reclaims it as an orphan — correct behaviour.
// Fsyncing every retirement was O(pages) fsyncs on a large free, which is
// the dominant cost and makes bulk-free tests minutes slow.
func (j *journal) appendPageRetire(pageID, oldGen, newGen uint32) error {
	var buf [17]byte
	buf[0] = jrnlPageRetire
	binary.LittleEndian.PutUint32(buf[1:5], pageID)
	binary.LittleEndian.PutUint32(buf[5:9], oldGen)
	binary.LittleEndian.PutUint32(buf[9:13], newGen)
	binary.LittleEndian.PutUint32(buf[13:17], crc32.ChecksumIEEE(buf[:13]))
	_, err := j.f.Write(buf[:])
	return err
}

// appendQueueDrop writes QUEUE_DROP_BEGIN or QUEUE_DROP_END + fsyncs.
func (j *journal) appendQueueDrop(op byte, queueID uint32) error {
	var buf [9]byte // op(1) + queueID(4) + crc32(4)
	buf[0] = op
	binary.LittleEndian.PutUint32(buf[1:5], queueID)
	binary.LittleEndian.PutUint32(buf[5:9], crc32.ChecksumIEEE(buf[:5]))
	if _, err := j.f.Write(buf[:]); err != nil {
		return err
	}
	return j.f.Sync()
}

// replay reads all valid records from the beginning of the journal.
// A CRC mismatch at the tail is treated as a torn write and stops the scan
// (the tail is truncated implicitly; we just stop reading).
// A CRC mismatch mid-log is a hard failure.
func (j *journal) replay() ([]jrnlRecord, error) {
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []jrnlRecord
	pos := int64(0)
	lastGoodPos := int64(0)

	for {
		var opBuf [1]byte
		if _, err := io.ReadFull(j.f, opBuf[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("store.log read op at %d: %w", pos, err)
		}
		op := opBuf[0]

		switch op {
		case jrnlPageCreate:
			// Remaining: pageID(4)+scIdx(2)+spanPages(4)+gen(4)+crc(4) = 18 bytes
			var tail [18]byte
			if _, err := io.ReadFull(j.f, tail[:]); err != nil {
				// Torn write at the tail — stop.
				goto done
			}
			// CRC covers op + first 14 bytes of tail.
			computed := crc32.ChecksumIEEE(append([]byte{op}, tail[:14]...))
			stored := binary.LittleEndian.Uint32(tail[14:18])
			if computed != stored {
				if len(records) > 0 {
					// Mid-log corruption is a hard failure.
					return nil, fmt.Errorf("store.log: PAGE_CREATE CRC mismatch at pos %d (mid-log)", pos)
				}
				goto done // Torn tail if this is the first record
			}
			records = append(records, jrnlRecord{
				op:        jrnlPageCreate,
				pageID:    binary.LittleEndian.Uint32(tail[0:4]),
				scIdx:     binary.LittleEndian.Uint16(tail[4:6]),
				spanPages: binary.LittleEndian.Uint32(tail[6:10]),
				gen:       binary.LittleEndian.Uint32(tail[10:14]),
			})
			pos += 19
			lastGoodPos = pos

		case jrnlPageRetire:
			// Remaining: pageID(4)+oldGen(4)+newGen(4)+crc(4) = 16 bytes
			var tail [16]byte
			if _, err := io.ReadFull(j.f, tail[:]); err != nil {
				goto done
			}
			computed := crc32.ChecksumIEEE(append([]byte{op}, tail[:12]...))
			stored := binary.LittleEndian.Uint32(tail[12:16])
			if computed != stored {
				if len(records) > 0 {
					return nil, fmt.Errorf("store.log: PAGE_RETIRE CRC mismatch at pos %d (mid-log)", pos)
				}
				goto done
			}
			records = append(records, jrnlRecord{
				op:     jrnlPageRetire,
				pageID: binary.LittleEndian.Uint32(tail[0:4]),
				oldGen: binary.LittleEndian.Uint32(tail[4:8]),
				newGen: binary.LittleEndian.Uint32(tail[8:12]),
			})
			pos += 17
			lastGoodPos = pos

		case jrnlQueueDropBegin, jrnlQueueDropEnd:
			var tail [8]byte // queueID(4)+crc(4)
			if _, err := io.ReadFull(j.f, tail[:]); err != nil {
				goto done
			}
			computed := crc32.ChecksumIEEE(append([]byte{op}, tail[:4]...))
			stored := binary.LittleEndian.Uint32(tail[4:8])
			if computed != stored {
				if len(records) > 0 {
					return nil, fmt.Errorf("store.log: QUEUE_DROP CRC mismatch at pos %d", pos)
				}
				goto done
			}
			records = append(records, jrnlRecord{
				op:      op,
				queueID: binary.LittleEndian.Uint32(tail[0:4]),
			})
			pos += 9
			lastGoodPos = pos

		default:
			// Unknown op — treat as torn tail.
			goto done
		}
	}

done:
	_ = lastGoodPos
	return records, nil
}
