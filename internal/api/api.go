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

// Package api exposes the broker over HTTP. It is a thin translation layer:
// all ordering, durability and lease logic lives in the queue package, and the
// only things decided here are wire encoding and status codes.
package api

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/samanyugoyal2010/frankenqueue/internal/queue"
	"github.com/samanyugoyal2010/frankenqueue/internal/types"
)

// Server routes HTTP requests to a broker.
type Server struct {
	b   *queue.Broker
	mux *http.ServeMux
}

// New builds the HTTP handler. If web is non-nil its contents are served at /.
func New(b *queue.Broker, web fs.FS) *Server {
	s := &Server{b: b, mux: http.NewServeMux()}
	s.mux.HandleFunc("/v1/queues", s.queues)
	s.mux.HandleFunc("/v1/queues/", s.queueOp)
	s.mux.HandleFunc("/v1/stats", s.storeStats)
	if web != nil {
		s.mux.Handle("/", http.FileServer(http.FS(web)))
	}
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type errBody struct {
	Error string `json:"error"`
}

// fail maps domain errors onto status codes.
func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNoQueue), errors.Is(err, queue.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errBody{err.Error()})
	case errors.Is(err, queue.ErrBadReceipt):
		writeJSON(w, http.StatusConflict, errBody{err.Error()})
	case errors.Is(err, queue.ErrDelayTooLong):
		writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
	case errors.Is(err, queue.ErrFull):
		writeJSON(w, http.StatusTooManyRequests, errBody{err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, errBody{err.Error()})
	}
}

func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(http.MaxBytesReader(nil, r.Body, 64<<20)).Decode(v)
}

type createReq struct {
	Name          string `json:"name"`
	Order         string `json:"order"`
	MaxAttempts   uint16 `json:"max_attempts"`
	VisibilityMs  int64  `json:"visibility_ms"`
	MaxDelayMs    int64  `json:"max_delay_ms"`
	AgeBoostMs    int64  `json:"age_boost_ms"`
	MaxDepth      int    `json:"max_depth"`
	DurableLeases bool   `json:"durable_leases"`
}

type queueInfo struct {
	Name   string       `json:"name"`
	Config queue.Config `json:"config"`
	Stats  queue.Stats  `json:"stats"`
}

func (s *Server) queues(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		out := []queueInfo{}
		for _, name := range s.b.Names() {
			q, err := s.b.Get(name)
			if err != nil {
				continue
			}
			out = append(out, queueInfo{Name: name, Config: q.Config(), Stats: q.Stats()})
		}
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var req createReq
		if err := decode(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
			return
		}
		order, err := types.ParseOrder(req.Order)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
			return
		}
		if strings.TrimSpace(req.Name) == "" || strings.ContainsAny(req.Name, "/\\.") {
			writeJSON(w, http.StatusBadRequest, errBody{"invalid queue name"})
			return
		}
		q, err := s.b.Create(queue.Config{
			Name: req.Name, Order: order, MaxAttempts: req.MaxAttempts,
			VisibilityMs: req.VisibilityMs, MaxDelayMs: req.MaxDelayMs,
			AgeBoostMs: req.AgeBoostMs, MaxDepth: req.MaxDepth,
			DurableLeases: req.DurableLeases,
		})
		if err != nil {
			writeJSON(w, http.StatusConflict, errBody{err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, queueInfo{Name: q.Config().Name, Config: q.Config(), Stats: q.Stats()})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) queueOp(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/queues/")
	parts := strings.SplitN(rest, "/", 2)
	name := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}
	if name == "" {
		writeJSON(w, http.StatusBadRequest, errBody{"missing queue name"})
		return
	}
	if action == "" && r.Method == http.MethodDelete {
		if err := s.b.Delete(name); err != nil {
			fail(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	q, err := s.b.Get(name)
	if err != nil {
		fail(w, err)
		return
	}
	switch action {
	case "":
		writeJSON(w, http.StatusOK, queueInfo{Name: name, Config: q.Config(), Stats: q.Stats()})
	case "messages":
		s.enqueue(w, r, q)
	case "lease":
		s.lease(w, r, q)
	case "ack":
		s.ack(w, r, q)
	case "nack":
		s.nack(w, r, q)
	case "dead":
		s.deadLetters(w, r, q)
	case "replay":
		s.replay(w, r, q)
	case "checkpoint":
		if err := q.Checkpoint(); err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, q.Stats())
	case "stats":
		writeJSON(w, http.StatusOK, q.Stats())
	default:
		writeJSON(w, http.StatusNotFound, errBody{"unknown action " + action})
	}
}

type enqueueItem struct {
	Body     string `json:"body"`
	Priority uint8  `json:"priority"`
	DelayMs  int64  `json:"delay_ms"`
}

type enqueueReq struct {
	enqueueItem
	Messages []enqueueItem `json:"messages"`
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request, q *queue.Queue) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req enqueueReq
	if err := decode(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
		return
	}
	items := req.Messages
	if len(items) == 0 {
		items = []enqueueItem{req.enqueueItem}
	}
	reqs := make([]queue.EnqueueRequest, len(items))
	for i, it := range items {
		reqs[i] = queue.EnqueueRequest{Payload: []byte(it.Body), Priority: it.Priority, DelayMs: it.DelayMs}
	}
	ids, err := q.Enqueue(reqs...)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ids": ids})
}

type deliveryOut struct {
	ID           uint64 `json:"id"`
	Body         string `json:"body"`
	Priority     uint8  `json:"priority"`
	Attempt      uint16 `json:"attempt"`
	Receipt      string `json:"receipt"`
	LeaseUntilMs int64  `json:"lease_until_ms"`
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request, q *queue.Queue) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Max          int   `json:"max"`
		VisibilityMs int64 `json:"visibility_ms"`
		WaitMs       int64 `json:"wait_ms"`
	}
	if r.ContentLength > 0 {
		if err := decode(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
			return
		}
	}
	if v := r.URL.Query().Get("max"); v != "" {
		req.Max, _ = strconv.Atoi(v)
	}
	if v := r.URL.Query().Get("wait_ms"); v != "" {
		req.WaitMs, _ = strconv.ParseInt(v, 10, 64)
	}
	if req.WaitMs > 30_000 {
		req.WaitMs = 30_000
	}
	var ds []queue.Delivery
	var err error
	if req.WaitMs > 0 {
		ds, err = q.LeaseWait(req.Max, req.VisibilityMs, time.Duration(req.WaitMs)*time.Millisecond)
	} else {
		ds, err = q.Lease(req.Max, req.VisibilityMs)
	}
	if err != nil {
		fail(w, err)
		return
	}
	out := make([]deliveryOut, 0, len(ds))
	for _, d := range ds {
		out = append(out, deliveryOut{
			ID: d.ID, Body: string(d.Payload), Priority: d.Priority,
			Attempt: d.Attempt, Receipt: d.Receipt, LeaseUntilMs: d.LeaseUntilMs,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request, q *queue.Queue) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Receipt string `json:"receipt"`
	}
	if err := decode(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
		return
	}
	if err := q.Ack(req.Receipt); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"acked": true})
}

func (s *Server) nack(w http.ResponseWriter, r *http.Request, q *queue.Queue) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Receipt string `json:"receipt"`
		DelayMs int64  `json:"delay_ms"`
	}
	if err := decode(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
		return
	}
	if err := q.Nack(req.Receipt, req.DelayMs); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nacked": true})
}

type deadOut struct {
	ID           uint64 `json:"id"`
	Body         string `json:"body"`
	Priority     uint8  `json:"priority"`
	Attempts     uint16 `json:"attempts"`
	EnqueuedAtMs int64  `json:"enqueued_at_ms"`
}

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request, q *queue.Queue) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ms, err := q.DeadLetters(limit)
	if err != nil {
		fail(w, err)
		return
	}
	out := make([]deadOut, 0, len(ms))
	for _, m := range ms {
		out = append(out, deadOut{ID: m.ID, Body: string(m.Payload), Priority: m.Priority, Attempts: m.Attempts, EnqueuedAtMs: m.EnqueuedAtMs})
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request, q *queue.Queue) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		IDs     []uint64 `json:"ids"`
		DelayMs int64    `json:"delay_ms"`
	}
	if r.ContentLength > 0 {
		if err := decode(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, errBody{err.Error()})
			return
		}
	}
	ids, err := q.Replay(req.IDs, req.DelayMs)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ids": ids})
}

func (s *Server) storeStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.b.Store().Stats())
}
