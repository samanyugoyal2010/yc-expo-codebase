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

// Command frankenqueue runs the queue server: durable storage, the ordering
// engine, the HTTP API and the demo UI in a single process with no external
// dependencies.
package main
 
import (
	"context"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
 
	"github.com/samanyugoyal2010/frankenqueue/internal/api"
	"github.com/samanyugoyal2010/frankenqueue/internal/queue"
	"github.com/samanyugoyal2010/frankenqueue/web"
)
 
func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data", "./data", "data directory")
	flag.Parse()
 
	b, err := queue.OpenBroker(*dataDir)
	if err != nil {
		log.Fatalf("open broker: %v", err)
	}
 
	static, err := fs.Sub(web.Assets, "static")
	if err != nil {
		log.Fatalf("web assets: %v", err)
	}
 
	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(b, static),
		ReadHeaderTimeout: 10 * time.Second,
		// No write timeout: long-poll leases hold the response open on purpose.
	}
 
	go func() {
		log.Printf("frankenqueue listening on %s (data=%s)", *addr, *dataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()
 
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
 
	log.Print("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if err := b.Close(); err != nil {
		log.Fatalf("close broker: %v", err)
	}
}
