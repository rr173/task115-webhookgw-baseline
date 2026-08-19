// Command webhookgw serves the webhook delivery gateway HTTP API backed by
// SQLite, and provides a --smoke-test that exercises the full delivery contract
// (enqueue, dispatch, HMAC signing, dead-letter, restart recovery) without any
// external service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webhookgw/internal/api"
	"webhookgw/internal/attempt"
	"webhookgw/internal/clock"
	"webhookgw/internal/delivery"
	"webhookgw/internal/metrics"
	"webhookgw/internal/store"
	"webhookgw/internal/subscription"
)

var osExit = os.Exit

func main() {
	smoke := flag.Bool("smoke-test", false, "run self-check and exit")
	dbPath := flag.String("db", "webhookgw.db", "SQLite database file path")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	if *smoke {
		if err := runSmokeTest(); err != nil {
			fmt.Println("smoke-test: FAIL:", err)
			osExit(1)
		}
		fmt.Println("smoke-test: ok")
		osExit(0)
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	clk := clock.RealClock{}
	subs := subscription.New(st, clk)
	mtr := metrics.New()
	d := delivery.New(st, subs, mtr, clk, nil)
	att := attempt.New(st)

	if n, err := d.Recover(); err != nil {
		log.Fatalf("recover: %v", err)
	} else if n > 0 {
		log.Printf("recover: reset %d in-flight attempts", n)
	}

	mux := api.NewMux(subs, d, att, mtr)
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("webhook gateway %s listening on %s (db=%s)", api.Version, *addr, *dbPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
