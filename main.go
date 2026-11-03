package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alash3al/ssefy/internal/api"
	"github.com/alash3al/ssefy/internal/config"
	"github.com/alash3al/ssefy/internal/store"
)

func main() {
	envFile := flag.String("env-file", "", "path to .env (default ./.env)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log) // the store logs through slog.Default

	cfg, err := config.Load(*envFile)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	if cfg.Dev {
		log.Warn("development mode: SSEFY_SERVER_SECRET defaults to \"dev\"")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := openStore(ctx, cfg.Store)
	if err != nil {
		log.Error("store", "err", err)
		os.Exit(1)
	}
	defer func() { _ = st.Close() }()

	a := api.New(st, cfg.Secret, cfg.CORS, log)
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           a.Handler(),
		Protocols:         protocols(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	srv.RegisterOnShutdown(a.Close) // end open SSE streams so Shutdown can finish

	errs := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.Listen, "store", storeKind(cfg.Store))
		errs <- srv.ListenAndServe()
	}()

	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			log.Error("shutdown", "err", err)
		}
	}
}

// protocols serves HTTP/1.1 and cleartext HTTP/2 (h2c) on the same port.
// Browsers only use HTTP/2 over TLS, so they talk HTTP/2 to the proxy; h2c is
// for Go clients and proxies that speak it upstream.
func protocols() *http.Protocols {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	return p
}

func storeKind(dsn string) string {
	if strings.HasPrefix(dsn, "memory://") {
		return "memory"
	}
	return "postgres"
}

func openStore(ctx context.Context, dsn string) (store.Store, error) {
	if strings.HasPrefix(dsn, "memory://") {
		return store.NewMemory(), nil
	}
	return store.OpenPostgres(ctx, dsn)
}
