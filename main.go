// Command ssefy serves server-sent-event streams backed by PostgreSQL.
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
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log) // the store logs through slog.Default

	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	envFile := flag.String("env-file", "", "path to .env (default ./.env)")
	flag.Parse()

	cfg, err := config.Load(*envFile)
	if err != nil {
		return err
	}
	if cfg.Dev {
		log.Warn("development mode: SSEFY_SERVER_SECRET defaults to \"dev\"")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := openStore(ctx, cfg.Store)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	a := api.New(st, cfg.Secret, cfg.CORS, log)
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           a.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
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
			return err
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			return err
		}
	}
	return nil
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
