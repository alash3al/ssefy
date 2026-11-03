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
	"github.com/alash3al/ssefy/internal/store"
)

func main() {
	listen := flag.String("listen", env("SSEFY_LISTEN", ":8080"), "listen address")
	dsn := flag.String("store", env("SSEFY_STORE", "memory://"), "store: postgres://... or memory://")
	secret := flag.String("secret", env("SSEFY_SERVER_SECRET", ""), "server secret for writes")
	cors := flag.String("cors", env("SSEFY_CORS_ORIGINS", ""), "allowed CORS origins, comma separated")
	dev := flag.Bool("dev", false, "development mode")
	cert := flag.String("tls-cert", env("SSEFY_TLS_CERT", ""), "TLS certificate file")
	key := flag.String("tls-key", env("SSEFY_TLS_KEY", ""), "TLS key file")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if *secret == "" {
		if !*dev {
			log.Error("SSEFY_SERVER_SECRET is required unless --dev")
			os.Exit(1)
		}
		*secret = "dev"
		log.Warn("using the insecure development secret")
	}
	if (*cert == "") != (*key == "") {
		log.Error("--tls-cert and --tls-key must be set together")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := openStore(ctx, *dsn)
	if err != nil {
		log.Error("store", "err", err)
		os.Exit(1)
	}
	defer func() { _ = st.Close() }()

	a := api.New(st, *secret, origins(*cors, *dev), log)
	srv := &http.Server{
		Addr:              *listen,
		Handler:           a.Handler(),
		Protocols:         protocols(*cert != ""),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	srv.RegisterOnShutdown(a.Close) // end open SSE streams so Shutdown can finish

	errs := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", *listen, "store", *dsn)
		if *cert != "" {
			errs <- srv.ListenAndServeTLS(*cert, *key)
			return
		}
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

func protocols(tls bool) *http.Protocols {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	if !tls {
		p.SetUnencryptedHTTP2(true) // h2c for a plain listener behind the load balancer
	}
	return p
}

func openStore(ctx context.Context, dsn string) (store.Store, error) {
	if strings.HasPrefix(dsn, "memory://") {
		return store.NewMemory(), nil
	}
	return store.OpenPostgres(ctx, dsn)
}

func origins(cors string, dev bool) []string {
	if cors == "" {
		if dev {
			return []string{"*"}
		}
		return nil
	}
	var out []string
	for _, o := range strings.Split(cors, ",") {
		if o = strings.TrimSpace(o); o != "" {
			out = append(out, o)
		}
	}
	return out
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
