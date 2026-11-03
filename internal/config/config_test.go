package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeEnv(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDotenv(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeEnv(t, dir, "SSEFY_SERVER_SECRET=s3cret\nSSEFY_LISTEN=:9000\nSSEFY_CORS_ORIGINS= a.example , b.example \n")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Secret != "s3cret" || cfg.Listen != ":9000" || cfg.Store != "memory://" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if len(cfg.CORS) != 2 || cfg.CORS[0] != "a.example" || cfg.CORS[1] != "b.example" {
		t.Fatalf("cors = %v", cfg.CORS)
	}
}

func TestEnvironmentWinsOverDotenv(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeEnv(t, dir, "SSEFY_SERVER_SECRET=from-file\nSSEFY_LISTEN=:9000\n")
	t.Setenv("SSEFY_LISTEN", ":7000")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":7000" {
		t.Fatalf("listen = %q, want :7000", cfg.Listen)
	}
	if cfg.Secret != "from-file" {
		t.Fatalf("secret = %q", cfg.Secret)
	}
}

func TestExplicitFileMustExist(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.env")); err == nil {
		t.Fatal("expected an error for a missing --env-file")
	}
}

func TestSecretRequired(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("SSEFY_SERVER_SECRET", "")
	if _, err := Load(""); err == nil {
		t.Fatal("expected an error without a secret")
	}
}

func TestDevDefaults(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("SSEFY_SERVER_SECRET", "")
	t.Setenv("SSEFY_DEV", "true")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Secret != "dev" || len(cfg.CORS) != 1 || cfg.CORS[0] != "*" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestStoreMustBeKnown(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("SSEFY_SERVER_SECRET", "s")
	t.Setenv("SSEFY_STORE", "redis://nope")
	if _, err := Load(""); err == nil {
		t.Fatal("expected an error for an unknown store")
	}
}
