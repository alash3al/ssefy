// Package config loads ssefy settings from a .env file and the environment.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config is the typed environment of one process.
type Config struct {
	Listen string   `env:"SSEFY_LISTEN" envDefault:":8080"`
	Store  string   `env:"SSEFY_STORE" envDefault:"memory://"`
	Secret string   `env:"SSEFY_SERVER_SECRET"`
	CORS   []string `env:"SSEFY_CORS_ORIGINS" envSeparator:","`
	Dev    bool     `env:"SSEFY_DEV"`
}

// Load reads path first, then the process environment; real environment
// variables win over the file. An empty path means ./.env, and only then is a
// missing file acceptable. The process environment is never modified.
func Load(path string) (Config, error) {
	defaultPath := path == ""
	if defaultPath {
		cwd, err := os.Getwd()
		if err != nil {
			return Config{}, err
		}
		path = filepath.Join(cwd, ".env")
	}

	values := environ()
	fileVars, err := godotenv.Read(path)
	switch {
	case err == nil:
		for key, value := range fileVars {
			if _, set := values[key]; !set {
				values[key] = value
			}
		}
	case defaultPath && errors.Is(err, fs.ErrNotExist):
		// no .env is fine
	default:
		return Config{}, err
	}

	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: values}); err != nil {
		return Config{}, err
	}
	cfg.CORS = cleanOrigins(cfg.CORS)
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	switch {
	case c.Secret == "" && c.Dev:
		c.Secret = "dev"
	case c.Secret == "":
		return errors.New("SSEFY_SERVER_SECRET is required (or set SSEFY_DEV=true)")
	}
	if c.Dev && len(c.CORS) == 0 {
		c.CORS = []string{"*"}
	}
	if !strings.HasPrefix(c.Store, "memory://") &&
		!strings.HasPrefix(c.Store, "postgres://") &&
		!strings.HasPrefix(c.Store, "postgresql://") {
		return fmt.Errorf("SSEFY_STORE must be memory:// or postgres://, got %q", c.Store)
	}
	return nil
}

func environ() map[string]string {
	out := make(map[string]string, len(os.Environ()))
	for _, kv := range os.Environ() {
		key, value, _ := strings.Cut(kv, "=")
		out[key] = value
	}
	return out
}

func cleanOrigins(in []string) []string {
	out := in[:0]
	for _, origin := range in {
		if origin = strings.TrimSpace(origin); origin != "" {
			out = append(out, origin)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
