package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type config struct {
	DataDir string `env:"DATA_DIR" envDefault:"./data"`
}

func main() {
	ctx := context.Background()

	cfg := config{}
	err := loadConfig(&cfg)
	if err != nil {
		slog.ErrorContext(ctx, "failed to load config", "error", err)
		os.Exit(1)
	}
}

func loadConfig(config any) error {
	// Ignore error if .env is missing
	err := godotenv.Load()

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Parse for built-in types
	if err := env.Parse(config); err != nil {
		return err
	}

	return nil
}
