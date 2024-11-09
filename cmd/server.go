package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0xc0d3d00d/candledb/internal/connect/handler"
	"github.com/0xc0d3d00d/candledb/internal/connect/server"
	"github.com/0xc0d3d00d/candledb/internal/storage"
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
	"golang.org/x/sync/errgroup"
)

type config struct {
	DataDir       string `env:"DATA_DIR" envDefault:"./data"`
	ListenAddress string `env:"ADDR" envDefault:":6969"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// set global logger with custom options
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.DateTime,
		}),
	))

	cfg := config{}
	err := loadConfig(&cfg)
	if err != nil {
		slog.ErrorContext(ctx, "failed to load config", "error", err)
		os.Exit(1)
	}

	db, err := storage.NewStorage(cfg.DataDir, 10000)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create storage", "error", err)
		os.Exit(1)
	}

	handler := handler.NewHandler(db)
	connectServer, err := server.New(ctx, cfg.ListenAddress, server.WithHandlerFunc(handler.HTTPHandler))
	if err != nil {
		slog.ErrorContext(ctx, "failed to create server", "error", err)
		os.Exit(1)
	}

	g, gCtx := errgroup.WithContext(ctx)
	// Start Connect server
	g.Go(func() error {
		slog.InfoContext(ctx, "starting server", "listen_address", cfg.ListenAddress)
		if err := runHttpServer(ctx, cfg.ListenAddress, connectServer); err != nil {
			slog.ErrorContext(ctx, "failed to start server", "error", err)
			cancel()
			return err
		}
		return nil
	})

	// Handle graceful shutdown
	g.Go(func() error {
		<-gCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		slog.Info("shutting down server gracefully")

		return connectServer.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil {
		slog.Error("server terminated", "err", err)
	}
}

func runHttpServer(ctx context.Context, listenAddress string, srv *server.Server) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", listenAddress)
	if err != nil {
		return err
	}

	err = srv.Serve(lis)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
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
