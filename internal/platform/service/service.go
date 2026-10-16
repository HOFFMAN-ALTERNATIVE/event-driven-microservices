// Package service is the shared runtime of every microservice: config,
// database, NATS connection, outbox relay, consumers, HTTP server, health
// endpoints and graceful shutdown.
package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/db"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/httpx"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/natsx"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/outbox"
)

// Config is read from the environment.
type Config struct {
	Name     string
	NATSURL  string
	DBPath   string
	HTTPAddr string
}

// ConfigFromEnv reads NATS_URL, DB_PATH and HTTP_ADDR with local defaults.
func ConfigFromEnv(name string) Config {
	return Config{
		Name:     name,
		NATSURL:  getenv("NATS_URL", nats.DefaultURL),
		DBPath:   getenv("DB_PATH", name+".db"),
		HTTPAddr: getenv("HTTP_ADDR", ":8080"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Base bundles the infrastructure a service needs.
type Base struct {
	Cfg    Config
	DB     *sql.DB
	NC     *nats.Conn
	JS     jetstream.JetStream
	Relay  *outbox.Relay
	Runner *messaging.Runner
	Log    *slog.Logger
}

// New opens the database (applying schema), connects to NATS and declares the
// JetStream topology.
func New(ctx context.Context, cfg Config, log *slog.Logger, schema ...string) (*Base, error) {
	d, err := db.Open(ctx, cfg.DBPath, schema...)
	if err != nil {
		return nil, err
	}
	nc, js, err := natsx.Connect(cfg.NATSURL, cfg.Name, log)
	if err != nil {
		_ = d.Close()
		return nil, err
	}
	// Retry topology creation while NATS (re)starts — compose start-up order is
	// not guaranteed.
	deadline := time.Now().Add(30 * time.Second)
	for {
		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = natsx.EnsureTopology(tctx, js)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			nc.Close()
			_ = d.Close()
			return nil, err
		}
		log.Warn("waiting for jetstream", "err", err)
		time.Sleep(time.Second)
	}
	return &Base{
		Cfg: cfg, DB: d, NC: nc, JS: js, Log: log,
		Relay:  outbox.NewRelay(d, js, log),
		Runner: messaging.NewRunner(d, js, nil, log),
	}, nil
}

// Start launches the outbox relay and the given consumers. stop() drains
// consumers and stops the relay.
func (b *Base) Start(ctx context.Context, consumers ...messaging.Consumer) (stop func(), err error) {
	rctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.Relay.Run(rctx)
	}()
	stops := make([]func(), 0, len(consumers))
	for _, c := range consumers {
		if c.AfterCommit == nil {
			c.AfterCommit = b.Relay.Kick
		}
		s, err := b.Runner.Start(ctx, c)
		if err != nil {
			for _, s := range stops {
				s()
			}
			cancel()
			wg.Wait()
			return nil, err
		}
		stops = append(stops, s)
	}
	return func() {
		for _, s := range stops {
			s()
		}
		cancel()
		wg.Wait()
	}, nil
}

// Close releases NATS and database resources.
func (b *Base) Close() {
	if b.NC != nil {
		_ = b.NC.Drain()
	}
	if b.DB != nil {
		_ = b.DB.Close()
	}
}

// Handler wraps routes with tracing and adds /healthz and /readyz.
func (b *Base) Handler(routes http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pending, dbErr := outbox.Pending(r.Context(), b.DB)
		body := map[string]any{"nats": b.NC.Status().String(), "outbox_pending": pending}
		if dbErr != nil || !b.NC.IsConnected() {
			body["status"] = "unavailable"
			if dbErr != nil {
				body["db_error"] = dbErr.Error()
			}
			httpx.JSON(w, http.StatusServiceUnavailable, body)
			return
		}
		body["status"] = "ready"
		httpx.JSON(w, http.StatusOK, body)
	})
	mux.Handle("/", routes)
	return otelhttp.NewHandler(mux, b.Cfg.Name,
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path }),
		otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/healthz" && r.URL.Path != "/readyz" }),
	)
}

// Run is the full lifecycle used by the binaries: start background workers,
// serve HTTP, and shut everything down gracefully on ctx cancellation.
func (b *Base) Run(ctx context.Context, routes http.Handler, consumers ...messaging.Consumer) error {
	defer b.Close()
	stop, err := b.Start(ctx, consumers...)
	if err != nil {
		return err
	}
	defer stop()

	srv := &http.Server{
		Addr:              b.Cfg.HTTPAddr,
		Handler:           b.Handler(routes),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		b.Log.Info("http listening", "addr", srv.Addr)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: %w", err)
		}
		return nil
	case <-ctx.Done():
	}
	b.Log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}
