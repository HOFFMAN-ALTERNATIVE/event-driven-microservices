// Package telemetry wires OpenTelemetry tracing and structured logging, and
// provides the carriers that propagate W3C trace context across NATS messages
// and the outbox table.
package telemetry

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Propagator is the W3C tracecontext + baggage propagator used everywhere.
var Propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// Setup installs a global tracer provider. When OTEL_EXPORTER_OTLP_ENDPOINT is
// set, spans are exported over OTLP/gRPC (e.g. to Jaeger or an OTel
// Collector); otherwise spans are still created so trace ids flow through logs
// and messages, but nothing is exported.
func Setup(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
		semconv.ServiceNamespace("edm"),
	))
	if err != nil {
		return nil, err
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exp, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(time.Second)))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(Propagator)
	return tp.Shutdown, nil
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer("github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/" + name)
}

// HeaderCarrier adapts nats.Header to propagation.TextMapCarrier.
type HeaderCarrier nats.Header

var _ propagation.TextMapCarrier = HeaderCarrier{}

func (c HeaderCarrier) Get(key string) string { return nats.Header(c).Get(key) }
func (c HeaderCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// Inject writes the span context of ctx into a plain map (stored in the outbox).
func Inject(ctx context.Context) map[string]string {
	m := propagation.MapCarrier{}
	Propagator.Inject(ctx, m)
	return m
}

// Extract returns a context carrying the remote span context found in h.
func Extract(ctx context.Context, h nats.Header) context.Context {
	if h == nil {
		return ctx
	}
	return Propagator.Extract(ctx, HeaderCarrier(h))
}

// Logger returns a JSON slog logger tagged with the service name.
func Logger(service string) *slog.Logger {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	return slog.New(traceHandler{slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})}).With("service", service)
}

// traceHandler adds trace_id / span_id to every record logged with a context
// that carries a span, so logs and traces can be joined.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(a)}
}
func (h traceHandler) WithGroup(n string) slog.Handler { return traceHandler{h.Handler.WithGroup(n)} }
