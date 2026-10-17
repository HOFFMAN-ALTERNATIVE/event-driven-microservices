package telemetry

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestPropagationThroughMapAndNATSHeaders(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "root")
	defer span.End()

	// outbox stores a map; the relay copies it into NATS headers.
	stored := Inject(ctx)
	if stored["traceparent"] == "" {
		t.Fatalf("traceparent not injected: %v", stored)
	}
	h := nats.Header{}
	for k, v := range stored {
		h.Set(k, v)
	}

	got := trace.SpanContextFromContext(Extract(context.Background(), h))
	if !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("span context lost: got %v want %v", got, span.SpanContext())
	}
	if len(HeaderCarrier(h).Keys()) == 0 {
		t.Fatal("carrier keys empty")
	}
}

func TestExtractNilHeader(t *testing.T) {
	if sc := trace.SpanContextFromContext(Extract(context.Background(), nil)); sc.IsValid() {
		t.Fatal("expected invalid span context")
	}
}

// Regression test: after an otel upgrade the resource schema URL must match
// the SDK's built-in resource, otherwise every service fails at startup with
// "conflicting Schema URL".
func TestSetupBuildsResource(t *testing.T) {
	shutdown, err := Setup(context.Background(), "test")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
