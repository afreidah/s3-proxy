// -------------------------------------------------------------------------------
// Tracing - OpenTelemetry Instrumentation
//
// Author: Alex Freidah
//
// OpenTelemetry tracer setup and span helpers. Supports OTLP export to collectors
// like Jaeger, Tempo, or any OTLP-compatible backend. Provides context propagation
// for distributed tracing across service boundaries.
// -------------------------------------------------------------------------------

package telemetry

import (
	"context"
	"fmt"

	"github.com/afreidah/s3-orchestrator/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// -------------------------------------------------------------------------
// CONSTANTS
// -------------------------------------------------------------------------

const (
	// TracerName identifies spans created by this service.
	TracerName = "s3proxy"
)

// Version of the service for trace metadata. Set at build time via
// -ldflags "-X github.com/afreidah/s3-orchestrator/internal/telemetry.Version=..."
var Version = "dev"

// -------------------------------------------------------------------------
// TRACER SETUP
// -------------------------------------------------------------------------

// InitTracer initializes the OpenTelemetry tracer with OTLP export. Returns a
// shutdown function that should be called on service termination to flush spans.
func InitTracer(ctx context.Context, cfg config.TracingConfig) (func(context.Context) error, error) {
	if !cfg.Enabled {
		// Return no-op shutdown when tracing is disabled
		return func(context.Context) error { return nil }, nil
	}

	// --- Create OTLP exporter ---
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// --- Create resource with service info ---
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(TracerName),
			semconv.ServiceVersion(Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// --- Configure sampler ---
	var sampler sdktrace.Sampler
	switch {
	case cfg.SampleRate >= 1.0:
		sampler = sdktrace.AlwaysSample()
	case cfg.SampleRate <= 0:
		sampler = sdktrace.NeverSample()
	default:
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRate)
	}

	// --- Create trace provider ---
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// --- Set global tracer provider ---
	otel.SetTracerProvider(tp)

	// --- Set propagator for context propagation ---
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// -------------------------------------------------------------------------
// SPAN HELPERS
// -------------------------------------------------------------------------

// Tracer returns the global tracer for this service.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// StartSpan creates a new span with the given name and attributes.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

// -------------------------------------------------------------------------
// COMMON ATTRIBUTES
// -------------------------------------------------------------------------

// S3 proxy specific attribute keys.
var (
	AttrVirtualBucket   = attribute.Key("s3proxy.bucket.virtual")
	AttrBackendBucket   = attribute.Key("s3proxy.bucket.backend")
	AttrObjectKey       = attribute.Key("s3proxy.key")
	AttrBackendName     = attribute.Key("s3proxy.backend.name")
	AttrBackendEndpoint = attribute.Key("s3proxy.backend.endpoint")
	AttrObjectSize      = attribute.Key("s3proxy.object.size")
	AttrContentType     = attribute.Key("s3proxy.object.content_type")
	AttrOperation       = attribute.Key("s3proxy.operation")
)

// RequestAttributes returns common attributes for HTTP request spans.
func RequestAttributes(method, path, bucket, key, clientIP string) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.HTTPRequestMethodKey.String(method),
		semconv.URLPath(path),
		AttrVirtualBucket.String(bucket),
		AttrObjectKey.String(key),
		semconv.ClientAddress(clientIP),
	}
}

// BackendAttributes returns common attributes for backend operation spans.
func BackendAttributes(operation, backendName, endpoint, bucket, key string) []attribute.KeyValue {
	return []attribute.KeyValue{
		AttrOperation.String(operation),
		AttrBackendName.String(backendName),
		AttrBackendEndpoint.String(endpoint),
		AttrBackendBucket.String(bucket),
		AttrObjectKey.String(key),
	}
}
