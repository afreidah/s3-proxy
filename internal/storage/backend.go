// -------------------------------------------------------------------------------
// Backend - S3-Compatible Storage Client
//
// Project: Munchbox / Author: Alex Freidah
//
// Storage backend implementation using AWS SDK v2. Connects to any S3-compatible
// endpoint (OCI, AWS, B2, MinIO) via custom endpoint configuration. The same code
// works for all providers since they all speak the S3 protocol.
// -------------------------------------------------------------------------------

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/afreidah/s3-proxy/internal/config"
	"github.com/afreidah/s3-proxy/internal/telemetry"
	"go.opentelemetry.io/otel/codes"
)

// -------------------------------------------------------------------------
// INTERFACE
// -------------------------------------------------------------------------

// GetObjectResult holds the response from a GetObject call.
type GetObjectResult struct {
	Body         io.ReadCloser
	Size         int64
	ContentType  string
	ETag         string
	ContentRange string
}

// ObjectBackend defines the interface for object storage operations.
type ObjectBackend interface {
	PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string) (etag string, err error)
	GetObject(ctx context.Context, key string, rangeHeader string) (*GetObjectResult, error)
	HeadObject(ctx context.Context, key string) (size int64, contentType string, etag string, err error)
	DeleteObject(ctx context.Context, key string) error
}

// -------------------------------------------------------------------------
// S3 BACKEND IMPLEMENTATION
// -------------------------------------------------------------------------

// S3Backend implements ObjectBackend using AWS SDK v2.
type S3Backend struct {
	client   *s3.Client
	bucket   string
	name     string
	endpoint string
}

// NewS3Backend creates a new S3-compatible backend client. Uses BaseEndpoint
// to direct requests to the configured provider instead of AWS.
func NewS3Backend(cfg config.BackendConfig) (*S3Backend, error) {
	// --- Create S3 client with custom endpoint ---
	client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.Endpoint),
		UsePathStyle: cfg.ForcePathStyle,
	})

	return &S3Backend{
		client:   client,
		bucket:   cfg.Bucket,
		name:     cfg.Name,
		endpoint: cfg.Endpoint,
	}, nil
}

// -------------------------------------------------------------------------
// CRUD OPERATIONS
// -------------------------------------------------------------------------

// PutObject uploads an object to the backend.
func (b *S3Backend) PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string) (string, error) {
	const operation = "PutObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Backend "+operation,
		telemetry.BackendAttributes(operation, b.name, b.endpoint, b.bucket, key)...,
	)
	defer span.End()

	// The AWS SDK requires a seekable body to compute the SigV4 payload hash.
	// HTTP request bodies are not seekable, so buffer when necessary.
	var seekableBody io.ReadSeeker
	if rs, ok := body.(io.ReadSeeker); ok {
		seekableBody = rs
	} else {
		data, err := io.ReadAll(body)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return "", fmt.Errorf("failed to read body: %w", err)
		}
		seekableBody = bytes.NewReader(data)
	}

	input := &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          seekableBody,
		ContentLength: aws.Int64(size),
	}

	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	result, err := b.client.PutObject(ctx, input)

	// --- Record metrics ---
	b.recordOperation(operation, start, err)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("put object failed: %w", err)
	}

	etag := ""
	if result.ETag != nil {
		etag = *result.ETag
	}
	return etag, nil
}

// GetObject retrieves an object from the backend. When rangeHeader is non-empty
// (e.g. "bytes=0-99"), it is passed through to S3 and the response includes a
// contentRange value (e.g. "bytes 0-99/1000") for 206 Partial Content responses.
func (b *S3Backend) GetObject(ctx context.Context, key string, rangeHeader string) (*GetObjectResult, error) {
	const operation = "GetObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Backend "+operation,
		telemetry.BackendAttributes(operation, b.name, b.endpoint, b.bucket, key)...,
	)
	defer span.End()

	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	result, err := b.client.GetObject(ctx, input)

	// --- Record metrics ---
	b.recordOperation(operation, start, err)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, fmt.Errorf("get object failed: %w", err)
	}

	out := &GetObjectResult{Body: result.Body}

	if result.ContentLength != nil {
		out.Size = *result.ContentLength
	}
	if result.ContentType != nil {
		out.ContentType = *result.ContentType
	} else {
		out.ContentType = "application/octet-stream"
	}
	if result.ETag != nil {
		out.ETag = *result.ETag
	}
	if result.ContentRange != nil {
		out.ContentRange = *result.ContentRange
	}

	return out, nil
}

// HeadObject retrieves object metadata without the body.
func (b *S3Backend) HeadObject(ctx context.Context, key string) (int64, string, string, error) {
	const operation = "HeadObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Backend "+operation,
		telemetry.BackendAttributes(operation, b.name, b.endpoint, b.bucket, key)...,
	)
	defer span.End()

	result, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})

	// --- Record metrics ---
	b.recordOperation(operation, start, err)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return 0, "", "", fmt.Errorf("head object failed: %w", err)
	}

	size := int64(0)
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	contentType := "application/octet-stream"
	if result.ContentType != nil {
		contentType = *result.ContentType
	}

	etag := ""
	if result.ETag != nil {
		etag = *result.ETag
	}

	return size, contentType, etag, nil
}

// DeleteObject removes an object from the backend.
func (b *S3Backend) DeleteObject(ctx context.Context, key string) error {
	const operation = "DeleteObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Backend "+operation,
		telemetry.BackendAttributes(operation, b.name, b.endpoint, b.bucket, key)...,
	)
	defer span.End()

	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})

	// --- Record metrics ---
	b.recordOperation(operation, start, err)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return fmt.Errorf("delete object failed: %w", err)
	}
	return nil
}

// -------------------------------------------------------------------------
// LISTING
// -------------------------------------------------------------------------

// ListedObject holds metadata for a single object returned by S3 ListObjects.
type ListedObject struct {
	Key       string
	SizeBytes int64
}

// ListObjects iterates all objects in the backend bucket with the given prefix,
// calling fn for each page of results. Uses ListObjectsV2 pagination internally.
func (b *S3Backend) ListObjects(ctx context.Context, prefix string, fn func([]ListedObject) error) error {
	const operation = "ListObjectsV2"

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	paginator := s3.NewListObjectsV2Paginator(b.client, input)
	for paginator.HasMorePages() {
		start := time.Now()
		page, err := paginator.NextPage(ctx)
		b.recordOperation(operation, start, err)

		if err != nil {
			return fmt.Errorf("list objects failed: %w", err)
		}

		objects := make([]ListedObject, len(page.Contents))
		for i, obj := range page.Contents {
			key := ""
			if obj.Key != nil {
				key = *obj.Key
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			objects[i] = ListedObject{Key: key, SizeBytes: size}
		}

		if len(objects) > 0 {
			if err := fn(objects); err != nil {
				return err
			}
		}
	}

	return nil
}

// -------------------------------------------------------------------------
// METRICS HELPER
// -------------------------------------------------------------------------

// recordOperation updates Prometheus metrics for a backend operation.
func (b *S3Backend) recordOperation(operation string, start time.Time, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}

	telemetry.BackendRequestsTotal.WithLabelValues(operation, b.name, status).Inc()
	telemetry.BackendDuration.WithLabelValues(operation, b.name).Observe(time.Since(start).Seconds())
}
