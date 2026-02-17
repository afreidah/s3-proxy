//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/munchbox/s3-proxy/internal/config"
	"github.com/munchbox/s3-proxy/internal/storage"
)

// -------------------------------------------------------------------------
// CRUD
// -------------------------------------------------------------------------

func TestCRUD(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()

	t.Run("PutGetRoundTrip", func(t *testing.T) {
		key := uniqueKey(t, "crud")
		body := bytes.Repeat([]byte("A"), 100)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(100),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}

		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		defer resp.Body.Close()

		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("body mismatch: got %d bytes, want %d", len(got), len(body))
		}
	})

	t.Run("PutHeadMetadata", func(t *testing.T) {
		key := uniqueKey(t, "crud")
		body := bytes.Repeat([]byte("B"), 200)

		putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(200),
			ContentType:   aws.String("text/plain"),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}

		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("HeadObject: %v", err)
		}

		if got := aws.ToInt64(head.ContentLength); got != 200 {
			t.Errorf("ContentLength = %d, want 200", got)
		}
		if got := aws.ToString(head.ContentType); got != "text/plain" {
			t.Errorf("ContentType = %q, want %q", got, "text/plain")
		}
		if head.ETag == nil || *head.ETag == "" {
			t.Error("ETag is empty")
		}
		if putResp.ETag != nil && head.ETag != nil && *putResp.ETag != *head.ETag {
			t.Errorf("ETag mismatch: put=%q head=%q", *putResp.ETag, *head.ETag)
		}
	})

	t.Run("PutDeleteGet404", func(t *testing.T) {
		key := uniqueKey(t, "crud")
		body := []byte("delete-me")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}

		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}

		_, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
		})
		if err == nil {
			t.Fatal("expected error for GET after DELETE, got nil")
		}
		assertHTTPStatus(t, err, 404)
	})

	t.Run("GetNonexistent", func(t *testing.T) {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(fmt.Sprintf("nonexistent-%d", time.Now().UnixNano())),
		})
		if err == nil {
			t.Fatal("expected error for nonexistent key, got nil")
		}
		assertHTTPStatus(t, err, 404)
	})
}

// -------------------------------------------------------------------------
// QUOTA ROUTING
// -------------------------------------------------------------------------

func TestQuotaRouting(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()

	t.Run("SmallObjectLandsOnFirstBackend", func(t *testing.T) {
		resetState(t)

		key := uniqueKey(t, "quota")
		body := bytes.Repeat([]byte("X"), 100)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(100),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}

		backend := queryObjectBackend(t, key)
		if backend != "minio-1" {
			t.Errorf("object landed on %q, want %q", backend, "minio-1")
		}
	})

	t.Run("OverflowToSecondBackend", func(t *testing.T) {
		resetState(t)

		// Fill minio-1 near capacity (900 of 1024 bytes)
		fillKey := uniqueKey(t, "fill")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(fillKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("F"), 900)),
			ContentLength: aws.Int64(900),
		})
		if err != nil {
			t.Fatalf("fill PutObject: %v", err)
		}

		// 200 bytes won't fit minio-1 (900+200=1100 > 1024), should overflow to minio-2
		overflowKey := uniqueKey(t, "overflow")
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(overflowKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("O"), 200)),
			ContentLength: aws.Int64(200),
		})
		if err != nil {
			t.Fatalf("overflow PutObject: %v", err)
		}

		backend := queryObjectBackend(t, overflowKey)
		if backend != "minio-2" {
			t.Errorf("overflow object landed on %q, want %q", backend, "minio-2")
		}
	})

	t.Run("AllBackendsFull507", func(t *testing.T) {
		resetState(t)

		// Fill minio-1 to capacity (1024 bytes)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(uniqueKey(t, "full1")),
			Body:          bytes.NewReader(bytes.Repeat([]byte("A"), 1024)),
			ContentLength: aws.Int64(1024),
		})
		if err != nil {
			t.Fatalf("fill minio-1: %v", err)
		}

		// Fill minio-2 to capacity (2048 bytes)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(uniqueKey(t, "full2")),
			Body:          bytes.NewReader(bytes.Repeat([]byte("B"), 2048)),
			ContentLength: aws.Int64(2048),
		})
		if err != nil {
			t.Fatalf("fill minio-2: %v", err)
		}

		// 1 more byte should fail with 507
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(uniqueKey(t, "excess")),
			Body:          bytes.NewReader([]byte("X")),
			ContentLength: aws.Int64(1),
		})
		if err == nil {
			t.Fatal("expected error when all backends full, got nil")
		}
		assertHTTPStatus(t, err, 507)
	})

	t.Run("DeleteFreesQuotaThenPutSucceeds", func(t *testing.T) {
		resetState(t)

		// Fill minio-1 exactly
		fillKey := uniqueKey(t, "del-quota")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(fillKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("D"), 1024)),
			ContentLength: aws.Int64(1024),
		})
		if err != nil {
			t.Fatalf("fill minio-1: %v", err)
		}

		// Fill minio-2 exactly
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(uniqueKey(t, "del-quota2")),
			Body:          bytes.NewReader(bytes.Repeat([]byte("E"), 2048)),
			ContentLength: aws.Int64(2048),
		})
		if err != nil {
			t.Fatalf("fill minio-2: %v", err)
		}

		if used := queryQuotaUsed(t, "minio-1"); used != 1024 {
			t.Fatalf("expected 1024 bytes used on minio-1, got %d", used)
		}

		// Delete from minio-1
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(fillKey),
		})
		if err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}

		if used := queryQuotaUsed(t, "minio-1"); used != 0 {
			t.Errorf("expected 0 bytes used after delete, got %d", used)
		}

		// New PUT should succeed on minio-1
		newKey := uniqueKey(t, "reuse")
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(newKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("N"), 500)),
			ContentLength: aws.Int64(500),
		})
		if err != nil {
			t.Fatalf("PutObject after delete: %v", err)
		}

		backend := queryObjectBackend(t, newKey)
		if backend != "minio-1" {
			t.Errorf("new object landed on %q, want %q", backend, "minio-1")
		}
	})
}

// -------------------------------------------------------------------------
// RANGE REQUESTS
// -------------------------------------------------------------------------

func TestRangeRequests(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()

	key := uniqueKey(t, "range")
	body := make([]byte, 256)
	for i := range body {
		body[i] = byte(i)
	}

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(256),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	t.Run("PartialGet206", func(t *testing.T) {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
			Range:  aws.String("bytes=0-99"),
		})
		if err != nil {
			t.Fatalf("GetObject with Range: %v", err)
		}
		defer resp.Body.Close()

		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(got) != 100 {
			t.Errorf("got %d bytes, want 100", len(got))
		}
		if !bytes.Equal(got, body[:100]) {
			t.Error("partial body doesn't match expected range")
		}
		if resp.ContentRange == nil || *resp.ContentRange == "" {
			t.Error("expected Content-Range header in response")
		}
	})

	t.Run("FullGetHasAcceptRanges", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/%s/%s", proxyAddr, virtualBucket, key)
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)

		if resp.StatusCode != 200 {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
			t.Errorf("Accept-Ranges = %q, want %q", got, "bytes")
		}
	})

	t.Run("HeadHasAcceptRanges", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/%s/%s", proxyAddr, virtualBucket, key)
		req, _ := http.NewRequestWithContext(ctx, "HEAD", url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("HEAD: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
			t.Errorf("Accept-Ranges = %q, want %q", got, "bytes")
		}
	})
}

// -------------------------------------------------------------------------
// MULTIPART UPLOAD
// -------------------------------------------------------------------------

func TestMultipartUpload(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()

	key := uniqueKey(t, "multipart")
	part1Data := bytes.Repeat([]byte("1"), 100)
	part2Data := bytes.Repeat([]byte("2"), 100)

	create, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := create.UploadId

	up1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		UploadId:      uploadID,
		PartNumber:    aws.Int32(1),
		Body:          bytes.NewReader(part1Data),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}

	up2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		UploadId:      uploadID,
		PartNumber:    aws.Int32(2),
		Body:          bytes.NewReader(part2Data),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(virtualBucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: up1.ETag},
				{PartNumber: aws.Int32(2), ETag: up2.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer getResp.Body.Close()

	got, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	expected := append(part1Data, part2Data...)
	if !bytes.Equal(got, expected) {
		t.Errorf("assembled body mismatch: got %d bytes, want %d", len(got), len(expected))
	}
}

// -------------------------------------------------------------------------
// LIST AND COPY
// -------------------------------------------------------------------------

func TestListAndCopy(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()

	t.Run("ListObjectsV2", func(t *testing.T) {
		prefix := fmt.Sprintf("list-test/%d/", time.Now().UnixNano())
		keys := []string{prefix + "a", prefix + "b", prefix + "c"}

		for _, k := range keys {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:        aws.String(virtualBucket),
				Key:           aws.String(k),
				Body:          bytes.NewReader([]byte("data")),
				ContentLength: aws.Int64(4),
			})
			if err != nil {
				t.Fatalf("PutObject(%s): %v", k, err)
			}
		}

		list, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(virtualBucket),
			Prefix: aws.String(prefix),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}

		if len(list.Contents) != 3 {
			t.Errorf("got %d objects, want 3", len(list.Contents))
		}

		found := make(map[string]bool)
		for _, obj := range list.Contents {
			found[*obj.Key] = true
		}
		for _, k := range keys {
			if !found[k] {
				t.Errorf("missing key %q in list results", k)
			}
		}
	})

	t.Run("CopyObject", func(t *testing.T) {
		srcKey := uniqueKey(t, "copy-src")
		dstKey := uniqueKey(t, "copy-dst")
		body := []byte("copy-me-please")

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(srcKey),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
		})
		if err != nil {
			t.Fatalf("PutObject source: %v", err)
		}

		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(virtualBucket),
			Key:        aws.String(dstKey),
			CopySource: aws.String(virtualBucket + "/" + srcKey),
		})
		if err != nil {
			t.Fatalf("CopyObject: %v", err)
		}

		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(dstKey),
		})
		if err != nil {
			t.Fatalf("GetObject copy: %v", err)
		}
		defer getResp.Body.Close()

		got, err := io.ReadAll(getResp.Body)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !bytes.Equal(got, body) {
			t.Errorf("copied body mismatch: got %q, want %q", got, body)
		}
	})
}

// -------------------------------------------------------------------------
// REBALANCER
// -------------------------------------------------------------------------

func TestRebalancePackTight(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// Setup: fill minio-1 to force overflow, then free space so pack can pull back.
	// Step 1: fill minio-1 completely
	fillKey := uniqueKey(t, "pack-fill")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(fillKey),
		Body:          bytes.NewReader(bytes.Repeat([]byte("F"), 1024)),
		ContentLength: aws.Int64(1024),
	})
	if err != nil {
		t.Fatalf("PutObject fill: %v", err)
	}

	// Step 2: these overflow to minio-2 (minio-1 is full)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("rebal-pack/obj-%d-%d", i, time.Now().UnixNano())
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(bytes.Repeat([]byte("P"), 100)),
			ContentLength: aws.Int64(100),
		})
		if err != nil {
			t.Fatalf("PutObject overflow %d: %v", i, err)
		}
	}

	// Step 3: delete fill and put a smaller object so minio-1 has room
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(fillKey),
	})
	refillKey := uniqueKey(t, "pack-refill")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(refillKey),
		Body:          bytes.NewReader(bytes.Repeat([]byte("R"), 600)),
		ContentLength: aws.Int64(600),
	})
	if err != nil {
		t.Fatalf("PutObject refill: %v", err)
	}

	// State: minio-1=600/1024 (58.6%), minio-2=300/2048 (14.6%)
	// minio-1 is more full and has 424 bytes free, enough for 3 x 100-byte objects
	m1Before := queryQuotaUsed(t, "minio-1")
	m2Before := queryQuotaUsed(t, "minio-2")
	t.Logf("before pack: minio-1=%d (%.1f%%) minio-2=%d (%.1f%%)",
		m1Before, float64(m1Before)/1024*100, m2Before, float64(m2Before)/2048*100)

	packCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "pack",
		BatchSize: 10,
		Threshold: 0,
	}

	moved, err := testManager.Rebalance(ctx, packCfg)
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}

	m1Used := queryQuotaUsed(t, "minio-1")
	m2Used := queryQuotaUsed(t, "minio-2")
	t.Logf("after pack: moved %d, minio-1=%d (%.1f%%) minio-2=%d (%.1f%%)",
		moved, m1Used, float64(m1Used)/1024*100, m2Used, float64(m2Used)/2048*100)

	// minio-1 should be more packed (objects pulled from minio-2)
	if moved == 0 {
		t.Error("pack should have moved objects from minio-2 into minio-1")
	}
	if m1Used <= m1Before {
		t.Errorf("minio-1 should be more packed: before=%d after=%d", m1Before, m1Used)
	}

	// Total bytes conserved
	if m1Used+m2Used != m1Before+m2Before {
		t.Errorf("total bytes_used = %d, want %d", m1Used+m2Used, m1Before+m2Before)
	}

	// No-op case: all objects already on the most-full backend
	resetState(t)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("rebal-pack/noop-%d-%d", i, time.Now().UnixNano())
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(bytes.Repeat([]byte("N"), 200)),
			ContentLength: aws.Int64(200),
		})
		if err != nil {
			t.Fatalf("PutObject noop %d: %v", i, err)
		}
	}

	// minio-1 is 97.6% (1000/1024), minio-2 is 0% — nothing to consolidate
	moved, err = testManager.Rebalance(ctx, packCfg)
	if err != nil {
		t.Fatalf("Rebalance noop: %v", err)
	}
	t.Logf("noop pack: moved %d objects", moved)
	if moved != 0 {
		t.Errorf("pack moved %d objects, want 0 (nothing to consolidate)", moved)
	}
}

func TestRebalancePackTinyToFuller(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()

	packCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "pack",
		BatchSize: 10,
		Threshold: 0,
	}

	t.Run("DestHasRoom", func(t *testing.T) {
		resetState(t)

		// Fill minio-1 to force overflow, then replace with a tiny object
		fillKey := uniqueKey(t, "tiny-fill")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(fillKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("F"), 1024)),
			ContentLength: aws.Int64(1024),
		})
		if err != nil {
			t.Fatalf("PutObject fill: %v", err)
		}

		// Overflow a 1000-byte object to minio-2
		bigKey := uniqueKey(t, "tiny-big")
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(bigKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("B"), 1000)),
			ContentLength: aws.Int64(1000),
		})
		if err != nil {
			t.Fatalf("PutObject big: %v", err)
		}

		// Delete fill, put a tiny object on minio-1
		_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(fillKey),
		})
		tinyKey := uniqueKey(t, "tiny-obj")
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(tinyKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("T"), 100)),
			ContentLength: aws.Int64(100),
		})
		if err != nil {
			t.Fatalf("PutObject tiny: %v", err)
		}

		// State: minio-1=100/1024 (9.8%), minio-2=1000/2048 (48.8%)
		m1Before := queryQuotaUsed(t, "minio-1")
		m2Before := queryQuotaUsed(t, "minio-2")
		t.Logf("before: minio-1=%d (%.1f%%) minio-2=%d (%.1f%%)",
			m1Before, float64(m1Before)/1024*100, m2Before, float64(m2Before)/2048*100)

		moved, err := testManager.Rebalance(ctx, packCfg)
		if err != nil {
			t.Fatalf("Rebalance: %v", err)
		}

		m1Used := queryQuotaUsed(t, "minio-1")
		m2Used := queryQuotaUsed(t, "minio-2")
		t.Logf("after: moved %d, minio-1=%d (%.1f%%) minio-2=%d (%.1f%%)",
			moved, m1Used, float64(m1Used)/1024*100, m2Used, float64(m2Used)/2048*100)

		// Tiny object should move from minio-1 to minio-2
		if moved != 1 {
			t.Errorf("moved = %d, want 1", moved)
		}
		if m2Used != 1100 {
			t.Errorf("minio-2 bytes_used = %d, want 1100", m2Used)
		}
		if m1Used != 0 {
			t.Errorf("minio-1 bytes_used = %d, want 0", m1Used)
		}

		// Object still accessible
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(tinyKey),
		})
		if err != nil {
			t.Errorf("tiny object not accessible: %v", err)
		}
	})

	t.Run("DestIsFull", func(t *testing.T) {
		resetState(t)

		// Fill minio-1 to force overflow, then replace with tiny object
		fillKey := uniqueKey(t, "full-fill")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(fillKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("F"), 1024)),
			ContentLength: aws.Int64(1024),
		})
		if err != nil {
			t.Fatalf("PutObject fill: %v", err)
		}

		// Fill minio-2 completely
		bigKey := uniqueKey(t, "full-big")
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(bigKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("B"), 2048)),
			ContentLength: aws.Int64(2048),
		})
		if err != nil {
			t.Fatalf("PutObject big: %v", err)
		}

		// Delete fill, put tiny on minio-1
		_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(fillKey),
		})
		tinyKey := uniqueKey(t, "full-tiny")
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(tinyKey),
			Body:          bytes.NewReader(bytes.Repeat([]byte("T"), 100)),
			ContentLength: aws.Int64(100),
		})
		if err != nil {
			t.Fatalf("PutObject tiny: %v", err)
		}

		// State: minio-1=100/1024 (9.8%), minio-2=2048/2048 (100%)
		t.Logf("before: minio-1=%d minio-2=%d",
			queryQuotaUsed(t, "minio-1"), queryQuotaUsed(t, "minio-2"))

		moved, err := testManager.Rebalance(ctx, packCfg)
		if err != nil {
			t.Fatalf("Rebalance: %v", err)
		}

		t.Logf("after: moved %d, minio-1=%d minio-2=%d",
			moved, queryQuotaUsed(t, "minio-1"), queryQuotaUsed(t, "minio-2"))

		// Nothing should move — minio-2 is full, can't pack into it
		if moved != 0 {
			t.Errorf("moved = %d, want 0 (destination full)", moved)
		}
		if got := queryQuotaUsed(t, "minio-1"); got != 100 {
			t.Errorf("minio-1 bytes_used = %d, want 100", got)
		}
	})
}

func TestRebalanceSpreadEven(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// Fill minio-1 near capacity: 5 x 200 = 1000 bytes (97.6% of 1024)
	// minio-2 is empty (0% of 2048)
	// Spread should equalize: target = 1000/3072 = 32.5%
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("rebal-spread/obj-%d-%d", i, time.Now().UnixNano())
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(bytes.Repeat([]byte("S"), 200)),
			ContentLength: aws.Int64(200),
		})
		if err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
	}

	// Verify initial state
	if got := queryQuotaUsed(t, "minio-1"); got != 1000 {
		t.Fatalf("minio-1 bytes_used = %d, want 1000", got)
	}
	if got := queryQuotaUsed(t, "minio-2"); got != 0 {
		t.Fatalf("minio-2 bytes_used = %d, want 0", got)
	}

	spreadCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "spread",
		BatchSize: 10,
		Threshold: 0,
	}

	moved, err := testManager.Rebalance(ctx, spreadCfg)
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if moved == 0 {
		t.Fatal("spread should have moved at least one object")
	}

	// Verify utilization is more balanced
	m1Used := queryQuotaUsed(t, "minio-1")
	m2Used := queryQuotaUsed(t, "minio-2")
	m1Ratio := float64(m1Used) / 1024.0
	m2Ratio := float64(m2Used) / 2048.0

	spread := m1Ratio - m2Ratio
	if spread < 0 {
		spread = -spread
	}

	t.Logf("spread moved %d objects, minio-1=%.1f%% minio-2=%.1f%% spread=%.3f",
		moved, m1Ratio*100, m2Ratio*100, spread)

	// Target ratio is 32.5%. Best achievable with 200-byte objects:
	// 2 on minio-1 (400/1024=39.1%), 3 on minio-2 (600/2048=29.3%), spread=0.098
	// Should NOT overshoot to 1 on minio-1 (19.5%), 4 on minio-2 (39.1%), spread=0.195
	if spread > 0.15 {
		t.Errorf("utilization spread = %.3f, want < 0.15 (should not overshoot)", spread)
	}

	// Verify total bytes are conserved
	if m1Used+m2Used != 1000 {
		t.Errorf("total bytes_used = %d, want 1000", m1Used+m2Used)
	}

	// Verify all objects are still accessible
	list, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(virtualBucket),
		Prefix: aws.String("rebal-spread/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(list.Contents) != 5 {
		t.Errorf("listed %d objects, want 5", len(list.Contents))
	}
}

func TestRebalanceSpreadAlreadyBalanced(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// Put proportional data: minio-1 gets ~33% full, minio-2 gets ~33% full.
	// minio-1 limit=1024, target=333. minio-2 limit=2048, target=667.
	// Put 300 on minio-1 (29.3%), then fill minio-1 and overflow 600 to minio-2 (29.3%).
	// Both near target → spread should do nothing.

	// 300 bytes on minio-1
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(uniqueKey(t, "bal")),
		Body:          bytes.NewReader(bytes.Repeat([]byte("A"), 300)),
		ContentLength: aws.Int64(300),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Fill minio-1 to force overflow
	fillKey := uniqueKey(t, "bal-fill")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(fillKey),
		Body:          bytes.NewReader(bytes.Repeat([]byte("F"), 724)),
		ContentLength: aws.Int64(724),
	})
	if err != nil {
		t.Fatalf("PutObject fill: %v", err)
	}

	// Overflow 600 to minio-2 (minio-1 has 0 bytes free now)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(uniqueKey(t, "bal-m2")),
		Body:          bytes.NewReader(bytes.Repeat([]byte("B"), 600)),
		ContentLength: aws.Int64(600),
	})
	if err != nil {
		t.Fatalf("PutObject m2: %v", err)
	}

	// Delete fill to get minio-1 back to 300
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(fillKey),
	})

	// State: minio-1=300/1024 (29.3%), minio-2=600/2048 (29.3%)
	// Target = 900/3072 = 29.3%. Both at target already.
	m1Used := queryQuotaUsed(t, "minio-1")
	m2Used := queryQuotaUsed(t, "minio-2")
	t.Logf("state: minio-1=%d (%.1f%%) minio-2=%d (%.1f%%)",
		m1Used, float64(m1Used)/1024*100, m2Used, float64(m2Used)/2048*100)

	spreadCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "spread",
		BatchSize: 10,
		Threshold: 0,
	}

	moved, err := testManager.Rebalance(ctx, spreadCfg)
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	t.Logf("moved %d objects", moved)
	if moved != 0 {
		t.Errorf("spread moved %d, want 0 (already balanced)", moved)
	}
}

func TestRebalanceSpreadOversizedObject(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// Put one large object (800 bytes) on minio-1 and a small one (200 bytes).
	// Target = 1000/3072 = 32.5%. minio-1 excess = 1000 - 333 = 667 bytes.
	// The 800-byte object is larger than the 667-byte excess, so spread should
	// only move the 200-byte object.
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(uniqueKey(t, "big")),
		Body:          bytes.NewReader(bytes.Repeat([]byte("B"), 800)),
		ContentLength: aws.Int64(800),
	})
	if err != nil {
		t.Fatalf("PutObject big: %v", err)
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(uniqueKey(t, "small")),
		Body:          bytes.NewReader(bytes.Repeat([]byte("S"), 200)),
		ContentLength: aws.Int64(200),
	})
	if err != nil {
		t.Fatalf("PutObject small: %v", err)
	}

	// State: minio-1=1000/1024 (97.6%), minio-2=0/2048 (0%)
	spreadCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "spread",
		BatchSize: 10,
		Threshold: 0,
	}

	moved, err := testManager.Rebalance(ctx, spreadCfg)
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}

	m1Used := queryQuotaUsed(t, "minio-1")
	m2Used := queryQuotaUsed(t, "minio-2")
	t.Logf("moved %d, minio-1=%d (%.1f%%) minio-2=%d (%.1f%%)",
		moved, m1Used, float64(m1Used)/1024*100, m2Used, float64(m2Used)/2048*100)

	// Only the 200-byte object should move (800 > excess of 667)
	if moved != 1 {
		t.Errorf("moved = %d, want 1 (only small object fits excess)", moved)
	}
	if m1Used != 800 {
		t.Errorf("minio-1 = %d, want 800 (big object stays)", m1Used)
	}
	if m2Used != 200 {
		t.Errorf("minio-2 = %d, want 200 (small object moved)", m2Used)
	}
}

func TestRebalanceSpreadStableAcrossCycles(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// Setup: 5 x 200-byte objects on minio-1
	for i := 0; i < 5; i++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(fmt.Sprintf("stable/obj-%d-%d", i, time.Now().UnixNano())),
			Body:          bytes.NewReader(bytes.Repeat([]byte("S"), 200)),
			ContentLength: aws.Int64(200),
		})
		if err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
	}

	spreadCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "spread",
		BatchSize: 10,
		Threshold: 0,
	}

	// Cycle 1
	moved1, err := testManager.Rebalance(ctx, spreadCfg)
	if err != nil {
		t.Fatalf("Cycle 1: %v", err)
	}
	m1After1 := queryQuotaUsed(t, "minio-1")
	m2After1 := queryQuotaUsed(t, "minio-2")
	t.Logf("cycle 1: moved %d, minio-1=%d minio-2=%d", moved1, m1After1, m2After1)

	// Cycle 2 — should be a no-op, nothing bounces
	moved2, err := testManager.Rebalance(ctx, spreadCfg)
	if err != nil {
		t.Fatalf("Cycle 2: %v", err)
	}
	m1After2 := queryQuotaUsed(t, "minio-1")
	m2After2 := queryQuotaUsed(t, "minio-2")
	t.Logf("cycle 2: moved %d, minio-1=%d minio-2=%d", moved2, m1After2, m2After2)

	if moved2 != 0 {
		t.Errorf("cycle 2 moved %d objects, want 0 (should be stable)", moved2)
	}
	if m1After2 != m1After1 || m2After2 != m2After1 {
		t.Errorf("state changed between cycles: before=(%d,%d) after=(%d,%d)",
			m1After1, m2After1, m1After2, m2After2)
	}
}

func TestRebalanceSpreadBatchLimited(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// 5 x 100-byte objects on minio-1. With batch_size=2, it takes
	// multiple cycles. No object should move twice.
	for i := 0; i < 5; i++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(fmt.Sprintf("batch/obj-%d-%d", i, time.Now().UnixNano())),
			Body:          bytes.NewReader(bytes.Repeat([]byte("B"), 100)),
			ContentLength: aws.Int64(100),
		})
		if err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
	}

	// State: minio-1=500/1024 (48.8%), minio-2=0/2048 (0%)
	// Target = 500/3072 = 16.3%. minio-1 target = 167, excess = 333.
	// With 100-byte objects, can move 3 (300 <= 333). batch_size=2 limits to 2 per cycle.
	smallBatchCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "spread",
		BatchSize: 2,
		Threshold: 0,
	}

	// Cycle 1: moves 2
	moved1, err := testManager.Rebalance(ctx, smallBatchCfg)
	if err != nil {
		t.Fatalf("Cycle 1: %v", err)
	}
	m1After1 := queryQuotaUsed(t, "minio-1")
	m2After1 := queryQuotaUsed(t, "minio-2")
	t.Logf("cycle 1: moved %d, minio-1=%d minio-2=%d", moved1, m1After1, m2After1)

	if moved1 != 2 {
		t.Errorf("cycle 1 moved %d, want 2 (batch limited)", moved1)
	}

	// Cycle 2: moves remaining needed
	moved2, err := testManager.Rebalance(ctx, smallBatchCfg)
	if err != nil {
		t.Fatalf("Cycle 2: %v", err)
	}
	m1After2 := queryQuotaUsed(t, "minio-1")
	m2After2 := queryQuotaUsed(t, "minio-2")
	t.Logf("cycle 2: moved %d, minio-1=%d minio-2=%d", moved2, m1After2, m2After2)

	// Total bytes always conserved
	if m1After2+m2After2 != 500 {
		t.Errorf("total bytes = %d, want 500", m1After2+m2After2)
	}

	// Cycle 3: should stabilize
	moved3, err := testManager.Rebalance(ctx, smallBatchCfg)
	if err != nil {
		t.Fatalf("Cycle 3: %v", err)
	}
	t.Logf("cycle 3: moved %d, minio-1=%d minio-2=%d",
		moved3, queryQuotaUsed(t, "minio-1"), queryQuotaUsed(t, "minio-2"))

	// Eventually no more moves
	totalMoved := moved1 + moved2 + moved3
	t.Logf("total moved across 3 cycles: %d", totalMoved)

	// Should not have moved more objects than exist (no bouncing)
	if totalMoved > 5 {
		t.Errorf("total moved = %d, want <= 5 (objects should not bounce)", totalMoved)
	}
}

func TestRebalanceThresholdSkip(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// PUT a small object on each backend to create roughly balanced usage
	key1 := uniqueKey(t, "threshold")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key1),
		Body:          bytes.NewReader(bytes.Repeat([]byte("T"), 100)),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("PutObject 1: %v", err)
	}

	// Fill minio-1 so it overflows to minio-2
	fillKey := uniqueKey(t, "threshold-fill")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(fillKey),
		Body:          bytes.NewReader(bytes.Repeat([]byte("F"), 1000)),
		ContentLength: aws.Int64(1000),
	})
	if err != nil {
		t.Fatalf("PutObject fill: %v", err)
	}

	// Put another small object that lands on minio-2
	key2 := uniqueKey(t, "threshold2")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key2),
		Body:          bytes.NewReader(bytes.Repeat([]byte("U"), 200)),
		ContentLength: aws.Int64(200),
	})
	if err != nil {
		t.Fatalf("PutObject 2: %v", err)
	}

	// With a high threshold, rebalance should skip
	skipCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "pack",
		BatchSize: 10,
		Threshold: 0.99, // extremely high threshold
	}

	moved, err := testManager.Rebalance(ctx, skipCfg)
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if moved != 0 {
		t.Errorf("expected 0 moves with high threshold, got %d", moved)
	}
}

// -------------------------------------------------------------------------
// REPLICATION
// -------------------------------------------------------------------------

func TestReplicationBasic(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	key := uniqueKey(t, "repl-basic")
	body := bytes.Repeat([]byte("R"), 100)

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Should have 1 copy initially
	if copies := queryObjectCopies(t, key); copies != 1 {
		t.Fatalf("expected 1 copy, got %d", copies)
	}

	// Run replication with factor=2
	replCfg := config.ReplicationConfig{
		Factor:         2,
		WorkerInterval: time.Minute,
		BatchSize:      50,
	}
	created, err := testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if created != 1 {
		t.Errorf("created = %d, want 1", created)
	}

	// Should have 2 copies on different backends
	if copies := queryObjectCopies(t, key); copies != 2 {
		t.Errorf("expected 2 copies, got %d", copies)
	}
	backends := queryObjectBackends(t, key)
	if len(backends) != 2 || backends[0] == backends[1] {
		t.Errorf("expected 2 different backends, got %v", backends)
	}

	// GET still works
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject after replication: %v", err)
	}
	defer resp.Body.Close()

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, body) {
		t.Errorf("body mismatch after replication")
	}
}

func TestReplicationOverwrite(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	key := uniqueKey(t, "repl-overwrite")
	body1 := bytes.Repeat([]byte("A"), 100)
	body2 := bytes.Repeat([]byte("B"), 150)

	// PUT original
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body1),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("PutObject v1: %v", err)
	}

	// Replicate to 2 copies
	replCfg := config.ReplicationConfig{
		Factor:         2,
		WorkerInterval: time.Minute,
		BatchSize:      50,
	}
	_, err = testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate v1: %v", err)
	}
	if copies := queryObjectCopies(t, key); copies != 2 {
		t.Fatalf("expected 2 copies after first replication, got %d", copies)
	}

	// Overwrite with new content
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body2),
		ContentLength: aws.Int64(150),
	})
	if err != nil {
		t.Fatalf("PutObject v2: %v", err)
	}

	// Old replicas should be gone, only 1 new copy
	if copies := queryObjectCopies(t, key); copies != 1 {
		t.Errorf("expected 1 copy after overwrite, got %d", copies)
	}

	// Replicate again
	created, err := testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate v2: %v", err)
	}
	if created != 1 {
		t.Errorf("created = %d, want 1", created)
	}

	// Verify new content on GET
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer resp.Body.Close()

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, body2) {
		t.Errorf("body mismatch: got %d bytes of %q, want %d bytes of %q",
			len(got), got[:1], len(body2), body2[:1])
	}
}

func TestReplicationDelete(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	key := uniqueKey(t, "repl-delete")
	body := bytes.Repeat([]byte("D"), 100)

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Replicate to 2 copies
	replCfg := config.ReplicationConfig{
		Factor:         2,
		WorkerInterval: time.Minute,
		BatchSize:      50,
	}
	_, err = testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	backends := queryObjectBackends(t, key)
	if len(backends) != 2 {
		t.Fatalf("expected 2 backends, got %v", backends)
	}

	// Record quota before delete
	m1Before := queryQuotaUsed(t, "minio-1")
	m2Before := queryQuotaUsed(t, "minio-2")

	// DELETE via proxy
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// All copies should be gone
	if copies := queryObjectCopies(t, key); copies != 0 {
		t.Errorf("expected 0 copies after delete, got %d", copies)
	}

	// Quota should be decremented on both backends
	m1After := queryQuotaUsed(t, "minio-1")
	m2After := queryQuotaUsed(t, "minio-2")
	totalFreed := (m1Before - m1After) + (m2Before - m2After)
	if totalFreed != 200 { // 100 bytes on each backend
		t.Errorf("expected 200 bytes freed total, got %d", totalFreed)
	}
}

func TestReplicationReadFailover(t *testing.T) {
	ctx := context.Background()
	resetState(t)

	key := uniqueKey(t, "repl-failover")
	body := bytes.Repeat([]byte("F"), 100)

	// PUT via manager (not proxy, we need direct backend access below)
	client := newS3Client(t)
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Replicate to 2 copies
	replCfg := config.ReplicationConfig{
		Factor:         2,
		WorkerInterval: time.Minute,
		BatchSize:      50,
	}
	_, err = testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	backends := queryObjectBackends(t, key)
	if len(backends) != 2 {
		t.Fatalf("expected 2 backends, got %v", backends)
	}

	// Delete the primary copy directly from its MinIO backend (bypass proxy)
	primaryBackend := backends[0]
	deleteDirectFromMinio(t, primaryBackend, key)

	// GET via proxy should still succeed (failover to replica)
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject should failover to replica: %v", err)
	}
	defer resp.Body.Close()

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, body) {
		t.Errorf("body mismatch after failover")
	}
}

func TestReplicationAlreadyReplicated(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	key := uniqueKey(t, "repl-noop")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(bytes.Repeat([]byte("N"), 100)),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	replCfg := config.ReplicationConfig{
		Factor:         2,
		WorkerInterval: time.Minute,
		BatchSize:      50,
	}

	// First replication
	created1, err := testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate 1: %v", err)
	}
	if created1 != 1 {
		t.Errorf("first replicate created = %d, want 1", created1)
	}

	// Second replication — should be a no-op
	created2, err := testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate 2: %v", err)
	}
	if created2 != 0 {
		t.Errorf("second replicate created = %d, want 0", created2)
	}
}

func TestReplicationNoSpace(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// Fill both backends to capacity
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(uniqueKey(t, "fill1")),
		Body:          bytes.NewReader(bytes.Repeat([]byte("A"), 1024)),
		ContentLength: aws.Int64(1024),
	})
	if err != nil {
		t.Fatalf("fill minio-1: %v", err)
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(uniqueKey(t, "fill2")),
		Body:          bytes.NewReader(bytes.Repeat([]byte("B"), 2048)),
		ContentLength: aws.Int64(2048),
	})
	if err != nil {
		t.Fatalf("fill minio-2: %v", err)
	}

	// Each object has 1 copy, factor=2 means they need replicas,
	// but the other backend is full — graceful degradation
	replCfg := config.ReplicationConfig{
		Factor:         2,
		WorkerInterval: time.Minute,
		BatchSize:      50,
	}
	created, err := testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if created != 0 {
		t.Errorf("created = %d, want 0 (no space for replicas)", created)
	}
}

func TestRebalancerWithReplicas(t *testing.T) {
	client := newS3Client(t)
	ctx := context.Background()
	resetState(t)

	// Put an object and replicate it to 2 copies
	key := uniqueKey(t, "rebal-repl")
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(virtualBucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(bytes.Repeat([]byte("X"), 100)),
		ContentLength: aws.Int64(100),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	replCfg := config.ReplicationConfig{
		Factor:         2,
		WorkerInterval: time.Minute,
		BatchSize:      50,
	}
	_, err = testManager.Replicate(ctx, replCfg)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	backends := queryObjectBackends(t, key)
	if len(backends) != 2 {
		t.Fatalf("expected 2 copies, got %v", backends)
	}

	// Run rebalancer — it should not place 2 copies on the same backend
	rebalCfg := config.RebalanceConfig{
		Enabled:   true,
		Strategy:  "spread",
		BatchSize: 10,
		Threshold: 0,
	}
	_, err = testManager.Rebalance(ctx, rebalCfg)
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}

	// Verify copies are still on different backends
	backendsAfter := queryObjectBackends(t, key)
	if len(backendsAfter) < 1 {
		t.Fatal("object lost all copies")
	}
	seen := make(map[string]bool)
	for _, b := range backendsAfter {
		if seen[b] {
			t.Errorf("duplicate backend %q — rebalancer placed 2 copies on same backend", b)
		}
		seen[b] = true
	}

	// Object still accessible
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(virtualBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject after rebalance: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

// -------------------------------------------------------------------------
// IMPORT (Sync)
// -------------------------------------------------------------------------

func TestImportPreExistingObjects(t *testing.T) {
	ctx := context.Background()

	t.Run("ImportAndAccessViaProxy", func(t *testing.T) {
		resetState(t)

		// Upload objects directly to MinIO (bypassing the proxy).
		// These exist in S3 but are invisible to the proxy's metadata DB.
		directClient := newDirectMinioClient(t, "minio-1")
		keys := make([]string, 3)
		for i := range keys {
			keys[i] = fmt.Sprintf("import-test/obj-%d-%d", i, time.Now().UnixNano())
			_, err := directClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:        aws.String("backend1"),
				Key:           aws.String(keys[i]),
				Body:          bytes.NewReader(bytes.Repeat([]byte("I"), 100)),
				ContentLength: aws.Int64(100),
			})
			if err != nil {
				t.Fatalf("direct PutObject(%s): %v", keys[i], err)
			}
		}

		// Verify they are NOT accessible via the proxy (no DB record)
		proxyClient := newS3Client(t)
		for _, key := range keys {
			_, err := proxyClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(virtualBucket),
				Key:    aws.String(key),
			})
			if err == nil {
				t.Fatalf("expected 404 for %q before import, got nil", key)
			}
			assertHTTPStatus(t, err, 404)
		}

		// Import each object
		store := testStore
		for _, key := range keys {
			imported, err := store.ImportObject(ctx, key, "minio-1", 100)
			if err != nil {
				t.Fatalf("ImportObject(%q): %v", key, err)
			}
			if !imported {
				t.Errorf("ImportObject(%q) = false, want true", key)
			}
		}

		// Verify quota was updated
		if used := queryQuotaUsed(t, "minio-1"); used != 300 {
			t.Errorf("minio-1 bytes_used = %d, want 300", used)
		}

		// Verify imported objects are now accessible via the proxy
		for _, key := range keys {
			resp, err := proxyClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(virtualBucket),
				Key:    aws.String(key),
			})
			if err != nil {
				t.Fatalf("GetObject(%q) after import: %v", key, err)
			}
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if len(got) != 100 {
				t.Errorf("GetObject(%q) body = %d bytes, want 100", key, len(got))
			}
		}

		// Verify they appear in ListObjectsV2
		list, err := proxyClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(virtualBucket),
			Prefix: aws.String("import-test/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}
		if len(list.Contents) != 3 {
			t.Errorf("listed %d objects, want 3", len(list.Contents))
		}
	})

	t.Run("ImportIdempotent", func(t *testing.T) {
		resetState(t)

		// Put an object directly on MinIO
		directClient := newDirectMinioClient(t, "minio-1")
		key := fmt.Sprintf("import-idem/%d", time.Now().UnixNano())
		_, err := directClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String("backend1"),
			Key:           aws.String(key),
			Body:          bytes.NewReader(bytes.Repeat([]byte("D"), 200)),
			ContentLength: aws.Int64(200),
		})
		if err != nil {
			t.Fatalf("direct PutObject: %v", err)
		}

		store := testStore

		// First import
		imported, err := store.ImportObject(ctx, key, "minio-1", 200)
		if err != nil {
			t.Fatalf("ImportObject first: %v", err)
		}
		if !imported {
			t.Error("first ImportObject = false, want true")
		}

		// Second import of the same key/backend — should be a no-op
		imported, err = store.ImportObject(ctx, key, "minio-1", 200)
		if err != nil {
			t.Fatalf("ImportObject second: %v", err)
		}
		if imported {
			t.Error("second ImportObject = true, want false (idempotent skip)")
		}

		// Quota should only count the object once
		if used := queryQuotaUsed(t, "minio-1"); used != 200 {
			t.Errorf("minio-1 bytes_used = %d, want 200 (not double-counted)", used)
		}

		// Only 1 row in object_locations
		if copies := queryObjectCopies(t, key); copies != 1 {
			t.Errorf("object copies = %d, want 1", copies)
		}
	})

	t.Run("ImportDoesNotOverwriteProxyObject", func(t *testing.T) {
		resetState(t)

		// PUT an object through the proxy (creates DB record)
		proxyClient := newS3Client(t)
		key := uniqueKey(t, "import-existing")
		body := bytes.Repeat([]byte("P"), 150)
		_, err := proxyClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(150),
		})
		if err != nil {
			t.Fatalf("PutObject via proxy: %v", err)
		}

		backend := queryObjectBackend(t, key)

		// Try to import the same key — should skip
		store := testStore
		imported, err := store.ImportObject(ctx, key, backend, 150)
		if err != nil {
			t.Fatalf("ImportObject: %v", err)
		}
		if imported {
			t.Error("ImportObject should skip existing proxy object")
		}

		// Quota unchanged
		if used := queryQuotaUsed(t, backend); used != 150 {
			t.Errorf("%s bytes_used = %d, want 150", backend, used)
		}
	})
}

func TestListObjectsFromBackend(t *testing.T) {
	ctx := context.Background()
	resetState(t)

	// Put objects directly on MinIO
	directClient := newDirectMinioClient(t, "minio-1")
	prefix := fmt.Sprintf("list-backend/%d/", time.Now().UnixNano())
	keys := []string{prefix + "aaa", prefix + "bbb", prefix + "ccc"}

	for _, key := range keys {
		_, err := directClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String("backend1"),
			Key:           aws.String(key),
			Body:          bytes.NewReader(bytes.Repeat([]byte("L"), 50)),
			ContentLength: aws.Int64(50),
		})
		if err != nil {
			t.Fatalf("direct PutObject(%s): %v", key, err)
		}
	}

	// Use S3Backend.ListObjects to scan the bucket
	backend, err := storage.NewS3Backend(config.BackendConfig{
		Name:            "minio-1",
		Endpoint:        envOrDefault("MINIO1_ENDPOINT", "http://localhost:19000"),
		Region:          "us-east-1",
		Bucket:          "backend1",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatalf("NewS3Backend: %v", err)
	}

	var listed []storage.ListedObject
	err = backend.ListObjects(ctx, prefix, func(page []storage.ListedObject) error {
		listed = append(listed, page...)
		return nil
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}

	if len(listed) != 3 {
		t.Errorf("ListObjects returned %d objects, want 3", len(listed))
	}

	found := make(map[string]bool)
	for _, obj := range listed {
		found[obj.Key] = true
		if obj.SizeBytes != 50 {
			t.Errorf("object %q size = %d, want 50", obj.Key, obj.SizeBytes)
		}
	}
	for _, key := range keys {
		if !found[key] {
			t.Errorf("missing key %q in ListObjects results", key)
		}
	}
}

// -------------------------------------------------------------------------
// HELPERS
// -------------------------------------------------------------------------

// newDirectMinioClient creates an S3 client pointed directly at a MinIO backend,
// bypassing the proxy. Used for placing objects that the proxy doesn't know about.
func newDirectMinioClient(t *testing.T, backendName string) *s3.Client {
	t.Helper()

	endpoints := map[string]string{
		"minio-1": envOrDefault("MINIO1_ENDPOINT", "http://localhost:19000"),
		"minio-2": envOrDefault("MINIO2_ENDPOINT", "http://localhost:19002"),
	}

	endpoint, ok := endpoints[backendName]
	if !ok {
		t.Fatalf("unknown backend %q", backendName)
	}

	return s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		UsePathStyle: true,
	})
}

// deleteDirectFromMinio deletes an object directly from a MinIO backend,
// bypassing the proxy. Used to simulate a backend failure for failover tests.
func deleteDirectFromMinio(t *testing.T, backendName, key string) {
	t.Helper()

	buckets := map[string]string{
		"minio-1": "backend1",
		"minio-2": "backend2",
	}

	directClient := newDirectMinioClient(t, backendName)
	_, err := directClient.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(buckets[backendName]),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("direct delete from %s: %v", backendName, err)
	}
}

// assertHTTPStatus checks that the error contains the expected HTTP status code.
func assertHTTPStatus(t *testing.T, err error, wantStatus int) {
	t.Helper()
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		if respErr.HTTPStatusCode() != wantStatus {
			t.Errorf("HTTP status = %d, want %d", respErr.HTTPStatusCode(), wantStatus)
		}
		return
	}
	// If we can't extract the HTTP status, just note the error type
	t.Logf("could not extract HTTP status from error (type %T): %v", err, err)
}

// -------------------------------------------------------------------------
// CIRCUIT BREAKER DEGRADED MODE
// -------------------------------------------------------------------------

func TestCircuitBreakerDegradedMode(t *testing.T) {
	t.Run("ReadsDuringOutage", func(t *testing.T) {
		resetState(t)
		client := newS3Client(t)
		ctx := context.Background()

		// PUT an object while DB is healthy
		key := uniqueKey(t, "cb-read")
		body := bytes.Repeat([]byte("R"), 100)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(100),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}

		// Toggle FailableStore to failing
		testFailableStore.SetFailing(true)
		defer testFailableStore.SetFailing(false)

		// Trip the circuit breaker
		tripCircuitBreaker(t)

		// Verify circuit is open
		if testCBStore.IsHealthy() {
			t.Fatal("expected circuit to be open")
		}

		// GET should succeed via broadcast (object exists on backend)
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject during outage should succeed via broadcast: %v", err)
		}
		defer resp.Body.Close()

		got, _ := io.ReadAll(resp.Body)
		if !bytes.Equal(got, body) {
			t.Errorf("body mismatch: got %d bytes, want %d", len(got), len(body))
		}

		// HEAD should also succeed via broadcast
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("HeadObject during outage should succeed via broadcast: %v", err)
		}

		// Restore DB and wait for recovery
		testFailableStore.SetFailing(false)
		waitForRecovery(t)

		if !testCBStore.IsHealthy() {
			t.Error("expected circuit to be closed after recovery")
		}

		// Normal GET should work again
		resp2, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject after recovery: %v", err)
		}
		resp2.Body.Close()
	})

	t.Run("WritesRejectedDuringOutage", func(t *testing.T) {
		resetState(t)
		client := newS3Client(t)
		ctx := context.Background()

		// Toggle FailableStore to failing and trip circuit
		testFailableStore.SetFailing(true)
		defer testFailableStore.SetFailing(false)

		tripCircuitBreaker(t)

		if testCBStore.IsHealthy() {
			t.Fatal("expected circuit to be open")
		}

		// PUT should return 503
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(uniqueKey(t, "cb-write")),
			Body:          bytes.NewReader([]byte("x")),
			ContentLength: aws.Int64(1),
		})
		if err == nil {
			t.Fatal("PutObject should fail during outage")
		}
		assertHTTPStatus(t, err, 503)

		// DELETE should return 503
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String("any-key"),
		})
		if err == nil {
			t.Fatal("DeleteObject should fail during outage")
		}
		assertHTTPStatus(t, err, 503)

		// LIST should return 503
		_, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(virtualBucket),
		})
		if err == nil {
			t.Fatal("ListObjectsV2 should fail during outage")
		}
		assertHTTPStatus(t, err, 503)

		// Restore and verify writes work again
		testFailableStore.SetFailing(false)
		waitForRecovery(t)

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(virtualBucket),
			Key:           aws.String(uniqueKey(t, "cb-write-after")),
			Body:          bytes.NewReader([]byte("y")),
			ContentLength: aws.Int64(1),
		})
		if err != nil {
			t.Fatalf("PutObject after recovery should succeed: %v", err)
		}
	})
}
