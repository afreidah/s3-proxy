// -------------------------------------------------------------------------------
// Authentication Tests - SigV4 and Token Verification
//
// Project: Munchbox / Author: Alex Freidah
//
// Unit tests for AWS SigV4 field parsing, canonical query construction, signing
// key derivation, and the authentication dispatch logic for both SigV4 and legacy
// token methods.
// -------------------------------------------------------------------------------

package auth

import (
	"encoding/hex"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/afreidah/s3-proxy/internal/config"
)

func TestParseSigV4Fields(t *testing.T) {
	input := "Credential=AKID/20260215/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcdef1234567890"
	fields := parseSigV4Fields(input)

	if fields["Credential"] != "AKID/20260215/us-east-1/s3/aws4_request" {
		t.Errorf("Credential = %q", fields["Credential"])
	}
	if fields["SignedHeaders"] != "host;x-amz-date" {
		t.Errorf("SignedHeaders = %q", fields["SignedHeaders"])
	}
	if fields["Signature"] != "abcdef1234567890" {
		t.Errorf("Signature = %q", fields["Signature"])
	}
}

func TestBuildCanonicalQueryString(t *testing.T) {
	tests := []struct {
		name   string
		values url.Values
		want   string
	}{
		{
			name:   "empty",
			values: url.Values{},
			want:   "",
		},
		{
			name:   "single param",
			values: url.Values{"prefix": {"photos/"}},
			want:   "prefix=photos%2F",
		},
		{
			name:   "multiple params sorted",
			values: url.Values{"prefix": {"a"}, "delimiter": {"/"}, "max-keys": {"100"}},
			want:   "delimiter=%2F&max-keys=100&prefix=a",
		},
		{
			name:   "spaces encoded as %20 not +",
			values: url.Values{"prefix": {"my photos"}},
			want:   "prefix=my%20photos",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildCanonicalQueryString(tt.values)
			if got != tt.want {
				t.Errorf("buildCanonicalQueryString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDeriveSigningKey(t *testing.T) {
	// AWS test vector from SigV4 documentation
	key := deriveSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20120215", "us-east-1", "iam")
	if len(key) != 32 {
		t.Errorf("signing key length = %d, want 32", len(key))
	}
}

func TestHmacSHA256(t *testing.T) {
	result := hmacSHA256([]byte("key"), []byte("data"))
	if len(result) != 32 {
		t.Errorf("hmacSHA256 result length = %d, want 32", len(result))
	}
}

func TestHashSHA256(t *testing.T) {
	// SHA256 of empty string
	got := hashSHA256([]byte(""))
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got != want {
		t.Errorf("hashSHA256('') = %q, want %q", got, want)
	}
}

func TestNeedsAuth(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.AuthConfig
		want bool
	}{
		{"no auth", config.AuthConfig{}, false},
		{"token only", config.AuthConfig{Token: "secret"}, true},
		{"sigv4 only", config.AuthConfig{AccessKeyID: "AKID", SecretAccessKey: "secret"}, true},
		{"both", config.AuthConfig{Token: "tok", AccessKeyID: "AKID", SecretAccessKey: "secret"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NeedsAuth(tt.cfg); got != tt.want {
				t.Errorf("NeedsAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAuthenticate_NoAuthConfigured(t *testing.T) {
	r, _ := http.NewRequest("GET", "/bucket/key", nil)
	err := Authenticate(r, config.AuthConfig{})
	if err != nil {
		t.Errorf("Authenticate with no auth configured should succeed, got: %v", err)
	}
}

func TestAuthenticate_LegacyToken(t *testing.T) {
	cfg := config.AuthConfig{Token: "my-secret-token"}

	// Valid token
	r, _ := http.NewRequest("GET", "/bucket/key", nil)
	r.Header.Set("X-Proxy-Token", "my-secret-token")
	if err := Authenticate(r, cfg); err != nil {
		t.Errorf("valid token should succeed, got: %v", err)
	}

	// Invalid token
	r2, _ := http.NewRequest("GET", "/bucket/key", nil)
	r2.Header.Set("X-Proxy-Token", "wrong-token")
	if err := Authenticate(r2, cfg); err == nil {
		t.Error("invalid token should fail")
	}

	// Missing token
	r3, _ := http.NewRequest("GET", "/bucket/key", nil)
	if err := Authenticate(r3, cfg); err == nil {
		t.Error("missing token should fail")
	}
}

func TestAuthenticate_SigV4Unconfigured(t *testing.T) {
	// SigV4 header present but no credentials configured
	cfg := config.AuthConfig{}
	r, _ := http.NewRequest("GET", "/bucket/key", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20260215/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc")

	err := Authenticate(r, cfg)
	if err == nil {
		t.Error("SigV4 with no credentials should fail")
	}
}

func TestAuthenticate_BypassWithNoHeader(t *testing.T) {
	// SigV4 configured but request has no Authorization header — must deny
	cfg := config.AuthConfig{AccessKeyID: "AKID", SecretAccessKey: "secret"}
	r, _ := http.NewRequest("GET", "/bucket/key", nil)

	err := Authenticate(r, cfg)
	if err == nil {
		t.Error("request with no auth header should be denied when SigV4 is configured")
	}
}

func TestAuthenticate_TokenConfiguredNoHeader(t *testing.T) {
	// Token configured but request has no X-Proxy-Token or Authorization header — must deny
	cfg := config.AuthConfig{Token: "my-secret"}
	r, _ := http.NewRequest("GET", "/bucket/key", nil)

	err := Authenticate(r, cfg)
	if err == nil {
		t.Error("request with no auth header should be denied when token is configured")
	}
}

func TestVerifySigV4_StaleTimestamp(t *testing.T) {
	// A request signed with a timestamp 30 minutes in the past should be rejected
	staleDate := time.Now().UTC().Add(-30 * time.Minute).Format("20060102T150405Z")
	dateStamp := staleDate[:8]
	secret := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	accessKey := "AKIDEXAMPLE"

	r, _ := http.NewRequest("GET", "/bucket/key", nil)
	r.Header.Set("X-Amz-Date", staleDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	r.Host = "localhost"

	// Build a valid signature so we test the timestamp check, not a sig mismatch
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonicalRequest := buildCanonicalRequest(r, signedHeaders)
	credentialScope := dateStamp + "/us-east-1/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + staleDate + "\n" + credentialScope + "\n" + hashSHA256([]byte(canonicalRequest))
	signingKey := deriveSigningKey(secret, dateStamp, "us-east-1", "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+accessKey+"/"+credentialScope+
			", SignedHeaders=host;x-amz-content-sha256;x-amz-date"+
			", Signature="+signature)

	err := VerifySigV4(r, accessKey, secret)
	if err == nil {
		t.Error("stale timestamp (30m) should be rejected")
	}
}

func TestSigV4Encode(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"hello", "hello"},
		{"hello world", "hello%20world"},
		{"a+b", "a%2Bb"},
		{"a/b", "a%2Fb"},
	}
	for _, tt := range tests {
		got := sigV4Encode(tt.in)
		if got != tt.want {
			t.Errorf("sigV4Encode(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
