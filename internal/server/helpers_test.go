// -------------------------------------------------------------------------------
// Helper Tests - Path Parsing and XML Escaping
//
// Author: Alex Freidah
//
// Unit tests for URL path parsing (bucket/key extraction) and XML special
// character escaping used in S3-compatible error responses.
// -------------------------------------------------------------------------------

package server

import (
	"testing"
)

func TestParsePath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantKey    string
		wantOk     bool
	}{
		{
			name:       "bucket and key",
			path:       "/mybucket/mykey",
			wantBucket: "mybucket",
			wantKey:    "mykey",
			wantOk:     true,
		},
		{
			name:       "bucket and nested key",
			path:       "/mybucket/path/to/object.jpg",
			wantBucket: "mybucket",
			wantKey:    "path/to/object.jpg",
			wantOk:     true,
		},
		{
			name:       "bucket only with trailing slash",
			path:       "/mybucket/",
			wantBucket: "mybucket",
			wantKey:    "",
			wantOk:     true,
		},
		{
			name:       "bucket only no trailing slash",
			path:       "/mybucket",
			wantBucket: "mybucket",
			wantKey:    "",
			wantOk:     true,
		},
		{
			name:   "empty path",
			path:   "/",
			wantOk: false,
		},
		{
			name:   "bare empty",
			path:   "",
			wantOk: false,
		},
		{
			name:       "key with spaces",
			path:       "/bucket/my file.txt",
			wantBucket: "bucket",
			wantKey:    "my file.txt",
			wantOk:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, ok := parsePath(tt.path)
			if ok != tt.wantOk {
				t.Errorf("parsePath(%q) ok = %v, want %v", tt.path, ok, tt.wantOk)
			}
			if ok {
				if bucket != tt.wantBucket {
					t.Errorf("parsePath(%q) bucket = %q, want %q", tt.path, bucket, tt.wantBucket)
				}
				if key != tt.wantKey {
					t.Errorf("parsePath(%q) key = %q, want %q", tt.path, key, tt.wantKey)
				}
			}
		})
	}
}

func TestXmlEscape(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"a&b", "a&amp;b"},
		{"<tag>", "&lt;tag&gt;"},
		{`"quoted"`, "&quot;quoted&quot;"},
		{"it's", "it&apos;s"},
		{"a&b<c>d\"e'f", "a&amp;b&lt;c&gt;d&quot;e&apos;f"},
	}

	for _, tt := range tests {
		got := xmlEscape(tt.input)
		if got != tt.want {
			t.Errorf("xmlEscape(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
