package storage

import (
	"testing"
)

func TestIndexOf(t *testing.T) {
	tests := []struct {
		s, substr string
		want      int
	}{
		{"hello/world", "/", 5},
		{"nodelim", "/", -1},
		{"a/b/c", "/", 1},
		{"", "/", -1},
		{"/leading", "/", 0},
		{"photos/2024/img.jpg", "/", 6},
	}

	for _, tt := range tests {
		got := indexOf(tt.s, tt.substr)
		if got != tt.want {
			t.Errorf("indexOf(%q, %q) = %d, want %d", tt.s, tt.substr, got, tt.want)
		}
	}
}

func TestGenerateUploadID(t *testing.T) {
	id := GenerateUploadID()

	// Should be 32 hex chars (16 bytes)
	if len(id) != 32 {
		t.Errorf("GenerateUploadID() length = %d, want 32", len(id))
	}

	// Should be unique
	id2 := GenerateUploadID()
	if id == id2 {
		t.Error("GenerateUploadID() should produce unique IDs")
	}
}
