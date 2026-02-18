// -------------------------------------------------------------------------------
// Manager Tests - Upload ID Generation and String Utilities
//
// Author: Alex Freidah
//
// Unit tests for the backend manager's utility functions including upload ID
// generation uniqueness and length validation.
// -------------------------------------------------------------------------------

package storage

import (
	"testing"
)

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
