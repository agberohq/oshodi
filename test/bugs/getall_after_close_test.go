package bugs

import (
	"path/filepath"
	"testing"

	oshodi "github.com/agberohq/oshodi"
)

// GetAll and Scan returned empty results with a nil error after Close,
// while every other read path reported ErrClosed — a silent data-loss
// contract for callers exporting data.
func TestGetAllAndScanAfterClose(t *testing.T) {
	db, err := oshodi.Open(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	b := mustBucket(t, db, "b")
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := b.GetAll(); err == nil {
		t.Fatal("GetAll after Close returned nil error")
	}
	if err := b.Scan(func(key, value []byte) bool { return true }); err == nil {
		t.Fatal("Scan after Close returned nil error")
	}
}
