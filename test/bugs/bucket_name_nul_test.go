package bugs

import (
	"errors"
	"path/filepath"
	"testing"

	oshodi "github.com/agberohq/oshodi"
)

// A bucket name containing a NUL byte would make its keys collide
// byte-for-byte with another bucket's keys (prefix = name + "\x00"), so it
// must be rejected at Bucket() time.
func TestBucketNameWithNulRejected(t *testing.T) {
	db, err := oshodi.Open(filepath.Join(t.TempDir(), "names.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Bucket("a\x00b"); !errors.Is(err, oshodi.ErrInvalidBucketName) {
		t.Fatalf("expected ErrInvalidBucketName, got %v", err)
	}

	// Ordinary names keep working.
	if _, err := db.Bucket("a"); err != nil {
		t.Fatalf("ordinary bucket name rejected: %v", err)
	}
}
