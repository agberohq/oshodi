package bugs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agberohq/oshodi"
)

// Symptom: after a restart the catalogue read 0 songs even though the database
// file was tens of MiB. Root cause: a run preallocated the file and exited
// before its first write, so records were appended behind a zero prefix.
// NewDB skipped loadIndex because IsEmpty() only inspected offset 0, and every
// reopen repeated that decision. Recovery must index the records behind the
// prefix without rewriting the file.
func TestCatalogueSurvivesPreallocatedZeroPrefix(t *testing.T) {
	dir := t.TempDir()

	src := filepath.Join(dir, "src.db")
	db, err := oshodi.Open(src)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	b := mustBucket(t, db, "songs")
	for k, v := range map[string]string{
		"testing":         `{"title":"Testing"}`,
		"june-express-v2": `{"title":"June Express V2"}`,
	} {
		if err := b.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	const prefix = 2 << 20
	target := filepath.Join(dir, "muca.db")
	if err := os.WriteFile(target, append(make([]byte, prefix), raw...), 0o600); err != nil {
		t.Fatalf("write damaged db: %v", err)
	}

	db, err = oshodi.Open(target)
	if err != nil {
		t.Fatalf("open damaged: %v", err)
	}
	defer db.Close()

	b = mustBucket(t, db, "songs")
	if v, err := b.Get([]byte("testing")); err != nil || len(v) == 0 {
		t.Fatalf("get testing: %v %q", err, v)
	}
	got := 0
	if err := b.Scan(func(k, v []byte) bool { got++; return true }); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != 2 {
		t.Fatalf("scan returned %d records, want 2", got)
	}

	// The recovered database must keep working across another restart.
	if err := b.Set([]byte("later"), []byte(`{"title":"Later"}`)); err != nil {
		t.Fatalf("set after recovery: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, err = oshodi.Open(target)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if got := count(mustBucket(t, db, "songs")); got != 3 {
		t.Fatalf("records after second restart = %d, want 3", got)
	}
}

func count(b *oshodi.Bucket) int {
	n := 0
	_ = b.Scan(func(k, v []byte) bool { n++; return true })
	return n
}
