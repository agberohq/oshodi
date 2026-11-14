package bugs

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/agberohq/oshodi"
)

// Scan must hand out slices the callback can retain; Scanner must reuse its
// buffers (callback-scoped lifetime) and stay allocation-free per record.
// Optimising Scan by silently reusing buffers would corrupt retained values,
// which is why the two contracts are separate APIs.
func TestScanRetainsSlicesWhileScannerReuses(t *testing.T) {
	db, err := oshodi.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	b := mustBucket(t, db, "b")
	const n = 128
	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%03d", i)
		v := fmt.Sprintf("value-%03d-%s", i, bytes.Repeat([]byte("x"), 32))
		want[k] = v
		if err := b.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	// Scan: retain every key and value, then verify after the scan has ended.
	type kept struct{ k, v []byte }
	var retained []kept
	if err := b.Scan(func(k, v []byte) bool {
		retained = append(retained, kept{k, v})
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(retained) != n {
		t.Fatalf("scan returned %d records, want %d", len(retained), n)
	}
	for _, kv := range retained {
		if got := want[string(kv.k)]; got != string(kv.v) {
			t.Fatalf("retained value for %q = %q, want %q", kv.k, kv.v, got)
		}
	}

	// Scanner: copy inside the callback and verify the result.
	seen := make(map[string]string, n)
	if err := b.Scanner(func(k, v []byte) bool {
		seen[string(k)] = string(v)
		return true
	}); err != nil {
		t.Fatalf("scan view: %v", err)
	}
	for k, v := range want {
		if seen[k] != v {
			t.Fatalf("scan view[%q] = %q, want %q", k, seen[k], v)
		}
	}

	// Buffers are reused, so a scan must not allocate per record.
	allocs := testing.AllocsPerRun(20, func() {
		_ = b.Scanner(func(k, v []byte) bool { return true })
	})
	if !raceEnabled && allocs > 16 {
		t.Fatalf("Scanner allocated %.0f objects per scan of %d records", allocs, n)
	}
}
