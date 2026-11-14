package bugs

import (
	"context"
	"path/filepath"
	"testing"

	oshodi "github.com/agberohq/oshodi"
)

// db.Scan(ctx, nil, fn) panicked with a nil-pointer dereference although the
// engine layer it delegates to handles nil options.
func TestScanNilOptions(t *testing.T) {
	db, err := oshodi.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Scan(context.Background(), nil, func(key, value []byte) error {
		return nil
	}); err != nil {
		t.Fatalf("Scan with nil options: %v", err)
	}
}
