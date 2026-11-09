package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecoverInterruptedCompaction(t *testing.T) {
	t.Run("compacted file wins", func(t *testing.T) {
		dir := t.TempDir()
		main := filepath.Join(dir, "db")
		if err := os.WriteFile(main+FilenameSuffixCompact, []byte("new"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(main+FilenameSuffixBackup, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		recovered, err := RecoverInterruptedCompaction(main)
		if err != nil || !recovered {
			t.Fatalf("recover = %v, %v", recovered, err)
		}
		assertFile(t, main, "new")
		if _, err := os.Stat(main + FilenameSuffixBackup); !os.IsNotExist(err) {
			t.Fatal("backup left behind after completing the compaction")
		}
	})

	t.Run("roll back to backup", func(t *testing.T) {
		dir := t.TempDir()
		main := filepath.Join(dir, "db")
		if err := os.WriteFile(main+FilenameSuffixBackup, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		recovered, err := RecoverInterruptedCompaction(main)
		if err != nil || !recovered {
			t.Fatalf("recover = %v, %v", recovered, err)
		}
		assertFile(t, main, "old")
	})

	t.Run("healthy main untouched", func(t *testing.T) {
		dir := t.TempDir()
		main := filepath.Join(dir, "db")
		if err := os.WriteFile(main, []byte("main"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(main+FilenameSuffixBackup, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		recovered, err := RecoverInterruptedCompaction(main)
		if err != nil || recovered {
			t.Fatalf("recover = %v, %v", recovered, err)
		}
		assertFile(t, main, "main")
	})
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}
