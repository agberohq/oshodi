package storage

import (
	"os"
)

// RecoverInterruptedCompaction repairs the state left by a crash in the middle
// of Compact's two-rename swap.
//
// Compact builds and fsyncs <path>.compact, renames <path> to <path>.bak, then
// renames <path>.compact to <path>. A crash between the renames leaves no main
// file at all; opening it would create a fresh, empty database and the
// catalogue would read as empty even though both copies are intact on disk.
//
// The compacted file is the newer, fully synced snapshot, so it wins when
// present. Otherwise the backup — the untouched pre-compaction database — is
// rolled back into place. A healthy main file is never touched.
func RecoverInterruptedCompaction(path string) (bool, error) {
	main := path
	tmp := path + FilenameSuffixCompact
	backup := path + FilenameSuffixBackup

	if st, err := os.Stat(main); err == nil && st.Size() > 0 {
		return false, nil
	}

	if _, err := os.Stat(tmp); err == nil {
		if err := os.Rename(tmp, main); err != nil {
			return false, err
		}
		_ = os.Remove(backup)
		fsyncDir(main)
		return true, nil
	}

	if _, err := os.Stat(backup); err == nil {
		if err := os.Rename(backup, main); err != nil {
			return false, err
		}
		fsyncDir(main)
		return true, nil
	}

	return false, nil
}
