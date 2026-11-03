package storage

import (
	"hash/crc32"
	"time"
)

// Disk artifact naming and record-encoding constants.
// Suffixes are appended to the database file path to derive the artifact
// names; they live here so a single edit retargets the whole layout.
const (
	// FilenameSuffixWAL is appended to the database path to name the WAL file.
	FilenameSuffixWAL = ".wal"
	// FilenameSuffixBackup names the pre-compaction backup file.
	FilenameSuffixBackup = ".bak"
	// FilenameSuffixCompact names the temporary file built during compaction.
	FilenameSuffixCompact = ".compact"
	// FilenameSuffixLock names the single-process advisory lock sidecar.
	FilenameSuffixLock = ".lock"

	// DefaultWALFlushInterval is the asynchronous WAL flusher period.
	DefaultWALFlushInterval = 10 * time.Millisecond
	// DefaultWALBufferSize is the WAL flush batch budget.
	DefaultWALBufferSize = 4 << 20 // 4MB

	// WALTombstoneValLen is the value-length sentinel stored in WAL
	// tombstones so deletions can never be confused with a regular
	// record holding an empty value (valLen == 0).
	WALTombstoneValLen uint32 = 0xFFFFFFFF
)

// crc32cTable powers the per-record integrity trailer (RecordChecksum).
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// RecordChecksum returns the Castagnoli CRC over a serialised record's
// payload (everything before the 4-byte trailer itself).
func RecordChecksum(p []byte) uint32 {
	return crc32.Checksum(p, crc32cTable)
}

// Record flag byte (single byte between valLen and value).
//
// Records with valLen > 0 use the flag as the compression marker.
// Zero-length records disambiguate regular empty values from tombstones.
const (
	// RecordFlagEmptyValue marks a zero-length record as a regular record
	// storing an empty user value.
	RecordFlagEmptyValue = 0x01
	// RecordFlagTombstone marks a zero-length record as a delete marker.
	RecordFlagTombstone = 0x02

	// RecordCRCSize is the per-record CRC trailer length.
	RecordCRCSize = 4
)

const (
	// MaxLegalKeyLen caps key length on decode and write so a corrupt
	// length prefix can never cause a multi-gigabyte allocation.
	MaxLegalKeyLen uint32 = 0x00FFFFFF
	// MaxLegalValLen caps value length on decode for the same reason.
	MaxLegalValLen uint32 = 1 << 30
)
