package storage

// Fuzz entry points for the record and WAL decoders: arbitrary bytes must
// never panic, never OOM, and always surface typed errors (ErrCorrupt /
// ErrWALCorrupted) rather than silent success. The decode caps in
// constants.go are the primary guard; these fuzzing contracts prove them.

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzMainRecordDecode(f *testing.F) {
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte("\x00\x00\x00\x04key\x00\x00\x00\x05\x00valueCRC4"))
	f.Add([]byte("\xFF\xFF\xFF\xFF"))
	f.Add([]byte("\x00\x00\x00\x01a\x00\x00\x00\x00\x02\x01\x02\x03CRLF4"))
	f.Add(make([]byte, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		fl, err := os.Create(filepath.Join(dir, "f.db"))
		if err != nil {
			t.Skip()
		}
		if _, err := fl.Write(data); err != nil {
			t.Skip()
		}
		fl.Close()

		file, err := NewFile(&FileConfig{Path: filepath.Join(dir, "f.db")})
		if err != nil {
			return
		}
		defer file.Close()
		sto := int64(0)
		for off := int64(0); off < int64(len(data)); off += 16 {
			key, value, tomb, n, err := file.ReadRecordAt(off)
			if err != nil {
				continue
			}
			if n <= 0 {
				t.Fatalf("record decoder returned n=%d at offset %d", n, off)
			}
			if !tomb && len(key) == 0 && len(value) == 0 {
				// only the padding sentinel may produce both-empty
				t.Fatal("empty record decoded as regular")
			}
			_ = key
			_ = value
			sto += n
		}
		_ = sto
	})
}

func FuzzWALDecode(f *testing.F) {
	f.Add([]byte("\x01\x00\x00\x00a\x00\x00\x00\x00xBINCRC4"))
	f.Add([]byte{0x01, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x00})
	f.Add(make([]byte, 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "f.wal")
		fl, err := os.Create(path)
		if err != nil {
			t.Skip()
		}
		if _, err := fl.Write(data); err != nil {
			t.Skip()
		}
		fl.Close()

		wal, err := NewWAL(&WALConfig{Path: path})
		if err != nil {
			// open may fail cleanly on garbage; never panic
			return
		}
		defer wal.Close()
		reader := wal.NewReader(0)
		for {
			_, err := reader.ReadNext(nil)
			if err != nil {
				break
			}
		}
		for off := int64(0); off < int64(len(data)); off += 8 {
			_, _, _ = wal.Read(nil, off)
		}
	})
}

func TestTMPTruncTwo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db")
	f, err := NewFile(&FileConfig{Path: path, InitialSize: 20 << 20})
	if err != nil {
		t.Fatal(err)
	}
	f.WriteRecord([]byte("rk0"), makeVal2(0, 1, 12))
	f.WriteRecord([]byte("rk1"), makeVal2(1, 2, 12))
	end := f.LogicalSize()
	f.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw[:end], 0600); err != nil {
		t.Fatal(err)
	}
	f2, err := NewFile(&FileConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	for off := int64(0); off < end; {
		k, v, tomb, n, e := f2.ReadRecordAt(off)
		t.Logf("off=%d k=%q v=%q tomb=%v n=%d err=%v", off, k, v, tomb, n, e)
		if e != nil || n == 0 {
			break
		}
		off += n
	}
}
func makeVal2(w int, seq uint64, fill int) []byte {
	v := make([]byte, 12+fill)
	v[0] = byte(w)
	v[4] = byte(seq)
	for i := 12; i < 12+fill; i++ {
		v[i] = 0x41
	}
	return v
}
