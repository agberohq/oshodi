package vector

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Vector is a float32 embedding.
type Vector = []float32

// encodeVectorRecord serializes a vector embedding to bytes with version framing.
func encodeVectorRecord(vec Vector) []byte {
	out := make([]byte, 0, 2+4+len(vec)*4)
	out = append(out, recMagic, recVersion)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(vec)))
	for _, v := range vec {
		out = binary.LittleEndian.AppendUint32(out, math.Float32bits(v))
	}
	return out
}

// decodeVectorRecord deserializes a vector embedding from bytes.
func decodeVectorRecord(b []byte) (Vector, error) {
	if len(b) < 6 || b[0] != recMagic || b[1] != recVersion {
		return nil, fmt.Errorf("vector: corrupt or foreign record")
	}
	dims := binary.LittleEndian.Uint32(b[2:6])
	// 64-bit arithmetic: a hostile dims value must not overflow on 32-bit.
	if uint64(dims)*4+6 > uint64(len(b)) {
		return nil, fmt.Errorf("vector: truncated record (dims %d exceeds %d bytes)", dims, len(b))
	}
	expected := 6 + int(dims)*4
	if len(b) < expected {
		return nil, fmt.Errorf("vector: truncated record (expected %d bytes, got %d)", expected, len(b))
	}
	vec := make(Vector, dims)
	pos := 6
	for i := range vec {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[pos : pos+4]))
		pos += 4
	}
	return vec, nil
}

// encodeEdgeRecord serializes an edge (neighbor relationship) to bytes.
func encodeEdgeRecord(src, dst string, layer int) []byte {
	out := []byte{recMagic, recVersion}
	out = appendField(out, []byte(src))
	out = appendField(out, []byte(dst))
	out = binary.LittleEndian.AppendUint32(out, uint32(layer))
	return out
}

// decodeEdgeRecord deserializes an edge from bytes.
func decodeEdgeRecord(b []byte) (src, dst string, layer int, err error) {
	if len(b) < 6 || b[0] != recMagic || b[1] != recVersion {
		return "", "", 0, fmt.Errorf("vector: corrupt or foreign edge record")
	}
	pos := 2
	var used int
	src, used, err = takeField(b, pos)
	if err != nil {
		return
	}
	pos += used
	dst, used, err = takeField(b, pos)
	if err != nil {
		return
	}
	pos += used
	if pos+4 > len(b) {
		return "", "", 0, fmt.Errorf("vector: truncated edge record")
	}
	layer = int(binary.LittleEndian.Uint32(b[pos : pos+4]))
	return src, dst, layer, nil
}

func appendField(dst []byte, field []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(field)))
	return append(dst, field...)
}

func takeField(b []byte, pos int) (string, int, error) {
	if pos >= len(b) {
		return "", 0, fmt.Errorf("vector: truncated field")
	}
	length, used := binary.Uvarint(b[pos:])
	// A corrupt varint near 2^64 would wrap to a negative int here.
	if used <= 0 || length > uint64(len(b)) || pos+used+int(length) > len(b) {
		return "", 0, fmt.Errorf("vector: truncated field")
	}
	start := pos + used
	return string(b[start : start+int(length)]), used + int(length), nil
}
