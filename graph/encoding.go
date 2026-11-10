package graph

import (
	"encoding/binary"
	"encoding/json"
)

// Record envelopes are versioned and length-prefixed: identity is stored in
// the record key (vertex ID / edge ID), metadata and properties in the
// value. IDs and labels containing \x00 are unambiguous by construction.

func appendField(dst []byte, field []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(field)))
	return append(dst, field...)
}

func encodeVertexRecord(label string, props []byte) []byte {
	out := []byte{recMagic, recVersion}
	out = appendField(out, []byte(label))
	out = appendField(out, props)
	return out
}

func decodeVertexRecord(b []byte) (label string, props []byte, err error) {
	if len(b) < 2 || b[0] != recMagic || b[1] != recVersion {
		return "", nil, errForeignRecord
	}
	pos := 2
	var used int
	label, used, err = takeField(b, pos)
	if err != nil {
		return "", nil, err
	}
	pos += used
	props, _, err = takeFieldRest(b, pos)
	if err != nil {
		return "", nil, err
	}
	return label, props, nil
}

func encodeEdgeRecord(src, dst, label string, props []byte) []byte {
	out := []byte{recMagic, recVersion}
	for _, field := range []string{src, dst, label} {
		out = appendField(out, []byte(field))
	}
	out = appendField(out, props)
	return out
}

func decodeEdgeRecord(b []byte) (src, dst, label string, props []byte, err error) {
	if len(b) < 2 || b[0] != recMagic || b[1] != recVersion {
		return "", "", "", nil, errForeignRecord
	}
	pos := 2
	var used int
	if src, used, err = takeField(b, pos); err != nil {
		return
	}
	pos += used
	if dst, used, err = takeField(b, pos); err != nil {
		return
	}
	pos += used
	if label, used, err = takeField(b, pos); err != nil {
		return
	}
	pos += used
	props, _, err = takeFieldRest(b, pos)
	if err != nil {
		return
	}
	return src, dst, label, props, nil
}

// takeField reads one length-prefixed field starting at pos and returns the
// field content and total consumed bytes.
func takeField(b []byte, pos int) (string, int, error) {
	if pos >= len(b) {
		return "", 0, errForeignRecord
	}
	length, used := binary.Uvarint(b[pos:])
	if used <= 0 || pos+used+int(length) > len(b) {
		return "", 0, errForeignRecord
	}
	start := pos + used
	return string(b[start : start+int(length)]), used + int(length), nil
}

// takeFieldRest reads the final length-prefixed field whose content extends
// to the end of the record (the props blob).
func takeFieldRest(b []byte, pos int) ([]byte, int, error) {
	if pos >= len(b) {
		return nil, 0, errForeignRecord
	}
	length, used := binary.Uvarint(b[pos:])
	if used <= 0 || pos+used+int(length) > len(b) {
		return nil, 0, errForeignRecord
	}
	start := pos + used
	return b[start : start+int(length)], used + int(length), nil
}

func decodeJSONObject(props []byte) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal(props, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
