package ograph

import "fmt"

// ElementKind identifies what a stream element represents.
type ElementKind uint8

const (
	VertexElement ElementKind = iota
	EdgeElement
	ValueElement
)

// Element is the one representation used by every traversal stage, terminal
// and mutation function. Not all fields are populated for every kind:
//
//	VertexElement: ID, Label, Props
//	EdgeElement:   ID (edge id), Src, Dst, EdgeLabel, Props
//	ValueElement:  Key (property key), Value (property value bytes)
type Element struct {
	Kind      ElementKind
	ID        string
	Label     string // vertex label from the record envelope
	Props     []byte // vertex/edge properties (opaque bytes)
	Src       string // EdgeElement only
	Dst       string // EdgeElement only
	EdgeLabel string // EdgeElement only
	Key       string // ValueElement only: the property key
	Value     []byte // ValueElement only: raw property value bytes
}

// Property values are opaque bytes at the storage layer. Has and Values use
// JSON-object properties by convention; opaque bytes users filter through
// Iterate or Filter.
func rawPropertyValue(props []byte, key string) ([]byte, bool, error) {
	obj, err := decodeJSONObject(props)
	if err != nil {
		return nil, false, err
	}
	v, ok := obj[key]
	if !ok {
		return nil, false, nil
	}
	raw, err := jsonMarshal(v)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// dedupKey builds an unambiguous composite key for value streams.
func dedupKey(k string, v []byte) string {
	return k + dedupKeySep + fmt.Sprint(len(k)) + dedupLenSep + string(v)
}
