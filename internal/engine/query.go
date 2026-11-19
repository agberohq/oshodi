package engine

import (
	"errors"
	"math"
)

// ErrQueryConflict is returned when a query combines Range and Prefix.
var ErrQueryConflict = errors.New("oshodi: query Range and Prefix are mutually exclusive")

// Query is a functional, ordered query over one bucket. It never decodes
// values: filters and aggregators receive the raw bytes. A Query is a builder
// and is not safe for concurrent use. Terminals return the first error from
// the scan, the filter or a user callback.
type Query struct {
	bucket    *Bucket
	start     []byte
	end       []byte
	prefix    []byte
	hasPrefix bool
	limit     int
	desc      bool
	filter    func(key, value []byte) bool
	err       error
}

// Query starts an ordered query over the bucket.
func (b *Bucket) Query() *Query {
	return &Query{bucket: b, limit: -1}
}

// Range restricts the query to keys in [start, end). Nil bounds mean the
// beginning/end of the bucket.
func (q *Query) Range(start, end []byte) *Query {
	if q.err != nil {
		return q
	}
	if q.hasPrefix {
		q.err = ErrQueryConflict
		return q
	}
	q.start = cloneBytes(start)
	q.end = cloneBytes(end)
	return q
}

// Prefix restricts the query to keys beginning with prefix.
func (q *Query) Prefix(prefix []byte) *Query {
	if q.err != nil {
		return q
	}
	if q.start != nil || q.end != nil {
		q.err = ErrQueryConflict
		return q
	}
	q.prefix = cloneBytes(prefix)
	q.hasPrefix = true
	return q
}

// Limit stops after n results. A negative limit (the default) is unbounded;
// zero produces no results.
func (q *Query) Limit(n int) *Query {
	q.limit = n
	return q
}

// Descending returns results in reverse key order.
func (q *Query) Descending() *Query {
	q.desc = true
	return q
}

// Filter keeps only elements for which fn returns true. Multiple filters are
// ANDed.
func (q *Query) Filter(fn func(key, value []byte) bool) *Query {
	if fn == nil {
		return q
	}
	if q.filter == nil {
		q.filter = fn
		return q
	}
	prev := q.filter
	q.filter = func(key, value []byte) bool {
		return prev(key, value) && fn(key, value)
	}
	return q
}

// ForEach invokes fn for each matching key in order. The key and value slices
// are only valid until fn returns, and the traversal stops on the first error
// returned by fn.
func (q *Query) ForEach(fn func(key, value []byte) error) error {
	if q.err != nil {
		return q.err
	}
	if q.limit == 0 {
		return nil
	}
	n := 0
	var cbErr error
	visit := func(key, value []byte) bool {
		if q.filter != nil && !q.filter(key, value) {
			return true
		}
		if err := fn(key, value); err != nil {
			cbErr = err
			return false
		}
		n++
		return q.limit < 0 || n < q.limit
	}

	var err error
	switch {
	case q.hasPrefix && q.desc:
		err = q.bucket.ScanPrefixDesc(q.prefix, visit)
	case q.hasPrefix:
		err = q.bucket.ScanPrefix(q.prefix, visit)
	case q.desc:
		err = q.bucket.ScanRangeDesc(q.start, q.end, visit)
	default:
		err = q.bucket.ScanRange(q.start, q.end, visit)
	}
	if err != nil {
		return err
	}
	return cbErr
}

// Count returns the number of matching elements. Without a filter it walks
// keys only and never reads values.
func (q *Query) Count() (int, error) {
	if q.err != nil {
		return 0, q.err
	}
	if q.limit == 0 {
		return 0, nil
	}
	if q.filter == nil {
		n := 0
		visit := func(key []byte) bool {
			n++
			return q.limit < 0 || n < q.limit
		}
		var err error
		switch {
		case q.hasPrefix && q.desc:
			err = q.bucket.ScanPrefixKeysDesc(q.prefix, visit)
		case q.hasPrefix:
			err = q.bucket.ScanPrefixKeys(q.prefix, visit)
		case q.desc:
			err = q.bucket.ScanRangeKeysDesc(q.start, q.end, visit)
		default:
			err = q.bucket.ScanRangeKeys(q.start, q.end, visit)
		}
		return n, err
	}
	n := 0
	err := q.ForEach(func(key, value []byte) error {
		n++
		return nil
	})
	return n, err
}

// Sum adds the value returned by fn for each matching element.
func (q *Query) Sum(fn func(key, value []byte) (float64, error)) (float64, error) {
	total := 0.0
	err := q.ForEach(func(key, value []byte) error {
		v, err := fn(key, value)
		if err != nil {
			return err
		}
		total += v
		return nil
	})
	return total, err
}

// Min returns the smallest value produced by fn. found is false when the query
// matched nothing.
func (q *Query) Min(fn func(key, value []byte) (float64, error)) (min float64, found bool, err error) {
	min = math.Inf(1)
	err = q.ForEach(func(key, value []byte) error {
		v, err := fn(key, value)
		if err != nil {
			return err
		}
		if !found || v < min {
			min = v
			found = true
		}
		return nil
	})
	if !found {
		min = 0
	}
	return min, found, err
}

// Max returns the largest value produced by fn. found is false when the query
// matched nothing.
func (q *Query) Max(fn func(key, value []byte) (float64, error)) (max float64, found bool, err error) {
	max = math.Inf(-1)
	err = q.ForEach(func(key, value []byte) error {
		v, err := fn(key, value)
		if err != nil {
			return err
		}
		if !found || v > max {
			max = v
			found = true
		}
		return nil
	})
	if !found {
		max = 0
	}
	return max, found, err
}

// GroupByCount counts matching elements per group key.
func (q *Query) GroupByCount(fn func(key, value []byte) (string, error)) (map[string]int, error) {
	out := make(map[string]int)
	err := q.ForEach(func(key, value []byte) error {
		group, err := fn(key, value)
		if err != nil {
			return err
		}
		out[group]++
		return nil
	})
	return out, err
}

// GroupBySum sums a value per group key.
func (q *Query) GroupBySum(
	keyFn func(key, value []byte) (string, error),
	valueFn func(key, value []byte) (float64, error),
) (map[string]float64, error) {
	out := make(map[string]float64)
	err := q.ForEach(func(key, value []byte) error {
		group, err := keyFn(key, value)
		if err != nil {
			return err
		}
		v, err := valueFn(key, value)
		if err != nil {
			return err
		}
		out[group] += v
		return nil
	})
	return out, err
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
