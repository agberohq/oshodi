package metrics

// Manager coordinates zero-lock global database analytics.
// Eliminated the internal mappo.Concurrent lookup to ensure 0 map lookups on the database insertion hot path.
// The Bloom filter is NOT managed here; it is owned by the Index to avoid duplicate updates.
type Manager struct {
	globalHLL    *HyperLogLog
	globalSketch *CountMinSketch

	enableHLL bool
	enableCMS bool
}

// NewManager creates a new extremely fast metrics manager.
func NewManager(enableCardinality bool, precision uint8, enableFrequency bool) *Manager {
	m := &Manager{
		enableHLL: enableCardinality,
		enableCMS: enableFrequency,
	}

	if enableCardinality {
		m.globalHLL = NewHyperLogLog(precision)
	}
	if enableFrequency {
		m.globalSketch = NewCountMinSketch(0.01, 0.99)
	}

	return m
}

// Add pushes a key to all enabled analytics components using direct pointers.
// The Bloom filter is NOT updated here; it is owned by the Index and updated
// once per write via Index.Set() to avoid duplicate work.
func (m *Manager) Add(key []byte) {
	if m.enableHLL && m.globalHLL != nil {
		m.globalHLL.Add(key)
	}

	if m.enableCMS && m.globalSketch != nil {
		m.globalSketch.Add(key)
	}
}

// EstimatedKeys computes the approximate unique keys lock-free.
func (m *Manager) EstimatedKeys() uint64 {
	if !m.enableHLL || m.globalHLL == nil {
		return 0
	}
	return uint64(m.globalHLL.Estimate())
}

// EstimateFrequency estimates frequency of a specific key lock-free.
func (m *Manager) EstimateFrequency(key []byte) uint64 {
	if !m.enableCMS || m.globalSketch == nil {
		return 0
	}
	return m.globalSketch.Estimate(key)
}
