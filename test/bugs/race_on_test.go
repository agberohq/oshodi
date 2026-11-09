//go:build race

package bugs

// raceEnabled reports whether the race detector is active. Race
// instrumentation allocates on its own, so allocation counts are not
// meaningful under -race.
const raceEnabled = true
