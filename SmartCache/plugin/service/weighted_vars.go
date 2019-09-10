package service

import (
	"math"
	"time"
)

const (
	// StatsMemorySize represents the  number of slots
	StatsMemorySize uint64 = 8
)

type cacheEmptyMsg struct{}

type weightedFileStats struct {
	filename          string
	weight            float32
	size              float32
	totRequests       uint32
	nHits             uint32
	nMiss             uint32
	lastTimeRequested time.Time
	requestTicks      [StatsMemorySize]time.Time
	requestLastIdx    int
	meanTime          float32
}

func (stats *weightedFileStats) updateStats(hit bool, totRequests uint32, size float32, curTime time.Time, meanTime float32) {
	stats.totRequests = totRequests
	stats.size = size

	if hit {
		stats.nHits++
	} else {
		stats.nMiss++
	}

	stats.lastTimeRequested = curTime

	stats.requestTicks[stats.requestLastIdx] = curTime
	stats.requestLastIdx = (stats.requestLastIdx + 1) % int(StatsMemorySize)

	if !math.IsNaN(float64(meanTime)) {
		stats.meanTime = meanTime
	}
}

func (stats *weightedFileStats) updateWeight(functionType FunctionType, exp float32) {
	switch functionType {
	case FuncFileWeight:
		stats.weight = fileWeight(
			stats.size,
			stats.totRequests,
			exp,
		)
	case FuncFileWeightAndTime:
		stats.weight = fileWeightAndTime(
			stats.size,
			stats.totRequests,
			exp,
			stats.lastTimeRequested,
		)
	case FuncFileWeightOnlyTime:
		stats.weight = fileWeightOnlyTime(
			stats.totRequests,
			exp,
			stats.lastTimeRequested,
		)
	case FuncWeightedRequests:

		stats.weight = fileWeightedRequest(
			stats.size,
			stats.totRequests,
			stats.getMeanReqTimes(),
			exp,
		)
	}
}

func (stats weightedFileStats) getMeanReqTimes() float32 {
	if !math.IsNaN(float64(stats.meanTime)) {
		return stats.meanTime
	}
	var timeDiffSum time.Duration
	lastTimeRequested := stats.lastTimeRequested
	for idx := 0; idx < int(StatsMemorySize); idx++ {
		if !stats.requestTicks[idx].IsZero() {
			timeDiffSum += lastTimeRequested.Sub(stats.requestTicks[idx])
		}
	}
	if timeDiffSum != 0. {
		return float32(timeDiffSum.Seconds()) / float32(StatsMemorySize)
	}
	return 0.
}

// UpdateStatsPolicyType is used to select the update stats policy
type UpdateStatsPolicyType int

const (
	// UpdateStatsOnRequest indicates to update the file stats on each request
	UpdateStatsOnRequest UpdateStatsPolicyType = iota
	// UpdateStatsOnMiss indicates to update the file stats only on file miss
	UpdateStatsOnMiss
)

// ByWeight implements sort.Interface based on the Weight field.
type ByWeight []*weightedFileStats

func (a ByWeight) Len() int { return len(a) }

// Order from the highest weight to the smallest
func (a ByWeight) Less(i, j int) bool { return a[i].weight > a[j].weight }
func (a ByWeight) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
