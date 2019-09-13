package cache

import (
	"math"
	"time"
)

const (
	// StatsMemorySize represents the  number of slots
	StatsMemorySize uint64 = 8
)

// UpdateStatsPolicyType is used to select the update stats policy
type UpdateStatsPolicyType int

const (
	// UpdateStatsOnRequest indicates to update the file stats on each request
	UpdateStatsOnRequest UpdateStatsPolicyType = iota
	// UpdateStatsOnMiss indicates to update the file stats only on file miss
	UpdateStatsOnMiss
)

// UpdateWeightPolicyType is used to select the update weight policy
type UpdateWeightPolicyType int

const (
	// UpdateSingleWeight indicates to update the file weight only for the current file
	UpdateSingleWeight UpdateWeightPolicyType = iota
	// UpdateAllWeights indicates to update the file weight for each file in stats
	UpdateAllWeights
)

// LimitStatsPolicyType is used to limit the amount of stats collected
type LimitStatsPolicyType int

const (
	// NoLimitStats indicates to not delete stats ever
	NoLimitStats LimitStatsPolicyType = iota
	// Q1IsDoubleQ2LimitStats indicates to remove stats with weight >= Q1 if Q1 > 2*Q2
	Q1IsDoubleQ2LimitStats
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
	} else {
		stats.meanTime = stats.getMeanReqTimes(curTime)
	}
}

func (stats *weightedFileStats) updateWeight(functionType FunctionType, exp float32, curTime time.Time) {
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
			stats.getMeanReqTimes(curTime),
			exp,
		)
	}
}

func (stats weightedFileStats) getMeanReqTimes(curTime time.Time) float32 {
	if !math.IsNaN(float64(stats.meanTime)) {
		return stats.meanTime
	}
	var timeDiffSum time.Duration
	var timeReference time.Time
	if curTime.IsZero() {
		timeReference = stats.lastTimeRequested
	} else {
		timeReference = curTime
	}
	for idx := 0; idx < int(StatsMemorySize); idx++ {
		if !stats.requestTicks[idx].IsZero() {
			timeDiffSum += timeReference.Sub(stats.requestTicks[idx])
		}
	}
	if timeDiffSum != 0. {
		return float32(timeDiffSum.Seconds()) / float32(StatsMemorySize)
	}
	return 0.
}

// ByWeight implements sort.Interface based on the Weight field.
type ByWeight []*weightedFileStats

func (a ByWeight) Len() int { return len(a) }

// Order from the highest weight to the smallest
func (a ByWeight) Less(i, j int) bool { return a[i].weight > a[j].weight }
func (a ByWeight) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
