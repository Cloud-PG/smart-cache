package cache

import "time"

const (
	// StatsMemorySize indicates the size of fileStats memory
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
}

func (stats *weightedFileStats) updateRequests(hit bool, curTime time.Time) {
	stats.totRequests++

	if hit {
		stats.nHits++
	} else {
		stats.nMiss++
	}

	stats.lastTimeRequested = curTime

	stats.requestTicks[stats.requestLastIdx] = curTime
	stats.requestLastIdx = (stats.requestLastIdx + 1) % int(StatsMemorySize)
}

func (stats weightedFileStats) getMeanReqTimes(curtime time.Time) float32 {
	var timeDiffSum time.Duration
	for idx := 0; idx < int(StatsMemorySize); idx++ {
		if !stats.requestTicks[idx].IsZero() {
			timeDiffSum += curtime.Sub(stats.requestTicks[idx])
		}
	}
	return float32(timeDiffSum.Seconds()) / float32(StatsMemorySize)
}
