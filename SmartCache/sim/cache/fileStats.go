package cache

import (
	"encoding/json"
	"time"
)

//##############################################################################
//                                LRU Statistics                               #
//##############################################################################

// LRUStats collector of statistics for LRU cache
type LRUStats struct {
	stats map[string]*LRUFileStats
}

// Init initialize LRUStats
func (statStruct *LRUStats) Init() {
	statStruct.stats = make(map[string]*LRUFileStats)
}

// GetOrCreate add the file into stats and returns it
func (statStruct *LRUStats) GetOrCreate(filename string, size float32) *LRUFileStats {
	curStats, inStats := statStruct.stats[filename]
	if !inStats {
		curStats = &LRUFileStats{
			size,
			0,
			0,
			0,
		}
		statStruct.stats[filename] = curStats
	}
	return curStats
}

// LRUFileStats contain file statistics collected by LRU cache
type LRUFileStats struct {
	Size        float32 `json:"size"`
	TotRequests uint32  `json:"totRequests"`
	NHits       uint32  `json:"nHits"`
	NMiss       uint32  `json:"nMiss"`
}

func (stats *LRUFileStats) updateRequests(hit bool) {
	stats.TotRequests++

	if hit {
		stats.NHits++
	} else {
		stats.NMiss++
	}
}

//##############################################################################
//                                Weighted files                               #
//##############################################################################

// WeightedStats collector of statistics for weighted cache
type WeightedStats struct {
	stats     map[string]*WeightedFileStats
	weightSum float32
}

// Init initialize WeightedStats
func (statStruct *WeightedStats) Init() {
	statStruct.stats = make(map[string]*WeightedFileStats)
	statStruct.weightSum = 0.0
}

// GetOrCreate add the file into stats and returns it
func (statStruct *WeightedStats) GetOrCreate(filename string, size float32) (*WeightedFileStats, bool) {
	curStats, inStats := statStruct.stats[filename]
	if !inStats {
		curStats = &WeightedFileStats{
			Size: size,
		}
		statStruct.stats[filename] = curStats
	}
	return curStats, !inStats
}

// UpdateWeight update the weight of a file and also the sum of all weights
func (statStruct *WeightedStats) UpdateWeight(stats *WeightedFileStats, newFile bool, functionType FunctionType, exp float32) {
	if newFile {
		statStruct.weightSum += stats.updateWeight(functionType, exp)
	} else {
		statStruct.weightSum -= stats.Weight
		statStruct.weightSum += stats.updateWeight(functionType, exp)
	}
}

// GetWeightMedian returns the mean of the weight of all files
func (statStruct *WeightedStats) GetWeightMedian() float32 {
	return statStruct.weightSum / float32(len(statStruct.stats))
}

const (
	// StatsMemorySize represents the  number of slots
	StatsMemorySize uint64 = 32
)

// UpdateStatsPolicyType is used to select the update stats policy
type UpdateStatsPolicyType int

const (
	// UpdateStatsOnRequest indicates to update the file stats on each request
	UpdateStatsOnRequest UpdateStatsPolicyType = iota
	// UpdateStatsOnMiss indicates to update the file stats only on file miss
	UpdateStatsOnMiss
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

// WeightedFileStats contains file statistics collected by weighted caches
type WeightedFileStats struct {
	Filename          string                     `json:"filename"`
	Weight            float32                    `json:"weight"`
	Size              float32                    `json:"size"`
	TotRequests       uint32                     `json:"totRequests"`
	NHits             uint32                     `json:"nHits"`
	NMiss             uint32                     `json:"nMiss"`
	InCacheSince      time.Time                  `json:"inCacheSince"`
	LastTimeRequested time.Time                  `json:"lastTimeRequested"`
	RequestTicksMean  float32                    `json:"requestTicksMean"`
	RequestTicks      [StatsMemorySize]time.Time `json:"requestTicks"`
	RequestLastIdx    int                        `json:"requestLastIdx"`
}

func (stats WeightedFileStats) dumps() []byte {
	dumpStats, _ := json.Marshal(stats)
	return dumpStats
}

func (stats *WeightedFileStats) loads(inString string) *WeightedFileStats {
	json.Unmarshal([]byte(inString), &stats)
	return stats
}

func (stats *WeightedFileStats) addInCache(curTime *time.Time) {
	stats.InCacheSince = *curTime
}

func (stats *WeightedFileStats) updateStats(hit bool, size float32, curTime *time.Time) {
	stats.TotRequests++
	stats.Size = size

	if hit {
		stats.NHits++
	} else {
		stats.NMiss++
	}

	if curTime != nil {
		stats.LastTimeRequested = *curTime
		stats.RequestTicks[stats.RequestLastIdx] = *curTime
		stats.RequestLastIdx = (stats.RequestLastIdx + 1) % int(StatsMemorySize)
		stats.RequestTicksMean = stats.getMeanReqTimes()
	}
}

func (stats *WeightedFileStats) updateWeight(functionType FunctionType, exp float32) float32 {
	switch functionType {
	case FuncFileWeight:
		stats.Weight = fileWeight(
			stats.Size,
			stats.TotRequests,
			exp,
		)
	case FuncFileWeightAndTime:
		stats.Weight = fileWeightAndTime(
			stats.Size,
			stats.TotRequests,
			exp,
			stats.LastTimeRequested,
		)
	case FuncFileWeightOnlyTime:
		stats.Weight = fileWeightOnlyTime(
			stats.TotRequests,
			exp,
			stats.LastTimeRequested,
		)
	case FuncWeightedRequests:
		stats.Weight = fileWeightedRequest(
			stats.Size,
			stats.TotRequests,
			stats.RequestTicksMean,
			exp,
		)
	}
	return stats.Weight
}

func (stats WeightedFileStats) getMeanReqTimes() float32 {
	var timeDiffSum time.Duration
	for idx := 0; idx < int(StatsMemorySize); idx++ {
		if !stats.RequestTicks[idx].IsZero() {
			timeDiffSum += stats.LastTimeRequested.Sub(stats.RequestTicks[idx])
		}
	}
	if timeDiffSum != 0. {
		return float32(timeDiffSum.Minutes()) / float32(StatsMemorySize)
	}
	return 0.
}

// ByWeight implements sort.Interface based on the Weight field.
type ByWeight []*WeightedFileStats

func (a ByWeight) Len() int { return len(a) }

// Order from the highest weight to the smallest
func (a ByWeight) Less(i, j int) bool { return a[i].Weight > a[j].Weight }
func (a ByWeight) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
