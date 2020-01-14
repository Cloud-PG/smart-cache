package cache

import (
	"encoding/json"
	"math"
	"sort"
	"time"
)

// Stats collector of statistics for weighted cache
type Stats struct {
	data      map[string]*FileStats
	weightSum float32
}

// Init initialize Stats
func (statStruct *Stats) Init() {
	statStruct.data = make(map[string]*FileStats)
	statStruct.weightSum = 0.0
}

// GetOrCreate add the file into stats and returns (stats, is new file)
func (statStruct *Stats) GetOrCreate(filename string, vars ...interface{}) (*FileStats, bool) {
	var (
		size      float32
		firstTime time.Time
	)

	switch {
	case len(vars) > 1:
		firstTime = vars[1].(time.Time)
		fallthrough
	default:
		size = vars[0].(float32)
	}

	curStats, inStats := statStruct.data[filename]

	if !inStats {
		curStats = &FileStats{
			Filename:  filename,
			Size:      size,
			FirstTime: firstTime,
		}
		statStruct.data[filename] = curStats
	} else {
		curStats.Size = size
	}

	return curStats, !inStats
}

// UpdateWeight update the weight of a file and also the sum of all weights
func (statStruct *Stats) updateWeight(stats *FileStats, newFile bool, functionType FunctionType, exp float32) {
	if newFile {
		statStruct.weightSum += stats.updateWeight(functionType, exp)
	} else {
		statStruct.weightSum -= stats.Weight
		statStruct.weightSum += stats.updateWeight(functionType, exp)
	}
}

// GetWeightMedian returns the mean of the weight of all files
func (statStruct *Stats) GetWeightMedian() float32 {
	return statStruct.weightSum / float32(len(statStruct.data))
}

func (statStruct *Stats) getPoints(filename string) float64 {
	stats, _ := statStruct.data[filename]
	return stats.Points
}

// updateFilePoints returns the points for a single file
func (statStruct Stats) updateFilesPoints(filename string, curTime *time.Time) float64 {
	curStats, _ := statStruct.data[filename]
	return curStats.updateFilePoints(curTime)
}

const (
	// StatsMemorySize represents the  number of slots
	StatsMemorySize uint64 = 32
	// NumDaysStatsDecay is the number of days that stats are maintained
	NumDaysStatsDecay = 7.0
	// NumDaysPointsDecay is the number of days that points are maintained
	NumDaysPointsDecay = 5.0
)

type cacheEmptyMsg struct{}

// FileStats contains file statistics collected by weighted caches
type FileStats struct {
	Filename          string                     `json:"filename"`
	Weight            float32                    `json:"weight"`
	Points            float64                    `json:"points"`
	Size              float32                    `json:"size"`
	TotRequests       uint32                     `json:"totRequests"`
	NHits             uint32                     `json:"nHits"`
	NMiss             uint32                     `json:"nMiss"`
	FirstTime         time.Time                  `json:"firstTime"`
	InCacheSince      time.Time                  `json:"inCacheSince"`
	LastTimeRequested time.Time                  `json:"lastTimeRequested"`
	RequestTicksMean  float32                    `json:"requestTicksMean"`
	RequestTicks      [StatsMemorySize]time.Time `json:"requestTicks"`
	RequestLastIdx    int                        `json:"requestLastIdx"`
	Users             []int                      `json:"users"`
	Sites             []string                   `json:"sites"`
}

func (stats FileStats) dumps() []byte {
	dumpStats, _ := json.Marshal(stats)
	return dumpStats
}

func (stats *FileStats) loads(inString string) *FileStats {
	json.Unmarshal([]byte(inString), &stats)
	return stats
}

func (stats *FileStats) addInCache(curTime *time.Time) {
	stats.InCacheSince = *curTime
}

func (stats *FileStats) addUser(userID int) {
	idx := sort.Search(len(stats.Users), func(idx int) bool { return userID <= stats.Users[idx] })
	if idx >= len(stats.Users) || stats.Users[idx] != userID {
		stats.Users = append(stats.Users, userID)
		sort.Ints(stats.Users)
	}
}

func (stats *FileStats) addSite(siteName string) {
	idx := sort.Search(len(stats.Sites), func(idx int) bool { return siteName <= stats.Sites[idx] })
	if idx >= len(stats.Sites) || stats.Sites[idx] != siteName {
		stats.Sites = append(stats.Sites, siteName)
		sort.Strings(stats.Sites)
	}
}

func (stats *FileStats) updateStats(hit bool, size float32, userID int, siteName string, curTime *time.Time) {
	stats.TotRequests++
	stats.Size = size

	stats.addUser(userID)
	stats.addSite(siteName)

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

// getRealTimeStats returns the weighted num. of requests
func (stats FileStats) getRealTimeStats(curTime *time.Time) (float64, float64, float64) {
	dayDiffFirstTime := math.Floor(curTime.Sub(stats.FirstTime).Hours() / 24.)
	realNumReq, realNumUsers, realNumSites := stats.getStats()
	if dayDiffFirstTime >= NumDaysStatsDecay {
		realNumReq, realNumUsers, realNumSites := stats.getStats()
		numReq := realNumReq * math.Exp(-1.0)
		numUsers := realNumUsers * math.Exp(-1.0)
		numSites := realNumSites * math.Exp(-1.0)
		return numReq, numUsers, numSites
	}
	return realNumReq, realNumUsers, realNumSites
}

// getStats returns number of requests, users and sites
func (stats FileStats) getStats() (float64, float64, float64) {
	numReq := float64(stats.TotRequests)
	numUsers := float64(len(stats.Users))
	numSites := float64(len(stats.Sites))
	return numReq, numUsers, numSites
}

// updateFilePoints returns the points for a single file
func (stats *FileStats) updateFilePoints(curTime *time.Time) float64 {
	numReq, numUsers, numSites := stats.getRealTimeStats(curTime)
	dayDiffInCache := math.Floor(curTime.Sub(stats.InCacheSince).Hours() / 24.)

	points := numReq * 10. * numUsers * 100. * numSites * 1000. * float64(stats.Size)

	if dayDiffInCache >= NumDaysPointsDecay {
		points = points * math.Exp(-1.0) // Decay points
	}

	stats.Points = points

	return points
}

func (stats *FileStats) updateWeight(functionType FunctionType, exp float32) float32 {
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

func (stats FileStats) getMeanReqTimes() float32 {
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
type ByWeight []*FileStats

func (a ByWeight) Len() int { return len(a) }

// Order from the highest weight to the smallest
func (a ByWeight) Less(i, j int) bool { return a[i].Weight > a[j].Weight }
func (a ByWeight) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
