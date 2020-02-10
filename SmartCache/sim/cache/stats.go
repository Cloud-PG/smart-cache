package cache

import (
	"encoding/json"
	"math"
	"sort"
	"time"
)

const (
	// MaxNumDaysStat limit to stay in the stats
	MaxNumDaysStat = 14.
	// NumDays2Purge limit the clean action of the stats
	NumDays2Purge = 7.
)

// Stats collector of statistics for weighted cache
type Stats struct {
	fileStats       map[string]*FileStats
	weightSum       float32
	firstUpdateTime time.Time
	lastUpdateTime  time.Time
	numRequests     int32
}

// Init initialize Stats
func (statStruct *Stats) Init() {
	statStruct.fileStats = make(map[string]*FileStats)
	statStruct.weightSum = 0.0
	statStruct.numRequests = 0
}

// DirtyStats indicates if the stats needs a purge
func (statStruct Stats) DirtyStats() bool {
	numDays := statStruct.lastUpdateTime.Sub(statStruct.firstUpdateTime).Hours() / 24.
	if numDays >= NumDays2Purge {
		return true
	}
	return false
}

// PurgeStats remove older stats
func (statStruct *Stats) PurgeStats() {
	for filename, stats := range statStruct.fileStats {
		if !stats.InCache && stats.DiffLastUpdate() >= MaxNumDaysStat {
			statStruct.weightSum -= stats.Weight
			delete(statStruct.fileStats, filename)
		}
	}
	statStruct.firstUpdateTime = statStruct.lastUpdateTime
}

// GetOrCreate add the file into stats and returns (stats, is new file)
func (statStruct *Stats) GetOrCreate(filename string, vars ...interface{}) (*FileStats, bool) {
	var (
		size    float32
		reqTime time.Time
	)

	switch {
	case len(vars) > 1:
		reqTime = vars[1].(time.Time)
		fallthrough
	default:
		size = vars[0].(float32)
	}

	// Stats age update
	if statStruct.firstUpdateTime.IsZero() {
		statStruct.firstUpdateTime = reqTime
	}
	statStruct.lastUpdateTime = reqTime

	// Update file stats
	curStats, inStats := statStruct.fileStats[filename]

	if !inStats || curStats == nil {
		curStats = &FileStats{
			Size:             size,
			FirstTime:        reqTime,
			DeltaLastRequest: 0,
		}
		statStruct.fileStats[filename] = curStats
	} else {
		curStats.Size = size
		curStats.DeltaLastRequest = statStruct.numRequests - curStats.LastRequest
		curStats.LastRequest = statStruct.numRequests
	}

	statStruct.numRequests++

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
	return statStruct.weightSum / float32(len(statStruct.fileStats))
}

// updateFilePoints returns the points for a single file
func (statStruct Stats) updateFilesPoints(filename string, curTime *time.Time) float64 {
	curStats, _ := statStruct.fileStats[filename]
	return curStats.updateFilePoints(curTime)
}

const (
	// RequestTicksSize represents the  number of slots
	RequestTicksSize int = 32
	// NumDaysStatsDecay is the number of days that stats are maintained
	NumDaysStatsDecay = 7.0
	// NumDaysPointsDecay is the number of days that points are maintained
	NumDaysPointsDecay = 3.0
)

type cacheEmptyMsg struct{}

// FileStats contains file statistics collected by weighted caches
type FileStats struct {
	Weight            float32     `json:"weight"`
	Points            float64     `json:"points"`
	Size              float32     `json:"size"`
	NHits             uint32      `json:"nHits"`
	NMiss             uint32      `json:"nMiss"`
	FirstTime         time.Time   `json:"firstTime"`
	InCacheSince      time.Time   `json:"inCacheSince"`
	InCache           bool        `json:"inCache"`
	LastTimeRequested time.Time   `json:"lastTimeRequested"`
	RequestTicksMean  float32     `json:"requestTicksMean"`
	RequestTicks      []time.Time `json:"requestTicks"`
	IdxLastRequest    int         `json:"idxLastRequest"`
	DeltaLastRequest  int32       `json:"deltaLastRequest"`
	LastRequest       int32       `json:"lastRequest"`
	Users             []int       `json:"users"`
	Sites             []string    `json:"sites"`
}

// DiffLastUpdate returns the number of days from the last update stats
func (stats FileStats) DiffLastUpdate() float64 {
	return stats.LastTimeRequested.Sub(stats.FirstTime).Hours() / 24.
}

// TotRequests returns the total amount of requests
func (stats FileStats) TotRequests() uint32 {
	return stats.NHits + stats.NMiss
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
	stats.InCache = true
}

func (stats *FileStats) removeFromCache() {
	stats.InCacheSince = time.Time{}
	stats.InCache = false
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

func (stats *FileStats) updateStats(hit bool, size float32, userID int, siteName string, curTime time.Time) {
	stats.Size = size

	stats.addUser(userID)
	stats.addSite(siteName)

	if hit {
		stats.NHits++
	} else {
		stats.NMiss++
	}

	stats.LastTimeRequested = curTime
	if len(stats.RequestTicks) < RequestTicksSize {
		stats.RequestTicks = append(stats.RequestTicks, time.Time{})
	}
	stats.RequestTicks[stats.IdxLastRequest] = curTime
	stats.IdxLastRequest = (stats.IdxLastRequest + 1) % int(RequestTicksSize)
	stats.RequestTicksMean = stats.getMeanReqTimes()
}

// getRealTimeStats returns the weighted num. of requests
func (stats FileStats) getRealTimeStats(curTime *time.Time) (float64, float64, float64) {
	dayDiffFirstTime := curTime.Sub(stats.FirstTime).Hours() / 24.
	realNumReq, realNumUsers, realNumSites := stats.getStats()
	if dayDiffFirstTime >= NumDaysStatsDecay {
		realNumReq, realNumUsers, realNumSites := stats.getStats()
		numReq := realNumReq * math.Exp(-dayDiffFirstTime/dayDiffFirstTime)
		numUsers := realNumUsers * math.Exp(-dayDiffFirstTime/dayDiffFirstTime)
		numSites := realNumSites * math.Exp(-dayDiffFirstTime/dayDiffFirstTime)
		return numReq, numUsers, numSites
	}
	return realNumReq, realNumUsers, realNumSites
}

// getStats returns number of requests, users and sites
func (stats FileStats) getStats() (float64, float64, float64) {
	numReq := float64(stats.TotRequests())
	numUsers := float64(len(stats.Users))
	numSites := float64(len(stats.Sites))
	return numReq, numUsers, numSites
}

// updateFilePoints returns the points for a single file
func (stats *FileStats) updateFilePoints(curTime *time.Time) float64 {
	numReq, numUsers, numSites := stats.getRealTimeStats(curTime)
	dayDiffInCache := math.Floor(curTime.Sub(stats.InCacheSince).Hours() / 24.)

	points := numReq*100. + numUsers*1000. + numSites*1000. + float64(stats.Size)

	if dayDiffInCache >= NumDaysPointsDecay {
		points = points * math.Exp(-dayDiffInCache/NumDaysPointsDecay) // Decay points
	}

	stats.Points = points

	return points
}

func (stats *FileStats) updateWeight(functionType FunctionType, exp float32) float32 {
	switch functionType {
	case FuncFileWeight:
		stats.Weight = fileWeight(
			stats.Size,
			stats.TotRequests(),
			exp,
		)
	case FuncFileWeightAndTime:
		stats.Weight = fileWeightAndTime(
			stats.Size,
			stats.TotRequests(),
			exp,
			stats.LastTimeRequested,
		)
	case FuncFileWeightOnlyTime:
		stats.Weight = fileWeightOnlyTime(
			stats.TotRequests(),
			exp,
			stats.LastTimeRequested,
		)
	case FuncWeightedRequests:
		stats.Weight = fileWeightedRequest(
			stats.Size,
			stats.TotRequests(),
			stats.RequestTicksMean,
			exp,
		)
	}
	return stats.Weight
}

func (stats FileStats) getMeanReqTimes() float32 {
	var timeDiffSum time.Duration
	for idx := 0; idx < len(stats.RequestTicks); idx++ {
		if !stats.RequestTicks[idx].IsZero() {
			timeDiffSum += stats.LastTimeRequested.Sub(stats.RequestTicks[idx])
		}
	}
	if timeDiffSum != 0. {
		return float32(timeDiffSum.Minutes()) / float32(RequestTicksSize)
	}
	return 0.
}

// ByWeight implements sort.Interface based on the Weight field.
type ByWeight []*FileStats

func (a ByWeight) Len() int { return len(a) }

// Order from the highest weight to the smallest
func (a ByWeight) Less(i, j int) bool { return a[i].Weight > a[j].Weight }
func (a ByWeight) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
