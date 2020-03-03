package cache

import (
	"encoding/json"
	"math"
	"time"

	"go.uber.org/zap"
)

const (
	// MaxNumDaysStat limit to stay in the stats
	MaxNumDaysStat = 14.
	// DeltaDays2Purge limit the clean action of the stats
	DeltaDays2Purge = 7.
)

// Stats collector of statistics for weighted cache
type Stats struct {
	fileStats       map[int64]*FileStats
	weightSum       float64
	firstUpdateTime time.Time
	lastUpdateTime  time.Time
	numRequests     int64
}

// Init initialize Stats
func (statStruct *Stats) Init() {
	statStruct.fileStats = make(map[int64]*FileStats)
	statStruct.weightSum = 0.0
	statStruct.numRequests = 0
}

// Dirty indicates if the stats needs a purge
func (statStruct Stats) Dirty() bool {
	numDays := statStruct.lastUpdateTime.Sub(statStruct.firstUpdateTime).Hours() / 24.
	if numDays >= DeltaDays2Purge {
		logger.Debug("Dirty Stats", zap.Float64("numDays", numDays),
			zap.String("lastTime", statStruct.lastUpdateTime.Format(time.UnixDate)),
			zap.String("firstTime", statStruct.firstUpdateTime.Format(time.UnixDate)),
		)
		return true
	}
	return false
}

// Purge remove older stats
func (statStruct *Stats) Purge() {
	numDeletedFiles := 0
	for filename, stats := range statStruct.fileStats {
		if !stats.InCache && stats.DiffLastUpdate() >= MaxNumDaysStat {
			logger.Debug("Purge", zap.Bool("in cache", stats.InCache))
			statStruct.weightSum -= stats.Weight
			delete(statStruct.fileStats, filename)
			numDeletedFiles++
		}
	}
	logger.Info("Stats purged", zap.Int("NumDeletedFiles", numDeletedFiles))
	statStruct.firstUpdateTime = statStruct.lastUpdateTime
}

// Get returns the stats without update them
func (statStruct Stats) Get(filename int64) *FileStats {
	curStats, inStats := statStruct.fileStats[filename]
	if !inStats {
		logger.Error("Get: no file found", zap.Int64("filename", filename))
	}
	return curStats
}

// GetOrCreate add the file into stats and returns (stats, is new file)
func (statStruct *Stats) GetOrCreate(filename int64, vars ...interface{}) (*FileStats, bool) {
	var (
		size    float64
		reqTime time.Time
	)

	switch {
	case len(vars) > 1:
		reqTime = vars[1].(time.Time)
		fallthrough
	default:
		size = vars[0].(float64)
	}

	// Stats age update
	if statStruct.firstUpdateTime.IsZero() {
		logger.Info("Update first time")
		statStruct.firstUpdateTime = reqTime
	}
	statStruct.lastUpdateTime = reqTime

	// Update file stats
	curStats, inStats := statStruct.fileStats[filename]

	if !inStats {
		curStats = &FileStats{
			Size:             size,
			FirstTime:        reqTime,
			DeltaLastRequest: 0,
			Recency:          statStruct.numRequests,
		}
		statStruct.fileStats[filename] = curStats
	} else {
		curStats.Size = size
		curStats.DeltaLastRequest = statStruct.numRequests - curStats.Recency
		curStats.Recency = statStruct.numRequests
	}

	statStruct.numRequests++

	return curStats, !inStats
}

// UpdateWeight update the weight of a file and also the sum of all weights
func (statStruct *Stats) updateWeight(stats *FileStats, newFile bool, functionType FunctionType, alpha float64, beta float64, gamma float64) {
	if newFile {
		statStruct.weightSum += stats.updateWeight(functionType, alpha, beta, gamma)
	} else {
		statStruct.weightSum -= stats.Weight
		statStruct.weightSum += stats.updateWeight(functionType, alpha, beta, gamma)
	}
}

// GetWeightMedian returns the mean of the weight of all files
func (statStruct *Stats) GetWeightMedian() float64 {
	return statStruct.weightSum / float64(len(statStruct.fileStats))
}

// updateFilePoints returns the points for a single file
func (statStruct Stats) updateFilesPoints(filename int64, curTime *time.Time) float64 {
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
	Filename          int64       `json:"filename"`
	Weight            float64     `json:"weight"`
	Points            float64     `json:"points"`
	Size              float64     `json:"size"`
	Frequency         int64       `json:"frequency"`
	NHits             int64       `json:"nHits"`
	NMiss             int64       `json:"nMiss"`
	FirstTime         time.Time   `json:"firstTime"`
	InCacheSince      time.Time   `json:"inCacheSince"`
	InCache           bool        `json:"inCache"`
	LastTimeRequested time.Time   `json:"lastTimeRequested"`
	RequestTicksMean  float64     `json:"requestTicksMean"`
	RequestTicks      []time.Time `json:"requestTicks"`
	IdxLastRequest    int         `json:"idxLastRequest"`
	DeltaLastRequest  int64       `json:"deltaLastRequest"`
	Recency           int64       `json:"recency"`
	Users             []int64     `json:"users"`
	Sites             []int64     `json:"sites"`
}

// DiffLastUpdate returns the number of days from the last update stats
func (stats FileStats) DiffLastUpdate() float64 {
	return stats.LastTimeRequested.Sub(stats.FirstTime).Hours() / 24.
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
	if curTime != nil {
		stats.InCacheSince = *curTime
	}
	stats.InCache = true
}

func (stats *FileStats) removeFromCache() {
	stats.InCacheSince = time.Time{}
	stats.InCache = false
}

func (stats *FileStats) addUser(userID int64) {
	for _, user := range stats.Users {
		if user == userID {
			return
		}
	}
	stats.Users = append(stats.Users, userID)
}

func (stats *FileStats) addSite(siteName int64) {
	for _, site := range stats.Sites {
		if site == siteName {
			return
		}
	}
	stats.Sites = append(stats.Sites, siteName)
}

func (stats *FileStats) updateStats(hit bool, size float64, userID int64, siteName int64, curTime time.Time) {
	stats.Size = size

	stats.addUser(userID)
	stats.addSite(siteName)

	stats.Frequency++

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
	numReq := float64(stats.Frequency)
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

func (stats *FileStats) updateWeight(functionType FunctionType, alpha float64, beta float64, gamma float64) float64 {
	switch functionType {
	case FuncAdditive:
		stats.Weight = fileWeightedAdditiveFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestTicksMean,
			alpha,
			beta,
			gamma,
		)
	case FuncAdditiveExp:
		stats.Weight = fileWeightedAdditiveExpFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestTicksMean,
			alpha,
			beta,
			gamma,
		)
	case FuncMultiplicative:
		stats.Weight = fileWeightedMultiplicativeFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestTicksMean,
			alpha,
			beta,
			gamma,
		)
	case FuncWeightedRequests:
		stats.Weight = fileWeightedRequest(
			stats.Frequency,
			stats.Size,
			stats.RequestTicksMean,
		)
	}
	return stats.Weight
}

func (stats FileStats) getMeanReqTimes() float64 {
	var timeDiffSum time.Duration
	for idx := 0; idx < len(stats.RequestTicks); idx++ {
		if !stats.RequestTicks[idx].IsZero() {
			timeDiffSum += stats.LastTimeRequested.Sub(stats.RequestTicks[idx])
		}
	}
	if timeDiffSum != 0. {
		return timeDiffSum.Minutes() / float64(RequestTicksSize)
	}
	return 0.
}
