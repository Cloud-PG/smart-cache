package cache

import (
	"time"

	"go.uber.org/zap"
)

const (
	// MaxNumDaysStat limit to stay in the stats
	MaxNumDaysStat = 7.
	// DeltaDays2Purge limit the clean action of the stats
	DeltaDays2Purge = 14.
)

// Stats collector of statistics for weight function cache
type Stats struct {
	fileStats       map[int64]*FileStats
	weightSum       float64
	firstUpdateTime time.Time
	lastUpdateTime  time.Time
}

// Init initialize Stats
func (statStruct *Stats) Init() {
	statStruct.fileStats = make(map[int64]*FileStats)
	statStruct.weightSum = 0.0
}

// Clear Stats after load
func (statStruct *Stats) Clear() {
	for _, fileStats := range statStruct.fileStats {
		fileStats.InCache = false
		fileStats.InCacheSince = time.Time{}
		fileStats.InCacheTick = -1
		fileStats.Recency = 0
	}
	statStruct.weightSum = 0.0
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
	logger.Debug("Stats purged", zap.Int("NumDeletedFiles", numDeletedFiles))
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
		curTick int64
	)

	switch {
	case len(vars) > 2:
		curTick = vars[2].(int64)
		fallthrough
	case len(vars) > 1:
		reqTime = vars[1].(time.Time)
		fallthrough
	default:
		size = vars[0].(float64)
	}

	// Stats age update
	if statStruct.firstUpdateTime.IsZero() {
		logger.Info("Updated first time")
		statStruct.firstUpdateTime = reqTime
	}
	statStruct.lastUpdateTime = reqTime

	// Update file stats
	curStats, inStats := statStruct.fileStats[filename]

	if !inStats {
		curStats = &FileStats{
			Filename:         filename,
			Size:             size,
			FirstTime:        reqTime,
			DeltaLastRequest: 0,
			Recency:          curTick,
			InCacheTick:      -1,
		}
		statStruct.fileStats[filename] = curStats
	} else {
		curStats.Size = size
		curStats.DeltaLastRequest = curTick - curStats.Recency
		curStats.Recency = curTick
	}

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

const (
	// RequestTicksSize represents the  number of slots
	RequestTicksSize int = 32
	// NumDaysStatsDecay is the number of days that stats are maintained
	NumDaysStatsDecay = 7.0
	// NumDaysPointsDecay is the number of days that points are maintained
	NumDaysPointsDecay = 3.0
)

// FileStats contains file statistics collected by weight function caches
type FileStats struct {
	Filename          int64       `json:"filename"`
	Weight            float64     `json:"weight"`
	Size              float64     `json:"size"`
	Frequency         int64       `json:"frequency"`
	FrequencyInCache  int64       `json:"frequencyInCache"`
	NHits             int64       `json:"nHits"`
	NMiss             int64       `json:"nMiss"`
	FirstTime         time.Time   `json:"firstTime"`
	InCacheSince      time.Time   `json:"inCacheSince"`
	InCacheTick       int64       `json:"inCacheTick"`
	InCache           bool        `json:"inCache"`
	LastTimeRequested time.Time   `json:"lastTimeRequested"`
	RequestTicksMean  float64     `json:"requestTicksMean"`
	RequestTicks      []time.Time `json:"requestTicks"`
	IdxLastRequest    int         `json:"idxLastRequest"`
	DeltaLastRequest  int64       `json:"deltaLastRequest"`
	Recency           int64       `json:"recency"`
	DataType          int64       `json:"dataType"`
	FileType          int64       `json:"fileType"`
}

// DiffLastUpdate returns the number of days from the last update stats
func (stats FileStats) DiffLastUpdate() float64 {
	return stats.LastTimeRequested.Sub(stats.FirstTime).Hours() / 24.
}

func (stats *FileStats) addInCache(tick int64, curTime *time.Time) {
	if curTime != nil {
		stats.InCacheSince = *curTime
	}
	stats.InCacheTick = tick
	stats.InCache = true
}

func (stats *FileStats) removeFromCache() {
	stats.InCacheSince = time.Time{}
	stats.InCache = false
	stats.InCacheTick = -1
	stats.FrequencyInCache = 0
}

func (stats *FileStats) updateStats(hit bool, size float64, userID int64, siteName int64, curTime time.Time) {
	stats.Size = size

	stats.Frequency++
	if stats.InCache {
		stats.FrequencyInCache++
	}

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

func (stats *FileStats) updateWeight(functionType FunctionType, alpha float64, beta float64, gamma float64) float64 {
	switch functionType {
	case FuncAdditive:
		stats.Weight = fileAdditiveWeightFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestTicksMean,
			alpha,
			beta,
			gamma,
		)
	case FuncAdditiveExp:
		stats.Weight = fileAdditiveExpWeightFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestTicksMean,
			alpha,
			beta,
			gamma,
		)
	case FuncMultiplicative:
		stats.Weight = fileMultiplicativeWeightFunction(
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
