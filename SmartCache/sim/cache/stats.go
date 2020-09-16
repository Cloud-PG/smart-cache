package cache

import (
	"time"

	"go.uber.org/zap"
)

const (
	numHoursInADay = 24.
)

// Stats collector of statistics for weight function cache
type Stats struct {
	fileStats       map[int64]*FileStats
	weightSum       float64
	calcWeight      bool
	firstUpdateTime time.Time
	lastUpdateTime  time.Time
	maxNumDayDiff   float64 // MaxNumDayDiff limit to stay in the stats
	deltaDaysStep   float64 // DeltaDaysStep limit the clean action of the stats
	logger          *zap.Logger
}

// Init initialize Stats
func (statStruct *Stats) Init(maxNumDayDiff float64, deltaDaysStep float64, calcWeight bool) {
	statStruct.fileStats = make(map[int64]*FileStats)
	statStruct.calcWeight = calcWeight
	statStruct.maxNumDayDiff = maxNumDayDiff
	statStruct.deltaDaysStep = deltaDaysStep
	statStruct.logger = zap.L()
}

// Clear Stats after load
func (statStruct *Stats) Clear() {
	for _, fileStats := range statStruct.fileStats {
		fileStats.InCache = false
		fileStats.InCacheSinceTime = time.Time{}
		fileStats.InCacheSinceTick = -1
		fileStats.Recency = 0
	}
	statStruct.weightSum = 0.0
}

// Dirty indicates if the stats needs a purge
func (statStruct Stats) Dirty() bool {
	numDays := statStruct.lastUpdateTime.Sub(statStruct.firstUpdateTime).Hours() / numHoursInADay

	if numDays >= statStruct.deltaDaysStep {
		statStruct.logger.Debug("Dirty Stats", zap.Float64("numDays", numDays),
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
		if !stats.InCache && stats.DiffLastUpdate(statStruct.lastUpdateTime) >= statStruct.maxNumDayDiff {
			statStruct.logger.Debug("Purge", zap.Bool("in cache", stats.InCache))
			if statStruct.calcWeight {
				statStruct.weightSum -= stats.Weight
			}
			delete(statStruct.fileStats, filename)
			numDeletedFiles++
		}
	}
	statStruct.logger.Debug("Stats purged", zap.Int("NumDeletedFiles", numDeletedFiles))
	statStruct.firstUpdateTime = statStruct.lastUpdateTime
}

// Get returns the stats without update them
func (statStruct Stats) Get(filename int64) *FileStats {
	curStats, inStats := statStruct.fileStats[filename]
	if !inStats {
		statStruct.logger.Error("Get: no file found", zap.Int64("filename", filename))
	}
	return curStats
}

// GetOrCreate add the file into stats and returns (stats, is new file)
func (statStruct *Stats) GetOrCreate(filename int64, size float64, reqTime time.Time, curTick int64) (*FileStats, bool) {
	// Stats age update
	if statStruct.firstUpdateTime.IsZero() {
		statStruct.logger.Info("Updated first time")
		statStruct.firstUpdateTime = reqTime
	}
	statStruct.lastUpdateTime = reqTime

	// Update file stats
	curStats, inStats := statStruct.fileStats[filename]

	if !inStats {
		curStats = &FileStats{
			Filename:         filename,
			Size:             size,
			StatInsertTime:   reqTime,
			DeltaLastRequest: 0,
			Recency:          curTick,
			InCacheSinceTick: -1,
		}
		statStruct.fileStats[filename] = curStats
	} else {
		curStats.Size = size
		curStats.DeltaLastRequest = curTick - curStats.Recency
		curStats.Recency = curTick
	}

	// fmt.Println(curTick, curStats.Recency, curStats.DeltaLastRequest)

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
	// NumRequestedTimes represents the  number of slots
	NumRequestedTimes int = 32
	// NumDaysStatsDecay is the number of days that stats are maintained
	NumDaysStatsDecay = 7.0
	// NumDaysPointsDecay is the number of days that points are maintained
	NumDaysPointsDecay = 3.0
)

// FileStats contains file statistics collected by weight function caches
type FileStats struct {
	Filename           int64       `json:"filename"`
	FileType           int64       `json:"fileType"`
	DataType           int64       `json:"dataType"`
	Weight             float64     `json:"weight"`
	Size               float64     `json:"size"`
	Frequency          int64       `json:"frequency"`
	FrequencyInCache   int64       `json:"frequencyInCache"`
	Recency            int64       `json:"recency"`
	NHits              int64       `json:"nHits"`
	NMiss              int64       `json:"nMiss"`
	InCacheSinceTime   time.Time   `json:"inCacheSinceTime"`
	InCacheSinceTick   int64       `json:"inCacheSinceTick"`
	InCache            bool        `json:"inCache"`
	StatInsertTime     time.Time   `json:"statInsertTime"`
	StatLastUpdateTime time.Time   `json:"statLastUpdateTime"`
	RequestedTimesMean float64     `json:"requestedTimesMean"`
	RequestedTimes     []time.Time `json:"requestedTimes"`
	DeltaLastRequest   int64       `json:"deltaLastRequest"`
	idxLastRequest     int
}

// DiffLastUpdate returns the number of days from the last update stats
func (stats FileStats) DiffLastUpdate(curTime time.Time) float64 {
	return curTime.Sub(stats.StatLastUpdateTime).Hours() / numHoursInADay
}

func (stats *FileStats) addInCache(tick int64, curTime *time.Time) {
	if curTime != nil {
		stats.InCacheSinceTime = *curTime
	}
	stats.InCacheSinceTick = tick
	stats.InCache = true
}

func (stats *FileStats) removeFromCache() {
	stats.InCacheSinceTime = time.Time{}
	stats.InCache = false
	stats.InCacheSinceTick = -1
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

	stats.StatLastUpdateTime = curTime
	if len(stats.RequestedTimes) < NumRequestedTimes {
		stats.RequestedTimes = append(stats.RequestedTimes, time.Time{})
	}
	stats.RequestedTimes[stats.idxLastRequest] = curTime
	stats.idxLastRequest = (stats.idxLastRequest + 1) % NumRequestedTimes
}

func (stats *FileStats) updateWeight(functionType FunctionType, alpha float64, beta float64, gamma float64) float64 {
	stats.RequestedTimesMean = stats.getMeanReqTimes()

	switch functionType {
	case FuncAdditive:
		stats.Weight = fileAdditiveWeightFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
			alpha,
			beta,
			gamma,
		)
	case FuncAdditiveExp:
		stats.Weight = fileAdditiveExpWeightFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
			alpha,
			beta,
			gamma,
		)
	case FuncMultiplicative:
		stats.Weight = fileMultiplicativeWeightFunction(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
			alpha,
			beta,
			gamma,
		)
	case FuncWeightedRequests:
		stats.Weight = fileWeightedRequest(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
		)
	}

	return stats.Weight
}

func (stats FileStats) getMeanReqTimes() float64 {
	var timeDiffSum time.Duration

	for idx := 0; idx < len(stats.RequestedTimes); idx++ {
		if !stats.RequestedTimes[idx].IsZero() {
			timeDiffSum += stats.StatLastUpdateTime.Sub(stats.RequestedTimes[idx])
		}
	}

	if timeDiffSum != 0. {
		return timeDiffSum.Minutes() / float64(NumRequestedTimes)
	}

	return 0.
}
