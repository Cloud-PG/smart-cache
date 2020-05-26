package cache

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"go.uber.org/zap"
)

const (
	// MeanCPUDiffIT is the mean difference value of CPU efficiency in IT region
	// Extracted from 2018 stats (upper bound - lower bound)
	MeanCPUDiffIT = 19.
	// MeanCPUDiffUS is the mean difference value of CPU efficiency in US region
	// Extracted from 2018 stats (upper bound - lower bound)
	MeanCPUDiffUS = 10.
	// DailyBandwidth1Gbit is 1Gibt day bandwidth available
	DailyBandwidth1Gbit = (1000. / 8.) * 60. * 60. * 24.
)

// SimpleCache cache
type SimpleCache struct {
	stats                              Stats
	files                              Manager
	ordType                            queueType
	hit, miss, size, MaxSize           float64
	hitCPUEff, missCPUEff              float64
	upperCPUEff, lowerCPUEff           float64
	numLocal, numRemote                int64
	dataWritten, dataRead, dataDeleted float64
	dataReadOnHit, dataReadOnMiss      float64
	HighWaterMark                      float64
	LowWaterMark                       float64
	numDailyHit                        int64
	numDailyMiss                       int64
	prevTime                           time.Time
	curTime                            time.Time
	region                             string
	bandwidth                          float64
}

// Init the LRU struct
func (cache *SimpleCache) Init(vars ...interface{}) interface{} {
	if len(vars) == 0 {
		cache.ordType = LRUQueue
	} else {
		cache.ordType = vars[0].(queueType)
	}

	cache.stats.Init()
	cache.files.Init()

	if cache.HighWaterMark == 0.0 {
		cache.HighWaterMark = 95.0
	}
	if cache.LowWaterMark == 0.0 {
		cache.LowWaterMark = 75.0
	}

	if cache.HighWaterMark < cache.LowWaterMark {
		panic(fmt.Sprintf("High watermark is lower then Low waterrmark -> %f < %f", cache.HighWaterMark, cache.LowWaterMark))
	}

	return cache
}

// SetRegion initialize the region field
func (cache *SimpleCache) SetRegion(region string) {
	cache.region = region
}

// SetBandwidth initialize the bandwidth field
func (cache *SimpleCache) SetBandwidth(bandwidth float64) {
	cache.bandwidth = bandwidth * DailyBandwidth1Gbit
}

// ClearFiles remove the cache files
func (cache *SimpleCache) ClearFiles() {
	cache.files.Init()
	cache.stats.Clear()
	cache.size = 0.
}

// Clear the LRU struct
func (cache *SimpleCache) Clear() {
	cache.stats.Init()
	cache.ClearFiles()

	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
	cache.hitCPUEff = 0.
	cache.missCPUEff = 0.
	cache.upperCPUEff = 0.
	cache.lowerCPUEff = 0.
	cache.numLocal = 0
	cache.numRemote = 0
}

// ClearHitMissStats the cache stats
func (cache *SimpleCache) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
	cache.hitCPUEff = 0.
	cache.missCPUEff = 0.
	cache.upperCPUEff = 0.
	cache.lowerCPUEff = 0.
	cache.numLocal = 0
	cache.numRemote = 0
}

// Dumps the SimpleCache cache
func (cache *SimpleCache) Dumps(fileAndStats bool) [][]byte {
	logger.Info("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		logger.Info("Dump cache files")
		for file := range cache.files.Get(LRUQueue) {
			dumpInfo, _ := json.Marshal(DumpInfo{Type: "FILES"})
			dumpFile, _ := json.Marshal(file)
			record, _ := json.Marshal(DumpRecord{
				Info: string(dumpInfo),
				Data: string(dumpFile),
			})
			record = append(record, newLine...)
			outData = append(outData, record)
		}
	}
	return outData
}

// Dump the SimpleCache cache
func (cache *SimpleCache) Dump(filename string, fileAndStats bool) {
	logger.Info("Dump cache", zap.String("filename", filename))
	outFile, osErr := os.Create(filename)
	if osErr != nil {
		panic(fmt.Sprintf("Error dump file creation: %s", osErr))
	}
	gwriter := gzip.NewWriter(outFile)

	for _, record := range cache.Dumps(fileAndStats) {
		gwriter.Write(record)
	}

	gwriter.Close()
}

// Loads the SimpleCache cache
func (cache *SimpleCache) Loads(inputString [][]byte, _ ...interface{}) {
	logger.Info("Load cache dump string")
	var curRecord DumpRecord
	var curRecordInfo DumpInfo
	for _, record := range inputString {
		json.Unmarshal(record, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileSupportData
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files.Insert(curFile)
			cache.size += curFile.Size
		}
	}
}

// Load the SimpleCache cache
func (cache *SimpleCache) Load(filename string) [][]byte {
	logger.Info("Load cache Dump", zap.String("filename", filename))

	inFile, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Error dump file opening: %s", err))
	}
	greader, gzipErr := gzip.NewReader(inFile)
	if gzipErr != nil {
		panic(gzipErr)
	}

	var records [][]byte
	var buffer []byte
	var charBuffer []byte

	buffer = make([]byte, 0)
	charBuffer = make([]byte, 1)

	for {
		_, err := greader.Read(charBuffer)
		if err != nil {
			if err == io.EOF {
				if len(buffer) > 0 {
					newRecord := make([]byte, len(buffer))
					copy(newRecord, buffer)
					records = append(records, newRecord)
					buffer = buffer[:0]
				}
				break
			}
			panic(err)
		}
		if string(charBuffer) == "\n" {
			newRecord := make([]byte, len(buffer))
			copy(newRecord, buffer)
			records = append(records, newRecord)
			buffer = buffer[:0]
		} else {
			buffer = append(buffer, charBuffer...)
		}
	}
	greader.Close()

	return records
}

// BeforeRequest of LRU cache
func (cache *SimpleCache) BeforeRequest(request *Request, hit bool) *FileStats {
	cache.prevTime = cache.curTime
	cache.curTime = request.DayTime

	if !cache.curTime.Equal(cache.prevTime) {
		cache.numDailyHit = 0
		cache.numDailyMiss = 0
		cache.hitCPUEff = 0.
		cache.missCPUEff = 0.
		cache.upperCPUEff = 0.
		cache.lowerCPUEff = 0.
		cache.numLocal = 0
		cache.numRemote = 0
	}

	curStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime)
	curStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

	return curStats
}

// UpdatePolicy of LRU cache
func (cache *SimpleCache) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added             = false
		requestedFileSize = request.Size
	)

	if !hit {
		if cache.Size()+requestedFileSize > cache.MaxSize {
			cache.Free(requestedFileSize, false)
		}
		if cache.Size()+requestedFileSize <= cache.MaxSize {
			cache.size += requestedFileSize
			fileStats.addInCache(nil)

			cache.files.Insert(FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.FrequencyInCache,
				Recency:   fileStats.Recency,
			})

			added = true
		}
	} else {
		cache.files.Update(FileSupportData{
			Filename:  request.Filename,
			Size:      request.Size,
			Frequency: fileStats.FrequencyInCache,
			Recency:   fileStats.Recency,
		})
	}
	return added
}

// AfterRequest of LRU cache
func (cache *SimpleCache) AfterRequest(request *Request, hit bool, added bool) {

	var currentCPUEff float64

	if request.CPUEff != 0. {

		if request.Protocol == 1 {
			// Local
			cache.upperCPUEff += request.CPUEff
			cache.numLocal++
			currentCPUEff = request.CPUEff
		} else if request.Protocol == 0 {
			// Remote
			cache.lowerCPUEff += request.CPUEff
			cache.numRemote++
			currentCPUEff = request.CPUEff + cache.CPUEffBoundDiff()
		}
	}

	if hit {
		cache.numDailyHit++
		cache.hit += 1.
		cache.dataReadOnHit += request.Size
		cache.hitCPUEff += currentCPUEff
	} else {
		cache.numDailyMiss++
		cache.miss += 1.
		cache.dataReadOnMiss += request.Size
		if currentCPUEff != 0. {
			cache.missCPUEff += currentCPUEff - cache.CPUEffBoundDiff()
		}
	}

	// Always true because of LRU policy
	// - added variable is needed just for code consistency
	if added {
		cache.dataWritten += request.Size
	}
	cache.dataRead += request.Size

	if cache.stats.Dirty() {
		cache.stats.Purge()
	}
}

// Free removes files from the cache
func (cache *SimpleCache) Free(amount float64, percentage bool) float64 {
	logger.Debug(
		"Cache free",
		zap.Float64("mean size", cache.MeanSize()),
		zap.Float64("mean frequency", cache.MeanFrequency()),
		zap.Float64("mean recency", cache.MeanRecency()),
		zap.Int("num. files", cache.NumFiles()),
		zap.Float64("std.dev. freq.", cache.StdDevFreq()),
		zap.Float64("std.dev. rec.", cache.StdDevRec()),
		zap.Float64("std.dev. size", cache.StdDevSize()),
	)
	var (
		totalDeleted float64
		sizeToDelete float64
	)
	if percentage {
		sizeToDelete = amount * (cache.MaxSize / 100.)
	} else {
		sizeToDelete = amount
	}
	if sizeToDelete > 0. {
		deletedFiles := make([]int64, 0)
		for curFile := range cache.files.Get(cache.ordType) {
			logger.Debug("delete",
				zap.Int64("filename", curFile.Filename),
				zap.Float64("fileSize", curFile.Size),
				zap.Int64("frequency", curFile.Frequency),
				zap.Int64("recency", curFile.Recency),
				zap.Float64("cacheSize", cache.Size()),
			)

			curFileStats := cache.stats.Get(curFile.Filename)
			curFileStats.removeFromCache()

			// Update sizes
			cache.size -= curFile.Size
			cache.dataDeleted += curFile.Size
			totalDeleted += curFile.Size

			deletedFiles = append(deletedFiles, curFile.Filename)

			if totalDeleted >= sizeToDelete {
				break
			}
		}
		cache.files.Remove(deletedFiles)
	}
	return totalDeleted
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *SimpleCache) CheckWatermark() bool {
	ok := true
	if cache.Capacity() >= cache.HighWaterMark {
		ok = false
		cache.Free(
			cache.Capacity()-cache.LowWaterMark,
			true,
		)
	}
	return ok
}

// HitRate of the cache
func (cache *SimpleCache) HitRate() float64 {
	perc := (cache.hit / (cache.hit + cache.miss)) * 100.
	if math.IsNaN(float64(perc)) {
		return 0.0
	}
	return perc
}

// HitOverMiss of the cache
func (cache *SimpleCache) HitOverMiss() float64 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache *SimpleCache) WeightedHitRate() float64 {
	return cache.HitRate() * cache.dataReadOnHit
}

// Size of the cache
func (cache *SimpleCache) Size() float64 {
	return cache.size
}

// Capacity of the cache
func (cache *SimpleCache) Capacity() float64 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// BandwidthUsage of the cache
func (cache *SimpleCache) BandwidthUsage() float64 {
	return (cache.dataReadOnMiss / cache.bandwidth) * 100.
}

// DataWritten of the cache
func (cache *SimpleCache) DataWritten() float64 {
	return cache.dataWritten
}

// DataRead of the cache
func (cache *SimpleCache) DataRead() float64 {
	return cache.dataRead
}

// DataReadOnHit of the cache
func (cache *SimpleCache) DataReadOnHit() float64 {
	return cache.dataReadOnHit
}

// DataReadOnMiss of the cache
func (cache *SimpleCache) DataReadOnMiss() float64 {
	return cache.dataReadOnMiss
}

// DataDeleted of the cache
func (cache *SimpleCache) DataDeleted() float64 {
	return cache.dataDeleted
}

// Check returns if a file is in cache or not
func (cache *SimpleCache) Check(key int64) bool {
	return cache.files.Check(key)
}

// ExtraStats for output
func (cache *SimpleCache) ExtraStats() string {
	return "NONE"
}

// ExtraOutput for output specific information
func (cache *SimpleCache) ExtraOutput(info string) string {
	return "NONE"
}

// CPUEff returns the CPU efficiency
func (cache *SimpleCache) CPUEff() float64 {
	return (cache.hitCPUEff + cache.missCPUEff) / float64(cache.numDailyHit+cache.numDailyMiss)
}

// CPUHitEff returns the CPU efficiency for hit data
func (cache *SimpleCache) CPUHitEff() float64 {
	return cache.hitCPUEff / float64(cache.numDailyHit)
}

// CPUMissEff returns the CPU efficiency for miss data
func (cache *SimpleCache) CPUMissEff() float64 {
	return cache.missCPUEff / float64(cache.numDailyMiss)
}

// CPUEffUpperBound returns the ideal CPU efficiency upper bound
func (cache *SimpleCache) CPUEffUpperBound() float64 {
	return cache.upperCPUEff / float64(cache.numLocal)
}

// CPUEffLowerBound returns the ideal CPU efficiency lower bound
func (cache *SimpleCache) CPUEffLowerBound() float64 {
	return cache.lowerCPUEff / float64(cache.numRemote)
}

// CPUEffBoundDiff returns the ideal CPU efficiency bound difference
func (cache *SimpleCache) CPUEffBoundDiff() float64 {
	diffValue := 0.
	if len(cache.region) == 0 {
		diff := cache.CPUEffUpperBound() - cache.CPUEffLowerBound()
		if !math.IsNaN(diff) && diff > 0. {
			diffValue = diff
		}
	} else {
		switch cache.region {
		case "it":
			diffValue = MeanCPUDiffIT
		case "us":
			diffValue = MeanCPUDiffUS
		}
	}
	return diffValue
}

// MeanSize returns the average size of the files in cache
func (cache *SimpleCache) MeanSize() float64 {
	return cache.files.SizeSum / float64(cache.files.Len())
}

// MeanFrequency returns the average frequency of the files in cache
func (cache *SimpleCache) MeanFrequency() float64 {
	return cache.files.FrequencySum / float64(cache.files.Len())
}

// MeanRecency returns the average recency of the files in cache
func (cache *SimpleCache) MeanRecency() float64 {
	totRecency := 0.0
	curTick := float64(cache.stats.Tick)
	for file := range cache.files.Get(NoQueue) {
		totRecency += (curTick - float64(file.Recency))
	}
	return totRecency / float64(cache.files.Len())
}

// NumFiles returns the number of files in cache
func (cache *SimpleCache) NumFiles() int {
	return cache.files.Len()
}

// StdDevFreq returns the standard deviation of the frequency
func (cache *SimpleCache) StdDevFreq() float64 {
	mean := cache.MeanFrequency()
	sum := 0.0
	for file := range cache.files.Get(NoQueue) {
		sum += math.Pow(float64(file.Frequency)-mean, 2)
	}
	return math.Sqrt(sum / (float64(cache.files.Len()) - 1.0))
}

// StdDevRec returns the standard deviation of the recency
func (cache *SimpleCache) StdDevRec() float64 {
	mean := cache.MeanRecency()
	sum := 0.0
	for file := range cache.files.Get(NoQueue) {
		sum += math.Pow(float64(file.Recency)-mean, 2)
	}
	return math.Sqrt(sum / (float64(cache.files.Len()) - 1.0))
}

// StdDevSize returns the standard deviation of the size
func (cache *SimpleCache) StdDevSize() float64 {
	mean := cache.MeanSize()
	sum := 0.0
	for file := range cache.files.Get(NoQueue) {
		sum += math.Pow(file.Size-mean, 2)
	}
	return math.Sqrt(sum / (float64(cache.files.Len()) - 1.0))
}
