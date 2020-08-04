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
	numReq, numAdded, numRedirected    int64
	numLocal, numRemote                int64
	dataWritten, dataRead, dataDeleted float64
	dataReadOnHit, dataReadOnMiss      float64
	dailyfreeSpace                     []float64
	sumDailyFreeSpace                  float64
	HighWaterMark                      float64
	LowWaterMark                       float64
	numDailyHit                        int64
	numDailyMiss                       int64
	prevTime                           time.Time
	curTime                            time.Time
	region                             string
	bandwidth                          float64
	tick                               int64
	canRedirect                        bool
	useWatermarks                      bool
}

// Init the LRU struct
func (cache *SimpleCache) Init(vars ...interface{}) interface{} {
	cache.ordType = vars[0].(queueType)
	cache.canRedirect = vars[1].(bool)
	cache.useWatermarks = vars[2].(bool)

	cache.stats.Init()
	cache.files.Init(cache.ordType)

	cache.dailyfreeSpace = make([]float64, 0)

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
	cache.files.Init(cache.ordType)
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
	cache.dailyfreeSpace = cache.dailyfreeSpace[:0]
	cache.sumDailyFreeSpace = 0.
	cache.hitCPUEff = 0.
	cache.missCPUEff = 0.
	cache.upperCPUEff = 0.
	cache.lowerCPUEff = 0.
	cache.numReq = 0
	cache.numAdded = 0
	cache.numRedirected = 0
	cache.numLocal = 0
	cache.numRemote = 0
	cache.tick = 0
}

// ClearStats the cache stats
func (cache *SimpleCache) ClearStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
	cache.dailyfreeSpace = cache.dailyfreeSpace[:0]
	cache.sumDailyFreeSpace = 0.
	cache.hitCPUEff = 0.
	cache.missCPUEff = 0.
	cache.upperCPUEff = 0.
	cache.lowerCPUEff = 0.
	cache.numReq = 0
	cache.numAdded = 0
	cache.numRedirected = 0
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
		for _, file := range cache.files.Get() {
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
		_, writeErr := gwriter.Write(record)
		if writeErr != nil {
			panic(writeErr)
		}
	}

	closeDumpErr := gwriter.Close()
	if closeDumpErr != nil {
		panic(closeDumpErr)
	}
}

// Loads the SimpleCache cache
func (cache *SimpleCache) Loads(inputString [][]byte, _ ...interface{}) {
	logger.Info("Load cache dump string")
	var (
		curRecord     DumpRecord
		curRecordInfo DumpInfo
		unmarshalErr  error
	)
	for _, record := range inputString {
		unmarshalErr = json.Unmarshal(record, &curRecord)
		if unmarshalErr != nil {
			panic(unmarshalErr)
		}
		unmarshalErr = json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		if unmarshalErr != nil {
			panic(unmarshalErr)
		}
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileSupportData
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFile)
			if unmarshalErr != nil {
				panic(unmarshalErr)
			}
			cache.files.Insert(&curFile)
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
				}
				break
			}
			panic(err)
		}
		if string(charBuffer) == "\n" {
			newRecord := make([]byte, len(buffer))
			copy(newRecord, buffer)
			records = append(records, newRecord)
			buffer = make([]byte, 0)
		} else {
			buffer = append(buffer, charBuffer...)
		}
	}
	closeErr := greader.Close()
	if closeErr != nil {
		panic(closeErr)
	}

	return records
}

// BeforeRequest of LRU cache
func (cache *SimpleCache) BeforeRequest(request *Request, hit bool) (*FileStats, bool) {
	// cache.prevTime = cache.curTime
	// cache.curTime = request.DayTime
	// if !cache.curTime.Equal(cache.prevTime) {}

	cache.numReq++

	curStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)
	curStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

	return curStats, hit
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
			fileStats.addInCache(cache.tick, nil)

			cache.files.Insert(&FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.FrequencyInCache,
				Recency:   fileStats.Recency,
			})

			added = true
		}
	} else {
		cache.files.Update(&FileSupportData{
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
		cache.numAdded++
	}
	cache.dataRead += request.Size

	freeSpace := cache.MaxSize - cache.size
	cache.dailyfreeSpace = append(cache.dailyfreeSpace, freeSpace)
	cache.sumDailyFreeSpace += freeSpace

	if cache.stats.Dirty() {
		cache.stats.Purge()
	}

	cache.tick++
}

// Free removes files from the cache
func (cache *SimpleCache) Free(amount float64, percentage bool) float64 {
	// TODO: remove all means and StdDev
	// logger.Debug(
	// 	"Cache free",
	// 	zap.Float64("mean size", cache.MeanSize()),
	// 	zap.Float64("mean frequency", cache.MeanFrequency()),
	// 	zap.Float64("mean recency", cache.MeanRecency()),
	// 	zap.Int("num. files", cache.NumFiles()),
	// )
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
		for _, curFile := range cache.files.Get(sizeToDelete) {
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
		}
		cache.files.Remove(deletedFiles, false)
	}
	return totalDeleted
}

// CheckRedirect checks the cache can redirect requests on miss
func (cache *SimpleCache) CheckRedirect() bool {
	redirect := false
	if cache.canRedirect {
		if cache.BandwidthUsage() >= 95. {
			redirect = true
			cache.numRedirected++
		}
	}
	return redirect
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *SimpleCache) CheckWatermark() bool {
	ok := true
	if cache.useWatermarks {
		// fmt.Println("CHECK WATERMARKS")
		if cache.Occupancy() >= cache.HighWaterMark {
			ok = false
			cache.Free(
				cache.Occupancy()-cache.LowWaterMark,
				true,
			)
		}
	}
	return ok
}

// HitRate of the cache
func (cache *SimpleCache) HitRate() float64 {
	perc := (cache.hit / (cache.hit + cache.miss)) * 100.
	if math.IsNaN(perc) {
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

// Occupancy of the cache
func (cache *SimpleCache) Occupancy() float64 {
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

// NumFiles returns the number of files in cache
func (cache *SimpleCache) NumFiles() int {
	return cache.files.Len()
}

// AvgFreeSpace returns the average free space of the cache
func (cache *SimpleCache) AvgFreeSpace() float64 {
	return cache.sumDailyFreeSpace / float64(len(cache.dailyfreeSpace))
}

// StdDevFreeSpace returns the standard deviation of the free space of the cache
func (cache *SimpleCache) StdDevFreeSpace() float64 {
	mean := cache.AvgFreeSpace()
	var sum float64
	for _, value := range cache.dailyfreeSpace {
		curDiff := value - mean
		sum += curDiff * curDiff
	}
	return math.Sqrt(sum / float64(len(cache.dailyfreeSpace)-1))
}

// NumRequests returns the # of requested files
func (cache *SimpleCache) NumRequests() int64 {
	return cache.numReq
}

// NumRedirected returns the # of redirected files
func (cache *SimpleCache) NumRedirected() int64 {
	return cache.numRedirected
}

// NumAdded returns the # of Added files
func (cache *SimpleCache) NumAdded() int64 {
	return cache.numAdded
}

// NumHits returns the # of Added files
func (cache *SimpleCache) NumHits() int64 {
	return int64(cache.hit)
}
