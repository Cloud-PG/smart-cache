package cache

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	"simulator/v2/cache/files"
	"simulator/v2/cache/queue"
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
	// ChoiceLogBufferDim is the max dimension of the buffer to store choices
	ChoiceLogBufferDim = 9999
	// ChoiceAdd string add action
	ChoiceAdd = "ADD"
	// ChoiceDelete string remove action
	ChoiceDelete = "DELETE"
	// ChoiceFree string free event
	ChoiceFree = "FREE"
	// ChoiceSkip string skip action
	ChoiceSkip = "SKIP"
	// ChoiceKeep string keep action
	ChoiceKeep = "KEEP"
	// ChoiceRedirect string redirect action
	ChoiceRedirect = "REDIRECT"
	// LogEventHit string hit event
	LogEventHit = "HIT"
	// LogEventMiss string miss event
	LogEventMiss = "MISS"
)

const (
	logBufferLen        = 24
	logBufferExitString = "EXIT"
)

var (
	logHeader = []string{
		"tick",
		"action or event",
		"cache size",
		"cache capacity",
		"filename",
		"size",
		"num req",
		"delta t",
	}
)

// SimpleCache cache
type SimpleCache struct {
	stats                              files.Manager
	files                              queue.Queue
	ordType                            queue.QueueType
	canRedirect                        bool
	useWatermarks                      bool
	logSimulation                      bool
	calcWeight                         bool
	hit, miss, size, MaxSize           float64
	hitCPUEff, missCPUEff              float64
	upperCPUEff, lowerCPUEff           float64
	numReq, numRedirected              int64
	numAdded, numDeleted               int64
	numLocal, numRemote                int64
	dataWritten, dataRead, dataDeleted float64
	dataReadOnHit, dataReadOnMiss      float64
	dailyfreeSpace                     []float64
	sumDailyFreeSpace                  float64
	highWatermark                      float64
	lowWatermark                       float64
	numDailyHit                        int64
	numDailyMiss                       int64
	prevTime                           time.Time
	curTime                            time.Time
	region                             string
	bandwidth                          float64
	redirectSize                       float64
	tick                               int64
	logFile                            *OutputCSV
	logBuffer                          chan []string
	logClose                           chan bool
	maxNumDayDiff                      float64
	deltaDaysStep                      float64
}

// Init the LRU struct
func (cache *SimpleCache) Init(param InitParameters) interface{} {
	cache.ordType = param.QueueType
	cache.logSimulation = param.Log
	cache.canRedirect = param.RedirectReq
	cache.useWatermarks = param.Watermarks
	cache.highWatermark = param.HighWatermark
	cache.lowWatermark = param.LowWatermark
	cache.maxNumDayDiff = param.MaxNumDayDiff
	cache.deltaDaysStep = param.DeltaDaysStep
	cache.calcWeight = param.CalcWeight

	cache.stats.Init(cache.maxNumDayDiff, cache.deltaDaysStep, cache.calcWeight)

	// cache.files.Init(cache.ordType)
	switch param.QueueType {
	case queue.LRUQueue:
		cache.files = &queue.LRU{}
	case queue.LFUQueue:
		cache.files = &queue.LFU{}
	case queue.SizeBigQueue:
		cache.files = &queue.SizeBig{}
	case queue.SizeSmallQueue:
		cache.files = &queue.SizeSmall{}
	case queue.WeightQueue:
		cache.files = &queue.Weighted{}
	case queue.NoQueue:
		cache.files = &queue.QueueNone{}
	default:
		panic(fmt.Errorf("type %d not implemented...", param.QueueType))
	}
	queue.Init(cache.files)

	cache.dailyfreeSpace = make([]float64, 0)

	if cache.highWatermark < cache.lowWatermark {
		panic(fmt.Sprintf("High watermark is lower then Low waterrmark -> %f < %f", cache.highWatermark, cache.lowWatermark))
	}

	if cache.logSimulation {
		cache.logFile = &OutputCSV{}
		cache.logFile.Create("simulationLogFile.csv", true)
		cache.logFile.Write(logHeader)
		cache.logBuffer = make(chan []string, logBufferLen)
		cache.logClose = make(chan bool)

		go func(file *OutputCSV, stream chan []string, done chan bool) {
			for {
				curLine := <-stream
				if len(curLine) == 1 && curLine[0] == logBufferExitString {
					break
				}

				file.Write(curLine)
			}

			file.Close()
			close(stream)
			done <- true

		}(cache.logFile, cache.logBuffer, cache.logClose)
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
	queue.Init(cache.files)
	cache.stats.Clear()
	cache.size = 0.
}

// Clear the LRU struct
func (cache *SimpleCache) Clear() {
	cache.stats.Init(cache.maxNumDayDiff, cache.deltaDaysStep, cache.calcWeight)
	cache.ClearFiles()
	cache.ClearStats()
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
	cache.numDailyHit = 0
	cache.numDailyMiss = 0
	cache.numReq = 0
	cache.numAdded = 0
	cache.numDeleted = 0
	cache.numRedirected = 0
	cache.numLocal = 0
	cache.numRemote = 0
	cache.redirectSize = 0.
}

// Dumps the SimpleCache cache
func (cache *SimpleCache) Dumps(fileAndStats bool) [][]byte {
	log.Info().Msg("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		log.Info().Msg("Dump cache files")
		for _, file := range queue.GetFromWorst(cache.files) {
			dumpInfo, _ := json.Marshal(DumpInfo{Type: "FILES"})
			dumpFile, _ := json.Marshal(file)
			record, _ := json.Marshal(DumpRecord{
				Info: string(dumpInfo),
				Data: string(dumpFile),
			})
			record = append(record, newLine...)
			outData = append(outData, record)
		}
		// ----- Stats -----
		log.Info().Msg("Dump cache stats")
		for filename, stats := range cache.stats.Data {
			dumpInfo, _ := json.Marshal(DumpInfo{Type: "STATS"})
			dumpStats, _ := json.Marshal(stats)
			record, _ := json.Marshal(DumpRecord{
				Info:     string(dumpInfo),
				Data:     string(dumpStats),
				Filename: filename,
			})
			record = append(record, newLine...)
			outData = append(outData, record)
		}
	}
	return outData
}

// Dump the SimpleCache cache
func (cache *SimpleCache) Dump(filename string, fileAndStats bool) {
	log.Info().Str("filename", filename).Msg("Dump cache")
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
	log.Info().Msg("Load cache dump string")

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
			var curFileStats files.Stats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			queue.Insert(cache.files, &curFileStats)
			if unmarshalErr != nil {
				panic(unmarshalErr)
			}
			cache.size += curFileStats.Size
			cache.stats.Data[curRecord.Filename] = &curFileStats
		case "STATS":
			var curFileStats files.Stats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			if unmarshalErr != nil {
				panic(unmarshalErr)
			}
			if _, inStats := cache.stats.Data[curRecord.Filename]; !inStats {
				cache.stats.Data[curRecord.Filename] = &curFileStats
			}
		}
	}
}

// Load the SimpleCache cache
func (cache *SimpleCache) Load(filename string) [][]byte {
	log.Info().Str("filename", filename).Msg("Load cache Dump")

	inFile, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Error dump file opening: %s", err))
	}
	greader, gzipErr := gzip.NewReader(inFile)
	if gzipErr != nil {
		panic(gzipErr)
	}

	var (
		records    [][]byte
		buffer     []byte
		charBuffer []byte
	)

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
func (cache *SimpleCache) BeforeRequest(request *Request, hit bool) (*files.Stats, bool) {
	// cache.prevTime = cache.curTime
	// cache.curTime = request.DayTime
	// if !cache.curTime.Equal(cache.prevTime) {}
	cache.numReq++

	curStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)
	curStats.UpdateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

	return curStats, hit
}

// UpdatePolicy of LRU cache
func (cache *SimpleCache) UpdatePolicy(request *Request, fileStats *files.Stats, hit bool) bool {
	var (
		added             = false
		requestedFileSize = request.Size
	)

	if !hit { //nolint:ignore,nestif
		if cache.Size()+requestedFileSize > cache.MaxSize {
			cache.Free(requestedFileSize, false)
		}

		if cache.Size()+requestedFileSize <= cache.MaxSize {
			cache.size += requestedFileSize

			queue.Insert(cache.files, fileStats)

			if cache.logFile != nil {
				cache.toLogBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					ChoiceAdd,
					fmt.Sprintf("%0.2f", cache.size),
					fmt.Sprintf("%0.2f", cache.Capacity()),
					fmt.Sprintf("%d", fileStats.Filename),
					fmt.Sprintf("%0.2f", fileStats.Size),
					fmt.Sprintf("%d", fileStats.Frequency),
					fmt.Sprintf("%d", fileStats.DeltaLastRequest),
				})
			}

			added = true
		}
	} else {
		queue.Update(cache.files, fileStats)
	}

	return added
}

// AfterRequest of LRU cache
func (cache *SimpleCache) AfterRequest(request *Request, fileStats *files.Stats, hit bool, added bool) {
	var currentCPUEff float64

	if request.CPUEff > 0. {
		switch request.Protocol {
		case 1:
			// Local
			cache.upperCPUEff += request.CPUEff
			cache.numLocal++
			currentCPUEff = request.CPUEff
		case 0:
			// Remote
			cache.lowerCPUEff += request.CPUEff
			cache.numRemote++
			currentCPUEff = request.CPUEff + cache.CPUEffBoundDiff()
		default:
			panic(fmt.Sprintf("%d is not a valid request Protocol", request.Protocol))
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
		cache.stats.Purge(cache.files.Check)
	}

	cache.tick++
}

// Free removes files from the cache
func (cache *SimpleCache) Free(amount float64, percentage bool) float64 { // nolint:ignore,funlen
	// log.Debug(
	// 	"Cache free",
	// 	zap.Float64("mean size", cache.MeanSize()),
	// 	zap.Float64("mean frequency", cache.MeanFrequency()),
	// 	zap.Float64("mean recency", cache.MeanRecency()),
	// 	zap.Int("num. files", cache.NumFiles()),
	// )
	cache.stats.ClearDeletedFileMiss()

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
		if cache.logFile != nil {
			cache.toLogBuffer([]string{
				fmt.Sprintf("%d", cache.tick),
				ChoiceFree,
				fmt.Sprintf("%0.2f", cache.size),
				fmt.Sprintf("%0.2f", cache.Capacity()),
				fmt.Sprintf("%d", -1),
				fmt.Sprintf("%0.2f", -1.),
				fmt.Sprintf("%d", -1),
				fmt.Sprintf("%d", -1),
			})
		}

		deletedFiles := make([]int64, 0)
		for _, curFile := range queue.GetWorstFilesUp2Size(cache.files, sizeToDelete) {
			log.Debug().Int64("filename",
				curFile.Filename).Float64("fileSize",
				curFile.Size).Int64("frequency",
				curFile.Frequency).Int64("recency",
				curFile.Recency).Float64("cacheSize",
				cache.Size()).Msg("delete")

			curFileStats := cache.stats.Get(curFile.Filename)
			if curFileStats != curFile {
				panic("ERROR: file stats is not the same...")
			}

			// Update sizes
			cache.size -= curFile.Size
			cache.dataDeleted += curFile.Size
			totalDeleted += curFile.Size

			deletedFiles = append(deletedFiles, curFile.Filename)

			cache.stats.AddDeletedFileMiss(curFile.Filename)

			if cache.logFile != nil {
				cache.toLogBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					ChoiceDelete,
					fmt.Sprintf("%0.2f", cache.size),
					fmt.Sprintf("%0.2f", cache.Capacity()),
					fmt.Sprintf("%d", curFile.Filename),
					fmt.Sprintf("%0.2f", curFile.Size),
					fmt.Sprintf("%d", curFile.Frequency),
					fmt.Sprintf("%d", curFile.DeltaLastRequest),
				})
			}

			cache.numDeleted++
		}

		queue.RemoveWorst(cache.files, deletedFiles)
	}
	return totalDeleted
}

// CheckRedirect checks the cache can redirect requests on miss
func (cache *SimpleCache) CheckRedirect(filename int64, size float64) bool {
	redirect := false

	if cache.canRedirect {
		if cache.BandwidthUsage() >= 95. {
			redirect = true
			cache.numRedirected++
			cache.redirectSize += size

			if cache.logFile != nil {
				cache.toLogBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					ChoiceRedirect,
					fmt.Sprintf("%0.2f", cache.size),
					fmt.Sprintf("%0.2f", cache.Capacity()),
					fmt.Sprintf("%d", filename),
					fmt.Sprintf("%0.2f", size),
					fmt.Sprintf("%d", -1),
					fmt.Sprintf("%d", -1),
				})
			}
		}
	}

	return redirect
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *SimpleCache) CheckWatermark() bool {
	ok := true

	if cache.useWatermarks {
		// fmt.Println("CHECK WATERMARKS")
		if cache.Capacity() >= cache.highWatermark {
			ok = false

			cache.Free(
				cache.Capacity()-cache.lowWatermark,
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

// WeightedHitRate of the cache
func (cache *SimpleCache) WeightedHitRate() float64 {
	return cache.HitRate() * cache.dataReadOnHit
}

// Size of the cache
func (cache *SimpleCache) Size() float64 {
	return cache.size
}

// GetMaxSize of the cache
func (cache *SimpleCache) GetMaxSize() float64 {
	return cache.MaxSize
}

// Capacity of the cache
func (cache *SimpleCache) Capacity() float64 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// Bandwidth of the cache
func (cache *SimpleCache) Bandwidth() float64 {
	return cache.bandwidth
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
func (cache *SimpleCache) Check(filename int64) bool {
	hit := queue.Check(cache.files, filename)

	if !hit {
		cache.stats.IncDeletedFileMiss(filename)
	}

	if cache.logFile != nil {
		event := LogEventMiss

		if hit {
			event = LogEventHit
		}

		cache.toLogBuffer([]string{
			fmt.Sprintf("%d", cache.tick),
			event,
			fmt.Sprintf("%0.2f", cache.size),
			fmt.Sprintf("%0.2f", cache.Capacity()),
			fmt.Sprintf("%d", filename),
			fmt.Sprintf("%0.2f", -1.),
			fmt.Sprintf("%d", -1),
			fmt.Sprintf("%d", -1),
		})
	}

	return hit
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
func (cache *SimpleCache) CPUEff() (result float64) {
	val := (cache.hitCPUEff + cache.missCPUEff) / float64(cache.numDailyHit+cache.numDailyMiss)

	if !math.IsNaN(val) {
		result = val
	}

	return result
}

// CPUHitEff returns the CPU efficiency for hit data
func (cache *SimpleCache) CPUHitEff() (result float64) {
	val := cache.hitCPUEff / float64(cache.numDailyHit)

	if !math.IsNaN(val) {
		result = val
	}

	return result
}

// CPUMissEff returns the CPU efficiency for miss data
func (cache *SimpleCache) CPUMissEff() (result float64) {
	val := cache.missCPUEff / float64(cache.numDailyMiss)

	if !math.IsNaN(val) {
		result = val
	}

	return result
}

// CPUEffUpperBound returns the ideal CPU efficiency upper bound
func (cache *SimpleCache) CPUEffUpperBound() (result float64) {
	log.Debug().Float64("upperCPUEff", cache.upperCPUEff).Int64("numLocal", cache.numLocal).Msg("UPPER BOUND FUNCTIONS")

	val := cache.upperCPUEff / float64(cache.numLocal)

	if !math.IsNaN(val) {
		result = val
	}

	return result
}

// CPUEffLowerBound returns the ideal CPU efficiency lower bound
func (cache *SimpleCache) CPUEffLowerBound() (result float64) {
	log.Debug().Float64("lowerCPUEff", cache.lowerCPUEff).Int64("numRemote", cache.numRemote).Msg("LOWER BOUND FUNCTIONS")

	val := cache.lowerCPUEff / float64(cache.numRemote)

	if !math.IsNaN(val) {
		result = val
	}

	return result
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
	return queue.Len(cache.files)
}

// AvgFreeSpace returns the average free space of the cache
func (cache *SimpleCache) AvgFreeSpace() (result float64) {
	val := cache.sumDailyFreeSpace / float64(len(cache.dailyfreeSpace))
	if !math.IsNaN(val) {
		result = val
	}

	return result
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

// RedirectedSize returns the size of all redirected files
func (cache *SimpleCache) RedirectedSize() float64 {
	return cache.redirectSize
}

// NumAdded returns the # of Added files
func (cache *SimpleCache) NumAdded() int64 {
	return cache.numAdded
}

// NumDeleted returns the # of Added files
func (cache *SimpleCache) NumDeleted() int64 {
	return cache.numDeleted
}

// NumHits returns the # of Added files
func (cache *SimpleCache) NumHits() int64 {
	return int64(cache.hit)
}

func (cache *SimpleCache) toLogBuffer(curChoice []string) {
	cache.logBuffer <- curChoice
}

func (cache *SimpleCache) flushAndCloseLog() {
	cache.logBuffer <- []string{logBufferExitString}
	<-cache.logClose
	close(cache.logClose)
}

// Terminate close all pending things of the cache
func (cache *SimpleCache) Terminate() error {
	if cache.logFile != nil {
		cache.flushAndCloseLog()
	}

	return nil
}

func (cache *SimpleCache) GetTotDeletedFileMiss() int {
	return cache.stats.GetTotDeletedFileMiss()
}
