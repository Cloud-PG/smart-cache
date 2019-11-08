package cache

import (
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// WeightedLRU cache
type WeightedLRU struct {
	files                              map[string]float32
	fileWeights                        []float32
	stats                              []*WeightedFileStats
	statsFilenames                     map[string]int
	queue                              *list.List
	hit, miss, size, MaxSize, Exp      float32
	dataWritten, dataRead, dataDeleted float32
	dataReadOnHit, dataReadOnMiss      float32
	SelFunctionType                    FunctionType
	SelUpdateStatPolicyType            UpdateStatsPolicyType
	SelLimitStatsPolicyType            LimitStatsPolicyType
	lastFileHitted                     bool
	lastFileAdded                      bool
	lastFileName                       string
}

// Init the WeightedLRU struct
func (cache *WeightedLRU) Init(_ ...interface{}) interface{} {
	cache.files = make(map[string]float32)
	cache.fileWeights = make([]float32, 0)
	cache.stats = make([]*WeightedFileStats, 0)
	cache.statsFilenames = make(map[string]int, 0)
	cache.queue = list.New()
	return cache
}

// ClearFiles remove the cache files
func (cache *WeightedLRU) ClearFiles() {
	cache.files = make(map[string]float32)
	cache.size = 0.
}

// Clear the WeightedLRU struct
func (cache *WeightedLRU) Clear() {
	cache.ClearFiles()
	cache.fileWeights = make([]float32, 0)
	cache.stats = make([]*WeightedFileStats, 0)
	cache.statsFilenames = make(map[string]int, 0)
	tmpVal := cache.queue.Front()
	for {
		if tmpVal == nil {
			break
		} else if tmpVal.Next() == nil {
			cache.queue.Remove(tmpVal)
			break
		}
		tmpVal = tmpVal.Next()
		cache.queue.Remove(tmpVal.Prev())
	}
	cache.queue = list.New()
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
}

// ClearHitMissStats the cache stats
func (cache *WeightedLRU) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
}

// Dumps the WeightedLRU cache
func (cache *WeightedLRU) Dumps() *[][]byte {
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	// Files
	for filename, size := range cache.files {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "FILES"})
		dumpFile, _ := json.Marshal(FileDump{
			Filename: filename,
			Size:     size,
		})
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpFile),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	// Stats
	for _, stats := range cache.stats {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "STATS"})
		dumpStats, _ := json.Marshal(stats)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	return &outData
}

// Dump the WeightedLRU cache
func (cache *WeightedLRU) Dump(filename string) {
	outFile, osErr := os.Create(filename)
	if osErr != nil {
		panic(fmt.Sprintf("Error dump file creation: %s", osErr))
	}
	gwriter := gzip.NewWriter(outFile)

	for _, record := range *cache.Dumps() {
		gwriter.Write(record)
	}

	gwriter.Close()
}

// Loads the WeightedLRU cache
func (cache *WeightedLRU) Loads(inputString *[][]byte) {
	var curRecord DumpRecord
	var curRecordInfo DumpInfo

	for _, record := range *inputString {
		buffer := record[:len(record)-1]
		json.Unmarshal(buffer, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileDump
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files[curFile.Filename] = curFile.Size
			cache.size += curFile.Size
		case "STATS":
			var curStats WeightedFileStats
			json.Unmarshal([]byte(curRecord.Data), &curStats)
			cache.fileWeights = append(cache.fileWeights, curStats.Weight)
			cache.stats = append(cache.stats, &curStats)
		}
	}

	cache.reIndex()
}

// Load the WeightedLRU cache
func (cache *WeightedLRU) Load(filename string) {
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

	records = make([][]byte, 0)
	buffer = make([]byte, 0)
	charBuffer = make([]byte, 1)

	for {
		curChar, err := greader.Read(charBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		if string(curChar) == "\n" {
			records = append(records, buffer)
			buffer = buffer[:0]
		} else {
			buffer = append(buffer, charBuffer...)
		}
	}
	greader.Close()

	cache.Loads(&records)
}

// SimGet updates the cache from a protobuf message
func (cache *WeightedLRU) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimClear deletes all cache content
func (cache *WeightedLRU) SimClear(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearFiles deletes all cache content
func (cache *WeightedLRU) SimClearFiles(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearFiles()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearHitMissStats deletes all cache content
func (cache *WeightedLRU) SimClearHitMissStats(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearHitMissStats()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimGetInfoCacheStatus returns the current simulation status
func (cache *WeightedLRU) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimDumps returns the content of the cache
func (cache *WeightedLRU) SimDumps(_ *empty.Empty, stream pb.SimService_SimDumpsServer) error {
	for _, record := range *cache.Dumps() {
		curRecord := &pb.SimDumpRecord{
			Raw: record,
		}
		if err := stream.Send(curRecord); err != nil {
			return err
		}
	}
	return nil
}

// SimLoads loads a cache state
func (cache *WeightedLRU) SimLoads(stream pb.SimService_SimLoadsServer) error {
	var records [][]byte
	records = make([][]byte, 0)

	for {
		record, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		records = append(records, record.Raw)
	}

	cache.Loads(&records)

	return nil
}

func (cache *WeightedLRU) reIndex(ranges ...int) {
	var (
		start = 0
		stop  = len(cache.stats)
	)
	if len(ranges) == 1 {
		start = ranges[0]
	} else if len(ranges) == 2 {
		start = ranges[0]
		stop = ranges[1] + 1
	}
	for curIdx := start; curIdx < stop; curIdx++ {
		curStatFilename := cache.stats[curIdx].Filename
		cache.statsFilenames[curStatFilename] = curIdx
	}
}

func (cache *WeightedLRU) getThreshold() float32 {
	if len(cache.fileWeights) == 0 {
		return 0.0
	}

	Q2 := cache.fileWeights[int(math.Floor(float64(0.5*float32(len(cache.fileWeights)))))]

	if cache.Capacity() > 75. {
		if cache.SelLimitStatsPolicyType == Q1IsDoubleQ2LimitStats {
			Q1Idx := int(math.Floor(float64(0.25 * float32(len(cache.fileWeights)))))
			Q1 := cache.fileWeights[Q1Idx]
			if Q1 > 2*Q2 {
				for idx := 0; idx < Q1Idx; idx++ {
					delete(cache.statsFilenames, cache.stats[idx].Filename)
				}
				copy(cache.stats, cache.stats[Q1Idx:])
				copy(cache.fileWeights, cache.fileWeights[Q1Idx:])
				cache.stats = cache.stats[:len(cache.stats)-Q1Idx]
				cache.fileWeights = cache.fileWeights[:len(cache.fileWeights)-Q1Idx]
				// Force to reindex
				cache.reIndex()
			}
		}
	}

	return Q2
}

func (cache *WeightedLRU) getOrInsertStats(filename string) (int, *WeightedFileStats) {
	var (
		resultIdx int
		stats     *WeightedFileStats
	)

	idx, inStats := cache.statsFilenames[filename]

	if !inStats {
		cache.stats = append(cache.stats, &WeightedFileStats{
			Filename:          filename,
			Weight:            0.,
			Size:              0.,
			TotRequests:       0,
			NHits:             0,
			NMiss:             0,
			LastTimeRequested: time.Time{},
			RequestTicksMean:  0.,
			RequestTicks:      [StatsMemorySize]time.Time{},
			RequestLastIdx:    0,
		})
		resultIdx = len(cache.stats) - 1
		stats = cache.stats[resultIdx]
		cache.fileWeights = append(cache.fileWeights, stats.Weight)
		cache.statsFilenames[filename] = resultIdx
	} else {
		resultIdx = idx
		stats = cache.stats[resultIdx]
	}

	return resultIdx, stats
}

func (cache *WeightedLRU) moveStat(curIdx int, curStats *WeightedFileStats) {
	curWeight := cache.fileWeights[curIdx]
	var targetIdx int

	// <--[Check left]
	if curIdx-1 >= 0 && cache.fileWeights[curIdx-1] < curWeight {
		targetIdx = -1
		for idx := curIdx - 1; idx >= 0; idx-- {
			targetWeight := cache.fileWeights[idx]
			if targetWeight >= curWeight {
				targetIdx = idx
				break
			}
		}

		copy(cache.stats[targetIdx+2:curIdx+1], cache.stats[targetIdx+1:curIdx])
		copy(cache.fileWeights[targetIdx+2:curIdx+1], cache.fileWeights[targetIdx+1:curIdx])
		cache.stats[targetIdx+1] = curStats
		cache.fileWeights[targetIdx+1] = curWeight
		cache.reIndex(targetIdx, curIdx)

	} else if curIdx+1 < len(cache.stats) && cache.fileWeights[curIdx+1] > curWeight { // [Check right]-->
		targetIdx = len(cache.stats)
		for idx := curIdx + 1; idx < len(cache.stats); idx++ {
			targetWeight := cache.fileWeights[idx]
			if targetWeight <= curWeight {
				targetIdx = idx
				break
			}
		}

		copy(cache.stats[curIdx:targetIdx], cache.stats[curIdx+1:targetIdx])
		copy(cache.fileWeights[curIdx:targetIdx], cache.fileWeights[curIdx+1:targetIdx])
		cache.stats[targetIdx-1] = curStats
		cache.fileWeights[targetIdx-1] = curWeight
		cache.reIndex(curIdx, targetIdx)

	}

}

func (cache *WeightedLRU) updatePolicy(filename string, size float32, hit bool, vars ...interface{}) bool {
	var (
		added       = false
		currentTime = vars[0].(time.Time)
		curStats    *WeightedFileStats
		statsIdx    int
	)

	if cache.SelUpdateStatPolicyType == UpdateStatsOnRequest {
		statsIdx, curStats = cache.getOrInsertStats(filename)
		curStats.updateStats(hit, size, currentTime)
		newWeight := curStats.updateWeight(cache.SelFunctionType, cache.Exp)
		cache.fileWeights[statsIdx] = newWeight
		cache.moveStat(statsIdx, curStats)
	}

	if !hit {
		if cache.SelUpdateStatPolicyType == UpdateStatsOnMiss {
			statsIdx, curStats = cache.getOrInsertStats(filename)
			curStats.updateStats(hit, size, currentTime)
			newWeight := curStats.updateWeight(cache.SelFunctionType, cache.Exp)
			cache.fileWeights[statsIdx] = newWeight
			cache.moveStat(statsIdx, curStats)
		}

		var Q2 = cache.getThreshold()
		// If weight is higher exit and return added = false
		if curStats.Weight > Q2 {
			return added
		}
		// Insert with LRU mechanism
		if cache.Size()+size > cache.MaxSize {
			var totalDeleted float32
			tmpVal := cache.queue.Front()
			for {
				if tmpVal == nil {
					break
				}
				fileSize := cache.files[tmpVal.Value.(string)]
				cache.size -= fileSize
				cache.dataDeleted += size

				totalDeleted += fileSize
				delete(cache.files, tmpVal.Value.(string))

				tmpVal = tmpVal.Next()
				// Check if all files are deleted
				if tmpVal == nil {
					break
				}
				cache.queue.Remove(tmpVal.Prev())

				if totalDeleted >= size {
					break
				}
			}
		}
		if cache.Size()+size <= cache.MaxSize {
			cache.files[filename] = size
			cache.queue.PushBack(filename)
			cache.size += size
			added = true
		}
	} else {
		var elm2move *list.Element
		for tmpVal := cache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
			if tmpVal.Value.(string) == filename {
				elm2move = tmpVal
				break
			}
		}
		if elm2move != nil {
			cache.queue.MoveToBack(elm2move)
		}
	}
	return added
}

// Get a file from the cache updating the statistics
func (cache *WeightedLRU) Get(filename string, size float32, vars ...interface{}) bool {
	day := vars[0].(int64)
	curTime := time.Unix(day, 0)

	hit := cache.check(filename)
	added := cache.updatePolicy(filename, size, hit, curTime)

	if hit {
		cache.hit += 1.
		cache.dataReadOnHit += size
	} else {
		cache.miss += 1.
		cache.dataReadOnMiss += size
	}

	if added {
		cache.dataWritten += size
	}
	cache.dataRead += size

	cache.lastFileHitted = hit
	cache.lastFileAdded = added
	cache.lastFileName = filename

	return added
}

// HitRate of the cache
func (cache *WeightedLRU) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// HitOverMiss of the cache
func (cache *WeightedLRU) HitOverMiss() float32 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache *WeightedLRU) WeightedHitRate() float32 {
	return cache.HitRate() * cache.dataReadOnHit
}

// Size of the cache
func (cache *WeightedLRU) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache *WeightedLRU) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// DataWritten of the cache
func (cache *WeightedLRU) DataWritten() float32 {
	return cache.dataWritten
}

// DataRead of the cache
func (cache *WeightedLRU) DataRead() float32 {
	return cache.dataRead
}

// DataReadOnHit of the cache
func (cache *WeightedLRU) DataReadOnHit() float32 {
	return cache.dataReadOnHit
}

// DataReadOnMiss of the cache
func (cache *WeightedLRU) DataReadOnMiss() float32 {
	return cache.dataReadOnMiss
}

// DataDeleted of the cache
func (cache *WeightedLRU) DataDeleted() float32 {
	return cache.dataDeleted
}

func (cache *WeightedLRU) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
