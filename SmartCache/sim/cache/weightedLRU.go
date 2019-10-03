package cache

import (
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// WeightedLRU cache
type WeightedLRU struct {
	files                                map[string]float32
	stats                                []*WeightedFileStats
	statsFilenames                       sync.Map
	queue                                *list.List
	hit, miss, size, MaxSize, Exp        float32
	dataWritten, dataRead, dataReadOnHit float32
	SelFunctionType                      FunctionType
	SelUpdateStatPolicyType              UpdateStatsPolicyType
	SelLimitStatsPolicyType              LimitStatsPolicyType
	lastFileHitted                       bool
	lastFileAdded                        bool
	lastFileName                         string
}

// Init the WeightedLRU struct
func (cache *WeightedLRU) Init(_ ...interface{}) {
	cache.files = make(map[string]float32)
	cache.stats = make([]*WeightedFileStats, 0)
	cache.statsFilenames = sync.Map{}
	cache.queue = list.New()
}

// ClearFiles remove the cache files
func (cache *WeightedLRU) ClearFiles() {
	cache.files = make(map[string]float32)
	cache.size = 0.
}

// Clear the WeightedLRU struct
func (cache *WeightedLRU) Clear() {
	cache.ClearFiles()
	cache.stats = make([]*WeightedFileStats, 0)
	cache.statsFilenames.Range(
		func(key interface{}, value interface{}) bool {
			cache.statsFilenames.Delete(key)
			return true
		},
	)
	cache.statsFilenames = sync.Map{}
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
}

// ClearHitMissStats the cache stats
func (cache *WeightedLRU) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
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

// GetReport from the cache
func (cache *WeightedLRU) GetReport() (*DatasetInput, error) {
	index, inStats := cache.statsFilenames.Load(cache.lastFileName)
	if !inStats {
		return nil, errors.New("The file is not in cache stats anymore")
	}
	stats := cache.stats[index.(int)]
	return &DatasetInput{
		CacheSize:       cache.size,
		CacheMaxSize:    cache.MaxSize,
		FileSize:        stats.Size,
		FileTotRequests: stats.TotRequests,
		FileNHits:       stats.NHits,
		FileNMiss:       stats.NMiss,
		FileMeanTimeReq: stats.RequestTicksMean,
		LastFileHitted:  cache.lastFileHitted,
		LastFileAdded:   cache.lastFileAdded,
	}, nil
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

func (cache *WeightedLRU) reIndex() {
	wg := sync.WaitGroup{}
	for curIdx := 0; curIdx < len(cache.stats); curIdx++ {
		curStatFilename := cache.stats[curIdx].Filename
		wg.Add(1)
		go func(index int, filename string, waitGroup *sync.WaitGroup) {
			cache.statsFilenames.Store(filename, index)
			waitGroup.Done()
		}(curIdx, curStatFilename, &wg)
	}
	wg.Wait()
}

func (cache *WeightedLRU) getThreshold() float32 {
	if len(cache.stats) == 0 {
		return 0.0
	}

	Q2 := cache.stats[int(math.Floor(float64(0.5*float32(len(cache.stats)))))].Weight

	if cache.Capacity() > 75. {
		if cache.SelLimitStatsPolicyType == Q1IsDoubleQ2LimitStats {
			Q1Idx := int(math.Floor(float64(0.25 * float32(len(cache.stats)))))
			Q1 := cache.stats[Q1Idx].Weight
			if Q1 > 2*Q2 {
				for idx := 0; idx < Q1Idx; idx++ {
					cache.statsFilenames.Delete(cache.stats[idx].Filename)
				}
				copy(cache.stats, cache.stats[Q1Idx:])
				cache.stats = cache.stats[:len(cache.stats)-Q1Idx]
				// Force to reindex
				cache.reIndex()
			}
		}
	}

	return Q2
}

func (cache *WeightedLRU) getOrInsertStats(filename string) *WeightedFileStats {
	var resultIdx int

	idx, inStats := cache.statsFilenames.Load(filename)

	if !inStats {
		cache.stats = append(cache.stats, &WeightedFileStats{
			filename,
			0.,
			0.,
			0,
			0,
			0,
			time.Now(),
			0.,
			[StatsMemorySize]time.Time{},
			0,
		})
		cache.statsFilenames.Store(filename, len(cache.stats)-1)
		resultIdx = len(cache.stats) - 1
	} else {
		resultIdx = idx.(int)
	}

	return cache.stats[resultIdx]
}

func (cache *WeightedLRU) moveStat(stat *WeightedFileStats, curTime time.Time) {
	idx, _ := cache.statsFilenames.Load(stat.Filename)
	curIdx := idx.(int)

	if curIdx-1 >= 0 { // <--[Check left]
		for idx := curIdx; idx > 0; idx-- {
			curStats := cache.stats[idx]
			prevStats := cache.stats[idx-1]
			if prevStats.Weight < curStats.Weight {
				cache.stats[idx-1] = curStats
				cache.stats[idx] = prevStats
				cache.statsFilenames.Store(curStats.Filename, idx-1)
				cache.statsFilenames.Store(prevStats.Filename, idx)
			} else {
				break
			}
		}
	}
	if curIdx+1 < len(cache.stats) { // [Check right]-->
		for idx := curIdx; idx < len(cache.stats)-1; idx++ {
			curStats := cache.stats[idx]
			nextStats := cache.stats[idx+1]
			if nextStats.Weight > curStats.Weight {
				cache.stats[idx+1] = curStats
				cache.stats[idx] = nextStats
				cache.statsFilenames.Store(curStats.Filename, idx+1)
				cache.statsFilenames.Store(nextStats.Filename, idx)
			} else {
				break
			}
		}
	}
}

func (cache *WeightedLRU) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	var currentTime = time.Now()
	var curStats *WeightedFileStats

	if cache.SelUpdateStatPolicyType == UpdateStatsOnRequest {
		curStats = cache.getOrInsertStats(filename)
		curStats.updateStats(hit, size, currentTime)
		curStats.updateWeight(cache.SelFunctionType, cache.Exp, time.Time{})
		cache.moveStat(curStats, time.Time{})
	}

	if !hit {
		if cache.SelUpdateStatPolicyType == UpdateStatsOnMiss {
			curStats = cache.getOrInsertStats(filename)
			curStats.updateStats(hit, size, currentTime)
			curStats.updateWeight(cache.SelFunctionType, cache.Exp, time.Time{})
			cache.moveStat(curStats, time.Time{})
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
	hit := cache.check(filename)
	added := cache.updatePolicy(filename, size, hit)

	if hit {
		cache.hit += 1.
		cache.dataReadOnHit += size
	} else {
		cache.miss += 1.
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

func (cache *WeightedLRU) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}