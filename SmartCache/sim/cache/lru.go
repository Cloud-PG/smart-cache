package cache

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"

	pb "simulator/v2/cache/simService"

	empty "github.com/golang/protobuf/ptypes/empty"
	"go.uber.org/zap"
)

// LRUCache cache
type LRUCache struct {
	Stats
	files                              map[int64]float64
	queue                              []int64
	hit, miss, size, MaxSize           float64
	hitCPUTime, missCPUTime            float64
	hitWTime, missWTime                float64
	idealCPUTime, idealWTime           float64
	dataWritten, dataRead, dataDeleted float64
	dataReadOnHit, dataReadOnMiss      float64
	HighWaterMark                      float64
	LowWaterMark                       float64
}

// Init the LRU struct
func (cache *LRUCache) Init(_ ...interface{}) interface{} {
	cache.Stats.Init()
	cache.files = make(map[int64]float64)
	cache.queue = make([]int64, 0)
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

// ClearFiles remove the cache files
func (cache *LRUCache) ClearFiles() {
	cache.files = make(map[int64]float64)
	cache.size = 0.
}

// Clear the LRU struct
func (cache *LRUCache) Clear() {
	cache.Stats.Init()
	cache.ClearFiles()
	cache.queue = make([]int64, 0)
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
	cache.hitCPUTime = 0.
	cache.missCPUTime = 0.
	cache.hitWTime = 0.
	cache.missWTime = 0.
	cache.idealWTime = 0.
	cache.idealCPUTime = 0.
}

// ClearHitMissStats the cache stats
func (cache *LRUCache) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
	cache.hitCPUTime = 0.
	cache.missCPUTime = 0.
	cache.hitWTime = 0.
	cache.missWTime = 0.
	cache.idealWTime = 0.
	cache.idealCPUTime = 0.
}

// Dumps the LRUCache cache
func (cache *LRUCache) Dumps() [][]byte {
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
	return outData
}

// Dump the LRUCache cache
func (cache *LRUCache) Dump(filename string) {
	outFile, osErr := os.Create(filename)
	if osErr != nil {
		panic(fmt.Sprintf("Error dump file creation: %s", osErr))
	}
	gwriter := gzip.NewWriter(outFile)

	for _, record := range cache.Dumps() {
		gwriter.Write(record)
	}

	gwriter.Close()
}

// Loads the LRUCache cache
func (cache *LRUCache) Loads(inputString [][]byte) {
	var curRecord DumpRecord
	var curRecordInfo DumpInfo
	for _, record := range inputString {
		json.Unmarshal(record, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileDump
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files[curFile.Filename] = curFile.Size
			cache.size += curFile.Size
		}
	}
}

// Load the LRUCache cache
func (cache LRUCache) Load(filename string) [][]byte {
	logger.Info("Dump cache", zap.String("filename", filename))

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

// SimGet updates the cache from a protobuf message
func (cache *LRUCache) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := GetFile(cache, commonFile.Filename, commonFile.Size, 0.0, 0.0)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimClear deletes all cache content
func (cache *LRUCache) SimClear(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearFiles deletes all cache content
func (cache *LRUCache) SimClearFiles(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearFiles()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearHitMissStats deletes all cache content
func (cache *LRUCache) SimClearHitMissStats(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearHitMissStats()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimGetInfoCacheStatus returns the current simulation status
func (cache *LRUCache) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimDumps returns the content of the cache
func (cache *LRUCache) SimDumps(_ *empty.Empty, stream pb.SimService_SimDumpsServer) error {
	for _, record := range cache.Dumps() {
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
func (cache *LRUCache) SimLoads(stream pb.SimService_SimLoadsServer) error {
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

	cache.Loads(records)

	return nil
}

// BeforeRequest of LRU cache
func (cache *LRUCache) BeforeRequest(request *Request, hit bool) *FileStats {
	curStats, _ := cache.GetOrCreate(request.Filename, request.Size, request.DayTime)
	curStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)
	return curStats
}

// UpdatePolicy of LRU cache
func (cache *LRUCache) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added = false

		requestedFileSize = request.Size
		requestedFilename = request.Filename
	)

	if !hit {
		if cache.Size()+requestedFileSize > cache.MaxSize {
			cache.Free(requestedFileSize, false)
		}
		if cache.Size()+requestedFileSize <= cache.MaxSize {
			cache.files[requestedFilename] = requestedFileSize
			cache.queue = append(cache.queue, requestedFilename)
			cache.size += requestedFileSize
			added = true
		}
	} else {
		cache.UpdateFileInQueue(requestedFilename)
	}
	return added
}

// AfterRequest of LRU cache
func (cache *LRUCache) AfterRequest(request *Request, hit bool, added bool) {
	cache.idealCPUTime += request.CPUTime
	cache.idealWTime += request.WTime

	if hit {
		cache.hit += 1.
		cache.dataReadOnHit += request.Size
		cache.hitCPUTime += request.CPUTime
		cache.hitWTime += request.WTime
	} else {
		cache.miss += 1.
		cache.dataReadOnMiss += request.Size
		cache.missCPUTime += request.CPUTime
		cache.missWTime += request.WTime
	}

	// Always true because of LRU policy
	// - added variable is needed just for code consistency
	if added {
		cache.dataWritten += request.Size
	}
	cache.dataRead += request.Size

	if cache.Stats.DirtyStats() {
		cache.Stats.PurgeStats()
	}
}

// UpdateFileInQueue move the file requested on the back of the queue
func (cache *LRUCache) UpdateFileInQueue(filename int64) {
	for idx, elm := range cache.queue {
		if elm == filename {
			cache.queue = append(cache.queue[:idx], cache.queue[idx+1:]...)
			cache.queue = append(cache.queue, elm)
			break
		}
	}
}

// Free removes files from the cache
func (cache *LRUCache) Free(amount float64, percentage bool) float64 {
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
		var maxIdx2Delete int
		for idx, fileName := range cache.queue {
			fileSize := cache.files[fileName]
			// Update sizes
			cache.size -= fileSize
			cache.dataDeleted += fileSize
			totalDeleted += fileSize

			// Remove from queue
			delete(cache.files, fileName)
			maxIdx2Delete = idx

			if totalDeleted >= sizeToDelete {
				break
			}
		}
		cache.queue = cache.queue[maxIdx2Delete+1:]
	}
	return totalDeleted
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *LRUCache) CheckWatermark() bool {
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
func (cache LRUCache) HitRate() float64 {
	perc := (cache.hit / (cache.hit + cache.miss)) * 100.
	if math.IsNaN(float64(perc)) {
		return 0.0
	}
	return perc
}

// HitOverMiss of the cache
func (cache LRUCache) HitOverMiss() float64 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache LRUCache) WeightedHitRate() float64 {
	return cache.HitRate() * cache.dataReadOnHit
}

// Size of the cache
func (cache LRUCache) Size() float64 {
	return cache.size
}

// Capacity of the cache
func (cache LRUCache) Capacity() float64 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// DataWritten of the cache
func (cache LRUCache) DataWritten() float64 {
	return cache.dataWritten
}

// DataRead of the cache
func (cache LRUCache) DataRead() float64 {
	return cache.dataRead
}

// DataReadOnHit of the cache
func (cache LRUCache) DataReadOnHit() float64 {
	return cache.dataReadOnHit
}

// DataReadOnMiss of the cache
func (cache LRUCache) DataReadOnMiss() float64 {
	return cache.dataReadOnMiss
}

// DataDeleted of the cache
func (cache LRUCache) DataDeleted() float64 {
	return cache.dataDeleted
}

// Check returns if a file is in cache or not
func (cache LRUCache) Check(key int64) bool {
	_, ok := cache.files[key]
	return ok
}

// ExtraStats for output
func (cache LRUCache) ExtraStats() string {
	return "NONE"
}

// ExtraOutput for output specific information
func (cache LRUCache) ExtraOutput(info string) string {
	return "NONE"
}

// CPUEff returns the CPU efficiency
func (cache LRUCache) CPUEff() float64 {
	totCPUTime := cache.hitCPUTime + cache.missCPUTime
	totWtime := cache.hitWTime + (cache.missWTime * 1.15)
	return (totCPUTime / totWtime) * 100.
}

// CPUHitEff returns the CPU efficiency for hit data
func (cache LRUCache) CPUHitEff() float64 {
	return (cache.hitCPUTime / cache.hitWTime) * 100.
}

// CPUMissEff returns the CPU efficiency for miss data
func (cache LRUCache) CPUMissEff() float64 {
	// Add the 15% to wall time -> estimated loss time to retrieve the files
	return (cache.missCPUTime / (cache.missWTime * 1.15)) * 100.
}

// CPUEffUpperBound returns the ideal CPU efficiency upper bound
func (cache LRUCache) CPUEffUpperBound() float64 {
	return (cache.idealCPUTime / cache.idealWTime) * 100.
}

// CPUEffLowerBound returns the ideal CPU efficiency lower bound
func (cache LRUCache) CPUEffLowerBound() float64 {
	return (cache.idealCPUTime / (cache.idealWTime * 1.15)) * 100.
}
