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

	pb "simulator/v2/cache/simService"

	empty "github.com/golang/protobuf/ptypes/empty"
)

// LRUCache cache
type LRUCache struct {
	Stats
	files                              map[string]float32
	queue                              *list.List
	hit, miss, size, MaxSize           float32
	hitCPUTime, missCPUTime            float32
	hitWTime, missWTime                float32
	dataWritten, dataRead, dataDeleted float32
	dataReadOnHit, dataReadOnMiss      float32
	HighWaterMark                      float32
	LowWaterMark                       float32
}

// Init the LRU struct
func (cache *LRUCache) Init(_ ...interface{}) interface{} {
	cache.Stats.Init()
	cache.files = make(map[string]float32)
	cache.queue = list.New()
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
	cache.files = make(map[string]float32)
	cache.size = 0.
}

// Clear the LRU struct
func (cache *LRUCache) Clear() {
	cache.Stats.Init()
	cache.ClearFiles()
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
	cache.hitCPUTime = 0.
	cache.missCPUTime = 0.
	cache.hitWTime = 0.
	cache.missWTime = 0.
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
}

// Dumps the LRUCache cache
func (cache *LRUCache) Dumps() *[][]byte {
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
	return &outData
}

// Dump the LRUCache cache
func (cache *LRUCache) Dump(filename string) {
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

// Loads the LRUCache cache
func (cache *LRUCache) Loads(inputString *[][]byte) {
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
		}
	}
}

// Load the LRUCache cache
func (cache LRUCache) Load(filename string) {
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

	cache.Loads(&records)

	return nil
}

// BeforeRequest of LRU cache
func (cache *LRUCache) BeforeRequest(request *Request, hit bool) *FileStats {
	curStats, _ := cache.GetOrCreate(request.Filename, request.Size)
	curStats.updateStats(hit, request.Size, request.UserID, request.SiteName, &request.DayTime)
	return curStats
}

// UpdatePolicy of LRU cache
func (cache *LRUCache) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var added = false

	requestedFileSize := request.Size
	requestedFilename := request.Filename

	if !hit {
		if cache.Size()+requestedFileSize > cache.MaxSize {
			cache.Free(requestedFileSize, false)
		}
		if cache.Size()+requestedFileSize <= cache.MaxSize {
			cache.files[requestedFilename] = requestedFileSize
			cache.queue.PushBack(requestedFilename)
			cache.size += requestedFileSize
			added = true
		}
	} else {
		cache.UpdateFileInQueue(requestedFilename)
	}
	return added
}

// UpdateFileInQueue move the file requested on the back of the queue
func (cache *LRUCache) UpdateFileInQueue(filename string) {
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

// AfterRequest of LRU cache
func (cache *LRUCache) AfterRequest(request *Request, hit bool, added bool) {
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
}

// Free removes files from the cache
func (cache *LRUCache) Free(amount float32, percentage bool) float32 {
	var (
		totalDeleted float32
		sizeToDelete float32
	)
	if percentage {
		sizeToDelete = amount * (cache.MaxSize / 100.)
	} else {
		sizeToDelete = amount
	}
	tmpVal := cache.queue.Front()
	for {
		if tmpVal == nil {
			break
		}
		fileSize := cache.files[tmpVal.Value.(string)]
		// Update sizes
		cache.size -= fileSize
		cache.dataDeleted += fileSize
		totalDeleted += fileSize

		// Remove from queue
		delete(cache.files, tmpVal.Value.(string))
		tmpVal = tmpVal.Next()
		// Check if all files are deleted
		if tmpVal == nil {
			break
		}
		cache.queue.Remove(tmpVal.Prev())

		if totalDeleted >= sizeToDelete {
			break
		}
	}
	return totalDeleted
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *LRUCache) CheckWatermark() bool {
	ok := true
	if cache.Capacity() >= cache.HighWaterMark {
		ok = false
		cache.Free(
			cache.HighWaterMark-cache.LowWaterMark,
			true,
		)
	}
	return ok
}

// HitRate of the cache
func (cache LRUCache) HitRate() float32 {
	perc := (cache.hit / (cache.hit + cache.miss)) * 100.
	if math.IsNaN(float64(perc)) {
		return 0.0
	}
	return perc
}

// HitOverMiss of the cache
func (cache LRUCache) HitOverMiss() float32 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache LRUCache) WeightedHitRate() float32 {
	return cache.HitRate() * cache.dataReadOnHit
}

// Size of the cache
func (cache LRUCache) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache LRUCache) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// DataWritten of the cache
func (cache LRUCache) DataWritten() float32 {
	return cache.dataWritten
}

// DataRead of the cache
func (cache LRUCache) DataRead() float32 {
	return cache.dataRead
}

// DataReadOnHit of the cache
func (cache LRUCache) DataReadOnHit() float32 {
	return cache.dataReadOnHit
}

// DataReadOnMiss of the cache
func (cache LRUCache) DataReadOnMiss() float32 {
	return cache.dataReadOnMiss
}

// DataDeleted of the cache
func (cache LRUCache) DataDeleted() float32 {
	return cache.dataDeleted
}

// Check returns if a file is in cache or not
func (cache LRUCache) Check(key string) bool {
	_, ok := cache.files[key]
	return ok
}

// ExtraStats for output
func (cache LRUCache) ExtraStats() string {
	return "NONE"
}

// CPUEff returns the CPU efficiency
func (cache LRUCache) CPUEff() float32 {
	totCPUTime := cache.hitCPUTime + cache.missCPUTime
	totWtime := cache.hitWTime + (cache.missWTime * 1.15)
	return (totCPUTime / totWtime) * 100.
}

// CPUHitEff returns the CPU efficiency for hit data
func (cache LRUCache) CPUHitEff() float32 {
	return (cache.hitCPUTime / cache.hitWTime) * 100.
}

// CPUMissEff returns the CPU efficiency for miss data
func (cache LRUCache) CPUMissEff() float32 {
	// Add the 15% to wall time -> estimated loss time to retrieve the files
	return (cache.missCPUTime / (cache.missWTime * 1.15)) * 100.
}

// Report returns the current cache file status and statistics
func (cache LRUCache) Report() []string {
	// var (
	// 	numFiles       = len(cache.files)
	// 	avgSize        float32
	// 	avgNumUsers    float32
	// 	avgNumSites    float32
	// 	avgNumRequests float32
	// 	avgNumHits     float32
	// 	avgNumMiss     float32
	// )
	// for filename, size := range cache.files {
	// 	avgSize += size
	// 	curStats := cache.GetOrCreate(filename)
	// 	avgNumUsers += len(curStats.
	// }

	return []string{""}
}
