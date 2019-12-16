package cache

import (
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	pb "simulator/v2/cache/simService"

	empty "github.com/golang/protobuf/ptypes/empty"
)

// LRUCache cache
type LRUCache struct {
	LRUStats
	files                              map[string]float32
	queue                              *list.List
	hit, miss, size, MaxSize           float32
	hitCPUTime, missCPUTime            float32
	hitWTime, missWTime                float32
	dataWritten, dataRead, dataDeleted float32
	dataReadOnHit, dataReadOnMiss      float32
}

// Init the LRU struct
func (cache *LRUCache) Init(_ ...interface{}) interface{} {
	cache.LRUStats.Init()
	cache.files = make(map[string]float32)
	cache.queue = list.New()

	return cache
}

// ClearFiles remove the cache files
func (cache *LRUCache) ClearFiles() {
	cache.files = make(map[string]float32)
	cache.size = 0.
}

// Clear the LRU struct
func (cache *LRUCache) Clear() {
	cache.LRUStats.Init()
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

// UpdatePolicy of LRU cache
func (cache *LRUCache) UpdatePolicy(filename string, size float32, hit bool, _ ...interface{}) bool {
	var added = false
	if !hit {
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

// BeforeRequest of LRU cache
func (cache *LRUCache) BeforeRequest(hit bool, filename string, size float32, vars ...interface{}) {
	curFileStats := cache.GetOrCreate(filename, size)
	curFileStats.updateRequests(hit)
}

// AfterRequest of LRU cache
func (cache *LRUCache) AfterRequest(hit bool, added bool, size float32, wTime float32, cpuTime float32) {
	if hit {
		cache.hit += 1.
		cache.dataReadOnHit += size
		cache.hitCPUTime += cpuTime
		cache.hitWTime += wTime
	} else {
		cache.miss += 1.
		cache.dataReadOnMiss += size
		cache.missCPUTime += cpuTime
		cache.missWTime += wTime
	}

	// Always true because of LRU policy
	// - added variable is needed just for code consistency
	if added {
		cache.dataWritten += size
	}
	cache.dataRead += size
}

// HitRate of the cache
func (cache LRUCache) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
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
	lostEff := (cache.missCPUTime * 0.15)
	totEff := cache.hitCPUTime + cache.missCPUTime - lostEff
	totWtime := cache.hitWTime + cache.missWTime
	return (totEff / totWtime) * 100.
}

// CPUHitEff returns the CPU efficiency for hit data
func (cache LRUCache) CPUHitEff() float32 {
	return (cache.hitCPUTime / cache.hitWTime) * 100.
}

// CPUMissEff returns the CPU efficiency for miss data
func (cache LRUCache) CPUMissEff() float32 {
	lostEff := (cache.missCPUTime * 0.15)
	return ((cache.missCPUTime - lostEff) / cache.missWTime) * 100. // subtract the 15%
}
