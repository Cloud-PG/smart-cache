package cache

import (
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "simulator/v2/cache/simService"

	empty "github.com/golang/protobuf/ptypes/empty"
)

// DatasetFiles represents the dataset file composition
type DatasetFiles struct {
	SelectedFiles []string `json:"selected_files"`
}

// LRUTestDataset cache
type LRUTestDataset struct {
	files                              map[string]float32
	stats                              map[string]*LRUFileStats
	queue                              *list.List
	hit, miss, size, MaxSize           float32
	dataWritten, dataRead, dataDeleted float32
	dataReadOnHit, dataReadOnMiss      float32
	lastFileHitted                     bool
	lastFileAdded                      bool
	lastFileName                       string
	datasetFiles                       *DatasetFiles
}

// Init the LRU struct
func (cache *LRUTestDataset) Init(args ...interface{}) interface{} {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*LRUFileStats)
	cache.queue = list.New()

	datasetFilePath := args[0].(string)

	datasetFile, errOpenFile := os.Open(datasetFilePath)
	if errOpenFile != nil {
		log.Fatalf("Cannot open file '%s'\n", errOpenFile)
	}

	datasetFileGz, errOpenZipFile := gzip.NewReader(datasetFile)
	if errOpenZipFile != nil {
		log.Fatalf("Cannot open zip stream from file '%s'\nError: %s\n", datasetFilePath, errOpenZipFile)
	}

	errJSONUnmarshal := json.NewDecoder(datasetFileGz).Decode(&cache.datasetFiles)
	if errJSONUnmarshal != nil {
		log.Fatalf("Cannot unmarshal json from file '%s'\nError: %s\n", datasetFilePath, errJSONUnmarshal)
	}

	return cache
}

// ClearFiles remove the cache files
func (cache *LRUTestDataset) ClearFiles() {
	cache.files = make(map[string]float32)
	cache.size = 0.
}

// Clear the LRU struct
func (cache *LRUTestDataset) Clear() {
	cache.ClearFiles()
	cache.stats = make(map[string]*LRUFileStats)
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
func (cache *LRUTestDataset) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
	cache.dataReadOnMiss = 0.
	cache.dataDeleted = 0.
}

// Dumps the LRUTestDataset cache
func (cache *LRUTestDataset) Dumps() *[][]byte {
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

// Dump the LRUTestDataset cache
func (cache *LRUTestDataset) Dump(filename string) {
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

// Loads the LRUTestDataset cache
func (cache *LRUTestDataset) Loads(inputString *[][]byte) {
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

// Load the LRUTestDataset cache
func (cache LRUTestDataset) Load(filename string) {
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
func (cache *LRUTestDataset) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimClear deletes all cache content
func (cache *LRUTestDataset) SimClear(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearFiles deletes all cache content
func (cache *LRUTestDataset) SimClearFiles(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearFiles()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearHitMissStats deletes all cache content
func (cache *LRUTestDataset) SimClearHitMissStats(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearHitMissStats()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimGetInfoCacheStatus returns the current simulation status
func (cache *LRUTestDataset) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimDumps returns the content of the cache
func (cache *LRUTestDataset) SimDumps(_ *empty.Empty, stream pb.SimService_SimDumpsServer) error {
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
func (cache *LRUTestDataset) SimLoads(stream pb.SimService_SimLoadsServer) error {
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

func (cache *LRUTestDataset) updatePolicy(filename string, size float32, hit bool, _ ...interface{}) bool {
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

// Get a file from the cache updating the statistics
func (cache *LRUTestDataset) Get(filename string, size float32, _ ...interface{}) bool {
	if _, ok := cache.stats[filename]; !ok {
		cache.stats[filename] = &LRUFileStats{
			size,
			0,
			0,
			0,
			time.Now(),
		}
	}

	hit := cache.check(filename)
	added := cache.updatePolicy(filename, size, hit)

	cache.stats[filename].updateRequests(hit, time.Now())

	if hit {
		cache.hit += 1.
		cache.dataReadOnHit += size
	} else {
		cache.miss += 1.
		cache.dataReadOnMiss += size
	}

	// Always true because of LRU policy
	// - added variable is needed just for code consistency
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
func (cache LRUTestDataset) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// HitOverMiss of the cache
func (cache LRUTestDataset) HitOverMiss() float32 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache LRUTestDataset) WeightedHitRate() float32 {
	return cache.HitRate() * cache.dataReadOnHit
}

// Size of the cache
func (cache LRUTestDataset) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache LRUTestDataset) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// DataWritten of the cache
func (cache LRUTestDataset) DataWritten() float32 {
	return cache.dataWritten
}

// DataRead of the cache
func (cache LRUTestDataset) DataRead() float32 {
	return cache.dataRead
}

// DataReadOnHit of the cache
func (cache LRUTestDataset) DataReadOnHit() float32 {
	return cache.dataReadOnHit
}

// DataReadOnMiss of the cache
func (cache LRUTestDataset) DataReadOnMiss() float32 {
	return cache.dataReadOnMiss
}

// DataDeleted of the cache
func (cache LRUTestDataset) DataDeleted() float32 {
	return cache.dataDeleted
}

func (cache LRUTestDataset) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
