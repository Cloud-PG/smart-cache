package cache

import (
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	aiPb "./aiService"
	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// AILRU cache
type AILRU struct {
	files                                map[string]float32
	stats                                []*WeightedFileStats
	statsFilenames                       sync.Map
	queue                                *list.List
	hit, miss, size, MaxSize, Exp        float32
	dataWritten, dataRead, dataReadOnHit float32
	lastFileHitted                       bool
	lastFileAdded                        bool
	lastFileName                         string
	aiClientHost                         string
	aiClientPort                         string
	aiClient                             aiPb.AIServiceClient
	grpcConn                             *grpc.ClientConn
}

// Init the AILRU struct
func (cache *AILRU) Init(args ...interface{}) {
	cache.files = make(map[string]float32)
	cache.queue = list.New()
	cache.stats = make([]*WeightedFileStats, 0)
	cache.statsFilenames = sync.Map{}

	cache.aiClientHost = args[0].(string)
	cache.aiClientPort = args[1].(string)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s",
		cache.aiClientHost, cache.aiClientPort,
	), opts...)

	cache.grpcConn = conn
	if err != nil {
		log.Fatalf("ERROR: Fail to dial wit AI Client: %v", err)
	}

	cache.aiClient = aiPb.NewAIServiceClient(cache.grpcConn)
}

// ClearFiles remove the cache files
func (cache *AILRU) ClearFiles() {
	cache.files = make(map[string]float32)
	cache.size = 0.
}

// Clear the AILRU struct
func (cache *AILRU) Clear() {
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
func (cache *AILRU) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.dataWritten = 0.
	cache.dataRead = 0.
	cache.dataReadOnHit = 0.
}

// Dumps the AILRU cache
func (cache *AILRU) Dumps() *[][]byte {
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

// Dump the AILRU cache
func (cache *AILRU) Dump(filename string) {
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

// Loads the AILRU cache
func (cache *AILRU) Loads(inputString *[][]byte) {
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
}

// Load the AILRU cache
func (cache *AILRU) Load(filename string) {
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
func (cache *AILRU) GetReport() (*DatasetInput, error) {
	index, inStats := cache.statsFilenames.Load(cache.lastFileName)
	if !inStats {
		return nil, errors.New("The file is not in cache stats anymore")
	}
	stats := cache.stats[index.(int)]
	return &DatasetInput{
		CacheSize:       cache.size,
		CacheMaxSize:    cache.MaxSize,
		CacheCapacity:   cache.Capacity(),
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
func (cache *AILRU) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimClear deletes all cache content
func (cache *AILRU) SimClear(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearFiles deletes all cache content
func (cache *AILRU) SimClearFiles(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearFiles()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimClearHitMissStats deletes all cache content
func (cache *AILRU) SimClearHitMissStats(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearHitMissStats()
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimGetInfoCacheStatus returns the current simulation status
func (cache *AILRU) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	curStatus := GetSimCacheStatus(cache)
	return curStatus, nil
}

// SimDumps returns the content of the cache
func (cache *AILRU) SimDumps(_ *empty.Empty, stream pb.SimService_SimDumpsServer) error {
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
func (cache *AILRU) SimLoads(stream pb.SimService_SimLoadsServer) error {
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

func (cache *AILRU) getOrInsertStats(filename string) (int, *WeightedFileStats) {
	var (
		resultIdx int
		stats     *WeightedFileStats
	)

	idx, inStats := cache.statsFilenames.Load(filename)

	if !inStats {
		cache.stats = append(cache.stats, &WeightedFileStats{
			Filename:          filename,
			Weight:            0.,
			Size:              0.,
			TotRequests:       0,
			NHits:             0,
			NMiss:             0,
			LastTimeRequested: time.Now(),
			RequestTicksMean:  0.,
			RequestTicks:      [StatsMemorySize]time.Time{},
			RequestLastIdx:    0,
		})
		resultIdx = len(cache.stats) - 1
		stats = cache.stats[resultIdx]
		cache.statsFilenames.Store(filename, resultIdx)
	} else {
		resultIdx = idx.(int)
		stats = cache.stats[resultIdx]
	}

	return resultIdx, stats
}

func (cache *AILRU) updatePolicy(filename string, size float32, hit bool, vars ...interface{}) bool {
	var added = false

	currentTime := time.Now()
	_, curStats := cache.getOrInsertStats(filename)
	curStats.updateStats(hit, size, currentTime)

	if !hit {
		siteName := vars[0].(string)
		userID := vars[1].(int)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result, err := cache.aiClient.AIPredictOne(ctx,
			&aiPb.AIInput{
				Filename:         filename,
				FileSize:         size,
				SiteName:         siteName,
				UserID:           uint32(userID),
				CacheCapacity:    cache.Capacity(),
				CacheLastFileHit: hit,
				FileTotRequests:  curStats.TotRequests,
				FileNHits:        curStats.NHits,
				FileNMiss:        curStats.NMiss,
				FileMeanTimeReq:  curStats.RequestTicksMean,
			})
		if err != nil {
			log.Fatalf("ERROR: %v.AIPredictOne(_) = _, %v", cache.aiClient, err)
		}
		if result.Store == false {
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
func (cache *AILRU) Get(filename string, size float32, vars ...interface{}) bool {
	hit := cache.check(filename)
	siteName := vars[0].(string)
	userID := vars[1].(int)

	added := cache.updatePolicy(filename, size, hit, siteName, userID)

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
func (cache *AILRU) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// HitOverMiss of the cache
func (cache *AILRU) HitOverMiss() float32 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache *AILRU) WeightedHitRate() float32 {
	return cache.HitRate() * cache.dataReadOnHit
}

// Size of the cache
func (cache *AILRU) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache *AILRU) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// DataWritten of the cache
func (cache *AILRU) DataWritten() float32 {
	return cache.dataWritten
}

// DataRead of the cache
func (cache *AILRU) DataRead() float32 {
	return cache.dataRead
}

// DataReadOnHit of the cache
func (cache *AILRU) DataReadOnHit() float32 {
	return cache.dataReadOnHit
}

func (cache *AILRU) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
