package cache

import (
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// LRUFileStats contain file statistics collected by LRU cache
type LRUFileStats struct {
	size              float32
	totRequests       uint32
	nHits             uint32
	nMiss             uint32
	lastTimeRequested time.Time
}

func (stats *LRUFileStats) updateRequests(hit bool, newTime time.Time) {
	stats.totRequests++

	if hit {
		stats.nHits++
	} else {
		stats.nMiss++
	}

	stats.lastTimeRequested = newTime
}

// LRUCache cache
type LRUCache struct {
	files                                            map[string]float32
	stats                                            map[string]*LRUFileStats
	queue                                            *list.List
	hit, miss, writtenData, readOnHit, size, MaxSize float32
	latestHitDecision                                bool
	latestAddDecision                                bool
}

// Init the LRU struct
func (cache *LRUCache) Init(vars ...interface{}) {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*LRUFileStats)
	cache.queue = list.New()
}

// ClearFiles remove the cache files
func (cache *LRUCache) ClearFiles() {
	cache.files = make(map[string]float32)
	cache.size = 0.
}

// Clear the LRU struct
func (cache *LRUCache) Clear() {
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
	cache.writtenData = 0.
	cache.readOnHit = 0.
}

// Dump the LRUCache cache
func (cache LRUCache) Dump(filename string) {
	outFile, osErr := os.Create(filename)
	if osErr != nil {
		panic(fmt.Sprintf("Error dump file creation: %s", osErr))
	}
	gwriter := gzip.NewWriter(outFile)

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
		gwriter.Write(record)
	}
	gwriter.Close()
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

	var buffer []byte
	var charBuffer []byte
	var curRecord DumpRecord
	var curRecordInfo DumpInfo

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
			json.Unmarshal(buffer, &curRecord)
			json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
			switch curRecordInfo.Type {
			case "FILES":
				var curFile FileDump
				json.Unmarshal([]byte(curRecord.Data), &curFile)
				cache.files[curFile.Filename] = curFile.Size
				cache.size += curFile.Size
			}
			buffer = buffer[:0]
		} else {
			buffer = append(buffer, charBuffer...)
		}
	}
	greader.Close()
}

// GetFileStats from the cache
func (cache *LRUCache) GetFileStats(filename string) (*DatasetInput, error) {
	stats, inStats := cache.stats[filename]
	if !inStats {
		return nil, errors.New("The file is not in cache stats anymore")
	}
	return &DatasetInput{
		stats.size,
		stats.nHits,
		stats.nMiss,
		stats.totRequests,
		0.0,
	}, nil
}

// ClearHitMissStats the LRU struct
func (cache *LRUCache) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.readOnHit = 0.
}

// SimGet updates the cache from a protobuf message
func (cache *LRUCache) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimReset deletes all cache content
func (cache *LRUCache) SimReset(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	return &pb.SimCacheStatus{
		HitRate:         cache.HitRate(),
		WeightedHitRate: cache.WeightedHitRate(),
		HitOverMiss:     cache.HitOverMiss(),
		Size:            cache.Size(),
		Capacity:        cache.Capacity(),
		WrittenData:     cache.WrittenData(),
		ReadOnHit:       cache.ReadOnHit(),
	}, nil
}

// SimResetHitMissStats deletes all cache content
func (cache *LRUCache) SimResetHitMissStats(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.ClearHitMissStats()
	return &pb.SimCacheStatus{
		HitRate:         cache.HitRate(),
		WeightedHitRate: cache.WeightedHitRate(),
		HitOverMiss:     cache.HitOverMiss(),
		Size:            cache.Size(),
		Capacity:        cache.Capacity(),
		WrittenData:     cache.WrittenData(),
		ReadOnHit:       cache.ReadOnHit(),
	}, nil
}

// SimGetInfoCacheStatus returns the current simulation status
func (cache *LRUCache) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	return &pb.SimCacheStatus{
		HitRate:         cache.HitRate(),
		WeightedHitRate: cache.WeightedHitRate(),
		HitOverMiss:     cache.HitOverMiss(),
		Size:            cache.Size(),
		Capacity:        cache.Capacity(),
		WrittenData:     cache.WrittenData(),
		ReadOnHit:       cache.ReadOnHit(),
	}, nil
}

// SimGetInfoCacheFiles returns the content of the cache: filenames and sizes
func (cache *LRUCache) SimGetInfoCacheFiles(_ *empty.Empty, stream pb.SimService_SimGetInfoCacheFilesServer) error {
	for key, value := range cache.files {
		curFile := &pb.SimCommonFile{
			Filename: key,
			Size:     value,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}
	return nil
}

// SimGetInfoFilesWeights returns the file weights
func (cache *LRUCache) SimGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesWeightsServer) error {
	return nil
}

// SimGetInfoFilesStats returns the content of the file stats
func (cache *LRUCache) SimGetInfoFilesStats(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesStatsServer) error {
	for filename, stats := range cache.stats {
		curFile := &pb.SimFileStats{
			Filename: filename,
			Size:     stats.size,
			TotReq:   stats.totRequests,
			NHits:    stats.nHits,
			NMiss:    stats.nMiss,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}
	return nil
}

func (cache *LRUCache) updatePolicy(filename string, size float32, hit bool) bool {
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

// GetLatestDecision returns the latest decision of the cache
func (cache *LRUCache) GetLatestDecision() (bool, bool) {
	return cache.latestHitDecision, cache.latestAddDecision
}

// Get a file from the cache updating the statistics
func (cache *LRUCache) Get(filename string, size float32) bool {
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
		cache.readOnHit += size
	} else {
		cache.miss += 1.
	}

	if added {
		cache.writtenData += size
	}

	cache.latestHitDecision = hit
	cache.latestAddDecision = added

	return added
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
	return cache.HitRate() * cache.readOnHit
}

// Size of the cache
func (cache LRUCache) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache LRUCache) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// WrittenData of the cache
func (cache LRUCache) WrittenData() float32 {
	return cache.writtenData
}

// ReadOnHit of the cache
func (cache LRUCache) ReadOnHit() float32 {
	return cache.readOnHit
}

func (cache LRUCache) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
