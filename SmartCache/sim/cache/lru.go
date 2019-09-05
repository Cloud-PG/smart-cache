package cache

import (
	"container/list"
	"context"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

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
}

// Init the LRU struct
func (cache *LRUCache) Init(vars ...interface{}) {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*LRUFileStats)
	cache.queue = list.New()
}

// Clear the LRU struct
func (cache *LRUCache) Clear() {
	cache.files = make(map[string]float32)
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
	cache.size = 0.
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
				cache.queue.Remove(tmpVal.Prev())

				if totalDeleted >= size {
					break
				}
			}
		}
		cache.files[filename] = size
		cache.queue.PushBack(filename)
		cache.size += size
		added = true
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
	if cache.hit == 0. {
		return 0.
	}
	var sumHits float32
	var sumMiss float32
	for _, stats := range cache.stats {
		sumHits += float32(stats.nHits) * stats.size
		sumMiss += float32(stats.nMiss) * stats.size
	}
	return (sumHits / (sumHits + sumMiss)) * 100.
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
