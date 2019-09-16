package cache

import (
	"context"
	"errors"
	"sort"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// WeightedCache cache
type WeightedCache struct {
	files                                                 map[string]float32
	stats                                                 map[string]*WeightedFileStats
	queue                                                 []*WeightedFileStats
	hit, miss, writtenData, readOnHit, size, MaxSize, Exp float32
	SelFunctionType                                       FunctionType
	latestHitDecision                                     bool
	latestAddDecision                                     bool
}

// Init the WeightedCache struct
func (cache *WeightedCache) Init(vars ...interface{}) {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*WeightedFileStats)
	cache.queue = make([]*WeightedFileStats, 0)
}

// Clear the WeightedCache struct
func (cache *WeightedCache) Clear() {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*WeightedFileStats)
	cache.queue = make([]*WeightedFileStats, 0)
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.readOnHit = 0.
	cache.size = 0.
}

// GetFileStats from the cache
func (cache *WeightedCache) GetFileStats(filename string) (*DatasetInput, error) {
	stats, inStats := cache.stats[filename]
	if !inStats {
		return nil, errors.New("The file is not in cache stats anymore")
	}
	return &DatasetInput{
		stats.size,
		stats.nHits,
		stats.nMiss,
		stats.totRequests,
		stats.getMeanReqTimes(time.Now()),
	}, nil
}

// ClearHitMissStats the LRU struct
func (cache *WeightedCache) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.readOnHit = 0.
}

// SimGet updates the cache from a protobuf message
func (cache *WeightedCache) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimReset deletes all cache content
func (cache *WeightedCache) SimReset(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
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
func (cache *WeightedCache) SimResetHitMissStats(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
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
func (cache *WeightedCache) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
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
func (cache *WeightedCache) SimGetInfoCacheFiles(_ *empty.Empty, stream pb.SimService_SimGetInfoCacheFilesServer) error {
	for filename, size := range cache.files {
		curFile := &pb.SimCommonFile{
			Filename: filename,
			Size:     size,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}
	return nil
}

// SimGetInfoFilesStats returns the content of the file stats
func (cache *WeightedCache) SimGetInfoFilesStats(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesStatsServer) error {
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

// SimGetInfoFilesWeights returns the file weights
func (cache *WeightedCache) SimGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesWeightsServer) error {
	for filename, stats := range cache.stats {
		curFile := &pb.SimFileWeight{
			Filename: filename,
			Weight:   stats.weight,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}

	return nil
}

func (cache *WeightedCache) getQueueSize() float32 {
	var size float32
	for _, stats := range cache.queue {
		size += stats.size
	}
	return size
}

func (cache *WeightedCache) removeLast() *WeightedFileStats {
	removedElm := cache.queue[len(cache.queue)-1]
	cache.queue = cache.queue[:len(cache.queue)-1]
	return removedElm
}

func (cache *WeightedCache) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	curTime := time.Now()

	if _, inMap := cache.stats[filename]; !inMap {
		cache.stats[filename] = &WeightedFileStats{
			filename,
			0.,
			0.,
			0,
			0,
			0,
			curTime,
			[StatsMemorySize]time.Time{},
			0,
		}
	}

	cache.stats[filename].updateStats(
		hit, size, curTime,
	)

	if !hit {
		cache.queue = append(
			cache.queue,
			cache.stats[filename],
		)
		added = true
	}

	queueSize := cache.getQueueSize()
	if queueSize > cache.MaxSize {
		// Update weights
		for _, curFileStats := range cache.queue {
			curFileStats.updateWeight(cache.SelFunctionType, cache.Exp, curTime)
		}
		// Sort queue
		sort.Slice(
			cache.queue,
			func(i, j int) bool {
				return cache.queue[i].weight < cache.queue[j].weight
			},
		)
		// Remove files
		for {
			if queueSize <= cache.MaxSize {
				break
			}
			elmRemoved := cache.removeLast()

			if elmRemoved.filename == filename {
				added = false
			} else {
				cache.size -= cache.files[elmRemoved.filename]
				delete(cache.files, elmRemoved.filename)
			}

			queueSize -= cache.stats[elmRemoved.filename].size
		}
	}

	if added {
		cache.files[filename] = size
		cache.size += size
	}

	return added
}

// GetLatestDecision returns the latest decision of the cache
func (cache *WeightedCache) GetLatestDecision() (bool, bool) {
	return cache.latestHitDecision, cache.latestAddDecision
}

// Get a file from the cache updating the statistics
func (cache *WeightedCache) Get(filename string, size float32) bool {
	hit := cache.check(filename)
	added := cache.updatePolicy(filename, size, hit)

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
func (cache WeightedCache) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// HitOverMiss of the cache
func (cache WeightedCache) HitOverMiss() float32 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache WeightedCache) WeightedHitRate() float32 {
	return cache.HitRate() * cache.readOnHit
}

// Size of the cache
func (cache WeightedCache) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache WeightedCache) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// WrittenData of the cache
func (cache WeightedCache) WrittenData() float32 {
	return cache.writtenData
}

// ReadOnHit of the cache
func (cache WeightedCache) ReadOnHit() float32 {
	return cache.readOnHit
}

func (cache WeightedCache) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
