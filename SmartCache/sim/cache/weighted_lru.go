package cache

import (
	"context"
	"sort"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// WeightedLRU cache
type WeightedLRU struct {
	files                                                 map[string]float32
	stats                                                 map[string]*weightedFileStats
	queue                                                 []*weightedFile
	hit, miss, writtenData, readOnHit, size, MaxSize, exp float32
	functionType                                          FunctionType
}

// Init the LRU struct
func (cache *WeightedLRU) Init(vars ...interface{}) {
	if len(vars) < 2 {
		panic("ERROR: you need to specify the weighted function to use and the exponent...")
	}
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*weightedFileStats)
	cache.queue = make([]*weightedFile, 0)
	cache.functionType = vars[0].(FunctionType)
	cache.exp = vars[1].(float32)
}

// Clear the LRU struct
func (cache *WeightedLRU) Clear() {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*weightedFileStats)
	cache.queue = make([]*weightedFile, 0)
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.readOnHit = 0.
	cache.size = 0.
}

// ClearHitMissStats the LRU struct
func (cache *WeightedLRU) ClearHitMissStats() {
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.readOnHit = 0.
}

// SimGet updates the cache from a protobuf message
func (cache *WeightedLRU) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimReset deletes all cache content
func (cache *WeightedLRU) SimReset(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
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
func (cache *WeightedLRU) SimResetHitMissStats(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
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
func (cache *WeightedLRU) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
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
func (cache *WeightedLRU) SimGetInfoCacheFiles(_ *empty.Empty, stream pb.SimService_SimGetInfoCacheFilesServer) error {
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
func (cache *WeightedLRU) SimGetInfoFilesStats(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesStatsServer) error {
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
func (cache *WeightedLRU) SimGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesWeightsServer) error {
	for filename, stats := range cache.stats {
		var weight float32

		switch cache.functionType {
		case FuncFileWeight:
			weight = fileWeight(
				stats.size,
				stats.totRequests,
				cache.exp,
			)
		case FuncFileWeightAndTime:
			weight = fileWeightAndTime(
				stats.size,
				stats.totRequests,
				cache.exp,
				stats.lastTimeRequested,
			)
		case FuncFileWeightOnlyTime:
			weight = fileWeightOnlyTime(
				stats.totRequests,
				cache.exp,
				stats.lastTimeRequested,
			)
		case FuncWeightedRequests:
			weight = fileWeightedRequest(
				stats.size,
				stats.totRequests,
				stats.getMeanReqTimes(time.Now()),
				cache.exp,
			)
		}

		curFile := &pb.SimFileWeight{
			Filename: filename,
			Weight:   weight,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}

	return nil
}

func (cache *WeightedLRU) getQueueSize() float32 {
	var size float32
	for _, curFile := range cache.queue {
		size += cache.stats[curFile.filename].size
	}
	return size
}

func (cache *WeightedLRU) removeLast() *weightedFile {
	removedElm := cache.queue[len(cache.queue)-1]
	cache.queue = cache.queue[:len(cache.queue)-1]
	return removedElm
}

func (cache *WeightedLRU) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	var currentTime = time.Now()

	if _, inMap := cache.stats[filename]; !inMap {
		cache.stats[filename] = &weightedFileStats{
			size,
			0.,
			0,
			0,
			currentTime,
			[StatsMemorySize]time.Time{},
			0,
		}
	}

	cache.stats[filename].updateRequests(hit, currentTime)

	if !hit {
		cache.queue = append(
			cache.queue,
			&weightedFile{
				filename,
				size,
				-1.,
			},
		)
		added = true
	}

	queueSize := cache.getQueueSize()
	if queueSize > cache.MaxSize {
		// Update weights
		for _, curFile := range cache.queue {
			curStats := cache.stats[curFile.filename]
			switch cache.functionType {
			case FuncFileWeight:
				curFile.weight = fileWeight(
					curStats.size,
					curStats.totRequests,
					cache.exp,
				)
			case FuncFileWeightAndTime:
				curFile.weight = fileWeightAndTime(
					curStats.size,
					curStats.totRequests,
					cache.exp,
					curStats.lastTimeRequested,
				)
			case FuncFileWeightOnlyTime:
				curFile.weight = fileWeightOnlyTime(
					curStats.totRequests,
					cache.exp,
					curStats.lastTimeRequested,
				)
			case FuncWeightedRequests:
				curFile.weight = fileWeightedRequest(
					curStats.size,
					curStats.totRequests,
					curStats.getMeanReqTimes(currentTime),
					cache.exp,
				)
			}
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

// Get a file from the cache updating the statistics
func (cache *WeightedLRU) Get(filename string, size float32) bool {
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

	return added
}

// HitRate of the cache
func (cache WeightedLRU) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// HitOverMiss of the cache
func (cache WeightedLRU) HitOverMiss() float32 {
	if cache.hit == 0. || cache.miss == 0. {
		return 0.
	}
	return cache.hit / cache.miss
}

// WeightedHitRate of the cache
func (cache WeightedLRU) WeightedHitRate() float32 {
	return cache.HitRate() * cache.readOnHit
}

// Size of the cache
func (cache WeightedLRU) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache WeightedLRU) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// WrittenData of the cache
func (cache WeightedLRU) WrittenData() float32 {
	return cache.writtenData
}

// ReadOnHit of the cache
func (cache WeightedLRU) ReadOnHit() float32 {
	return cache.readOnHit
}

func (cache WeightedLRU) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
