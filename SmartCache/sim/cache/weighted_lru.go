package cache

import (
	"container/list"
	"context"
	"math"
	"sort"
	"sync"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// WeightedLRU cache
type WeightedLRU struct {
	files                                                 map[string]float32
	stats                                                 []*weightedFileStats
	statsFilenames                                        map[string]int
	statsWaitGroup                                        sync.WaitGroup
	queue                                                 *list.List
	hit, miss, writtenData, readOnHit, size, MaxSize, exp float32
	functionType                                          FunctionType
}

// Init the LRU struct
func (cache *WeightedLRU) Init(vars ...interface{}) {
	if len(vars) < 2 {
		panic("ERROR: you need to specify the weighted function to use and the exponent...")
	}
	cache.files = make(map[string]float32)
	cache.stats = make([]*weightedFileStats, 0)
	cache.statsFilenames = make(map[string]int)
	cache.statsWaitGroup = sync.WaitGroup{}
	cache.queue = list.New()
	cache.functionType = vars[0].(FunctionType)
	cache.exp = vars[1].(float32)
}

// Clear the LRU struct
func (cache *WeightedLRU) Clear() {
	cache.files = make(map[string]float32)
	cache.stats = make([]*weightedFileStats, 0)
	cache.statsFilenames = make(map[string]int)
	cache.statsWaitGroup = sync.WaitGroup{}
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
	for idx := 0; idx < len(cache.stats); idx++ {
		curStats := cache.stats[idx]
		curFile := &pb.SimFileStats{
			Filename: curStats.filename,
			Size:     curStats.size,
			TotReq:   curStats.totRequests,
			NHits:    curStats.nHits,
			NMiss:    curStats.nMiss,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}
	return nil
}

// SimGetInfoFilesWeights returns the file weights
func (cache *WeightedLRU) SimGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesWeightsServer) error {
	for idx := 0; idx < len(cache.stats); idx++ {
		stats := cache.stats[idx]
		filename := stats.filename

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

func (cache *WeightedLRU) getThreshold() float32 {
	if len(cache.stats) == 0 {
		return 0.0
	}

	for _, stats := range cache.stats {
		cache.statsWaitGroup.Add(1)

		go func(curStats *weightedFileStats, wg *sync.WaitGroup) {
			var weight float32

			switch cache.functionType {
			case FuncFileWeight:
				weight = fileWeight(
					curStats.size,
					curStats.totRequests,
					cache.exp,
				)
			case FuncFileWeightAndTime:
				weight = fileWeightAndTime(
					curStats.size,
					curStats.totRequests,
					cache.exp,
					curStats.lastTimeRequested,
				)
			case FuncFileWeightOnlyTime:
				weight = fileWeightOnlyTime(
					curStats.totRequests,
					cache.exp,
					curStats.lastTimeRequested,
				)
			case FuncWeightedRequests:
				weight = fileWeightedRequest(
					curStats.size,
					curStats.totRequests,
					curStats.getMeanReqTimes(time.Now()),
					cache.exp,
				)
			}
			curStats.weight = weight
			wg.Done()
		}(stats, &cache.statsWaitGroup)
	}

	cache.statsWaitGroup.Wait()

	// Order from the highest weight to the smallest
	sort.Slice(
		cache.stats,
		func(i, j int) bool {
			return cache.stats[i].weight > cache.stats[j].weight
		},
	)
	Q2 := cache.stats[int(math.Floor(float64(0.5*float32(len(cache.stats)))))].weight
	return Q2
}

func (cache *WeightedLRU) getOrInsertStats(filename string, size float32) *weightedFileStats {
	var result *weightedFileStats
	if _, inStats := cache.statsFilenames[filename]; !inStats {
		cache.stats = append(cache.stats, &weightedFileStats{
			filename,
			-1,
			size,
			0.,
			0,
			0,
			time.Now(),
			[StatsMemorySize]time.Time{},
			0,
		})
		cache.statsFilenames[filename] = len(cache.stats) - 1
		result = cache.stats[len(cache.stats)-1]
	} else {
		if cache.stats[cache.statsFilenames[filename]].filename != filename {
			for idx := 0; idx < len(cache.stats); idx++ {
				if cache.stats[idx].filename == filename {
					cache.statsFilenames[filename] = idx
					break
				}
			}
		}
		result = cache.stats[cache.statsFilenames[filename]]
	}
	return result
}

func (cache *WeightedLRU) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	var currentTime = time.Now()
	var curStats *weightedFileStats

	curStats = cache.getOrInsertStats(filename, size)
	curStats.updateRequests(hit, currentTime)

	if !hit {
		var Q2 = cache.getThreshold()
		// If weight is higher exit and return added = false
		if curStats.weight > Q2 {
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
