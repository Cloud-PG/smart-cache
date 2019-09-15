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

const (
	numCoRoutines4ReIndex int = 8
)

// WeightedLRU cache
type WeightedLRU struct {
	files                                                 map[string]float32
	stats                                                 []*weightedFileStats
	statsFilenames                                        map[string]int
	queue                                                 *list.List
	hit, miss, writtenData, readOnHit, size, MaxSize, Exp float32
	SelFunctionType                                       FunctionType
	SelUpdateStatPolicyType                               UpdateStatsPolicyType
	SelUpdateWeightPolicyType                             UpdateWeightPolicyType
	SelLimitStatsPolicyType                               LimitStatsPolicyType
}

// Init the WeightedLRU struct
func (cache *WeightedLRU) Init(vars ...interface{}) {
	cache.files = make(map[string]float32)
	cache.stats = make([]*weightedFileStats, 0)
	cache.statsFilenames = make(map[string]int)
	cache.queue = list.New()
}

// Clear the WeightedLRU struct
func (cache *WeightedLRU) Clear() {
	cache.files = make(map[string]float32)
	cache.stats = make([]*weightedFileStats, 0)
	cache.statsFilenames = make(map[string]int)
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
		curFile := &pb.SimFileWeight{
			Filename: stats.filename,
			Weight:   stats.weight,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}

	return nil
}

func updateWeightSingleFile(curStats *weightedFileStats, functionType FunctionType, exp float32, curTime time.Time, curWg *sync.WaitGroup) {
	curStats.updateWeight(
		functionType,
		exp,
		curTime,
	)
	curWg.Done()
}

func (cache *WeightedLRU) updateWeights() {
	wg := sync.WaitGroup{}
	curTime := time.Now()
	for idx := 0; idx < len(cache.stats); idx++ {
		wg.Add(1)
		go updateWeightSingleFile(cache.stats[idx], cache.SelFunctionType, cache.Exp, curTime, &wg)
	}
	wg.Wait()
}

func (cache *WeightedLRU) reIndex(numCoRoutines int) {
	chunkSize := len(cache.stats) / numCoRoutines
	waitGroup := sync.WaitGroup{}
	for idx := 0; idx < numCoRoutines; idx++ {
		waitGroup.Add(1)
		go func(stats []*weightedFileStats, fileMap map[string]int, startIdx int, chunkSize int, wg *sync.WaitGroup) {
			start := startIdx * chunkSize
			stop := startIdx*chunkSize + chunkSize
			if stop > len(stats) {
				stop = len(stats)
			}
			for curIdx := start; curIdx < stop; curIdx++ {
				curStatFilename := stats[curIdx].filename
				if fileMap[curStatFilename] != curIdx {
					fileMap[curStatFilename] = curIdx
				}
			}
			wg.Done()
		}(cache.stats, cache.statsFilenames, idx, chunkSize, &waitGroup)
	}
	waitGroup.Wait()
}

func (cache *WeightedLRU) getThreshold() float32 {
	if len(cache.stats) == 0 {
		return 0.0
	}

	if cache.SelUpdateWeightPolicyType == UpdateAllWeights {
		cache.updateWeights()
	}

	// Order from the highest weight to the smallest
	if !sort.IsSorted(ByWeight(cache.stats)) {
		sort.Sort(ByWeight(cache.stats))
		// Force to reindex
		cache.reIndex(numCoRoutines4ReIndex)
	}

	Q2 := cache.stats[int(math.Floor(float64(0.5*float32(len(cache.stats)))))].weight

	if cache.Capacity() > 75. {
		if cache.SelLimitStatsPolicyType == Q1IsDoubleQ2LimitStats {
			Q1Idx := int(math.Floor(float64(0.25 * float32(len(cache.stats)))))
			Q1 := cache.stats[Q1Idx].weight
			if Q1 > 2.*Q2 {
				for idx := 0; idx < Q1Idx; idx++ {
					delete(cache.statsFilenames, cache.stats[idx].filename)
				}
				copy(cache.stats, cache.stats[Q1Idx:])
				cache.stats = cache.stats[:len(cache.stats)-1]
				// Force to reindex
				cache.reIndex(numCoRoutines4ReIndex)
			}
		}
	}

	return Q2
}

func (cache *WeightedLRU) getOrInsertStats(filename string) *weightedFileStats {
	var result *weightedFileStats
	if _, inStats := cache.statsFilenames[filename]; !inStats {
		cache.stats = append(cache.stats, &weightedFileStats{
			filename,
			0.,
			0.,
			0.,
			0,
			0,
			time.Now(),
			[StatsMemorySize]time.Time{},
			0,
			float32(math.NaN()),
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

	if cache.SelUpdateStatPolicyType == UpdateStatsOnRequest {
		curStats = cache.getOrInsertStats(filename)
		curStats.updateStats(
			hit, curStats.totRequests+1, size, currentTime, float32(math.NaN()),
		)
		if cache.SelUpdateWeightPolicyType == UpdateSingleWeight {
			curStats.updateWeight(cache.SelFunctionType, cache.Exp, time.Time{})
		}
	}

	if !hit {
		if cache.SelUpdateStatPolicyType == UpdateStatsOnMiss {
			curStats = cache.getOrInsertStats(filename)
			curStats.updateStats(
				hit, curStats.totRequests+1, size, currentTime, float32(math.NaN()),
			)
			if cache.SelUpdateWeightPolicyType == UpdateSingleWeight {
				curStats.updateWeight(cache.SelFunctionType, cache.Exp, time.Time{})
			}
		}

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
