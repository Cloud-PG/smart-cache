package cache

import (
	"container/list"
	"context"
	"errors"
	"math"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

const (
	numCoRoutines4ReIndex int = 64
)

// WeightedLRU cache
type WeightedLRU struct {
	files                                                 map[string]float32
	stats                                                 []*WeightedFileStats
	statsFilenames                                        map[string]int
	queue                                                 *list.List
	hit, miss, writtenData, readOnHit, size, MaxSize, Exp float32
	SelFunctionType                                       FunctionType
	SelUpdateStatPolicyType                               UpdateStatsPolicyType
	SelLimitStatsPolicyType                               LimitStatsPolicyType
	latestHitDecision                                     bool
	latestAddDecision                                     bool
}

// Init the WeightedLRU struct
func (cache *WeightedLRU) Init(vars ...interface{}) {
	cache.files = make(map[string]float32)
	cache.stats = make([]*WeightedFileStats, 0)
	cache.statsFilenames = make(map[string]int)
	cache.queue = list.New()
}

// Clear the WeightedLRU struct
func (cache *WeightedLRU) Clear() {
	cache.files = make(map[string]float32)
	cache.stats = make([]*WeightedFileStats, 0)
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

// GetFileStats from the cache
func (cache *WeightedLRU) GetFileStats(filename string) (*DatasetInput, error) {
	index, inStats := cache.statsFilenames[filename]
	if !inStats {
		return nil, errors.New("The file is not in cache stats anymore")
	}
	stats := cache.stats[index]
	return &DatasetInput{
		stats.size,
		stats.totRequests,
		stats.nHits,
		stats.nMiss,
		stats.getMeanReqTimes(time.Time{}),
	}, nil
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

func (cache *WeightedLRU) reIndex() {
	for curIdx := 0; curIdx < len(cache.stats); curIdx++ {
		curStatFilename := cache.stats[curIdx].filename
		cache.statsFilenames[curStatFilename] = curIdx
	}
}

func (cache *WeightedLRU) getThreshold() float32 {
	if len(cache.stats) == 0 {
		return 0.0
	}

	Q2 := cache.stats[int(math.Floor(float64(0.5*float32(len(cache.stats)))))].weight

	if cache.Capacity() > 75. {
		if cache.SelLimitStatsPolicyType == Q1IsDoubleQ2LimitStats {
			Q1Idx := int(math.Floor(float64(0.25 * float32(len(cache.stats)))))
			Q1 := cache.stats[Q1Idx].weight
			if Q1 > 1.5*Q2 {
				deleteCount := 0
				for idx := 0; idx < Q1Idx; idx++ {
					delete(cache.statsFilenames, cache.stats[idx].filename)
					deleteCount++
				}
				copy(cache.stats, cache.stats[Q1Idx:])
				cache.stats = cache.stats[:len(cache.stats)-deleteCount]
				// Force to reindex
				cache.reIndex()
			}
		}
	}

	return Q2
}

func (cache *WeightedLRU) getOrInsertStats(filename string) *WeightedFileStats {
	idx, inStats := cache.statsFilenames[filename]
	if !inStats {
		cache.stats = append(cache.stats, &WeightedFileStats{
			filename,
			0.,
			0.,
			0,
			0,
			0,
			time.Now(),
			[StatsMemorySize]time.Time{},
			0,
		})
		cache.statsFilenames[filename] = len(cache.stats) - 1
		idx = len(cache.stats) - 1
	}
	return cache.stats[idx]
}

func (cache *WeightedLRU) moveStat(stat *WeightedFileStats, curTime time.Time) {
	curIdx, _ := cache.statsFilenames[stat.filename]

	if curIdx-1 >= 0 { // <--[Check left]
		for idx := curIdx; idx > 0; idx-- {
			curStats := cache.stats[idx]
			prevStats := cache.stats[idx-1]
			if prevStats.weight < curStats.weight {
				cache.stats[idx-1] = curStats
				cache.stats[idx] = prevStats
				cache.statsFilenames[curStats.filename] = idx - 1
				cache.statsFilenames[prevStats.filename] = idx
			} else {
				break
			}
		}
	}
	if curIdx+1 < len(cache.stats) { // [Check right]-->
		for idx := curIdx; idx < len(cache.stats)-1; idx++ {
			curStats := cache.stats[idx]
			nextStats := cache.stats[idx+1]
			if nextStats.weight > curStats.weight {
				cache.stats[idx+1] = curStats
				cache.stats[idx] = nextStats
				cache.statsFilenames[curStats.filename] = idx + 1
				cache.statsFilenames[nextStats.filename] = idx
			} else {
				break
			}
		}
	}
}

func (cache *WeightedLRU) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	var currentTime = time.Now()
	var curStats *WeightedFileStats

	if cache.SelUpdateStatPolicyType == UpdateStatsOnRequest {
		curStats = cache.getOrInsertStats(filename)
		curStats.updateStats(hit, size, currentTime)
		curStats.updateWeight(cache.SelFunctionType, cache.Exp, time.Time{})
		cache.moveStat(curStats, time.Time{})
	}

	if !hit {
		if cache.SelUpdateStatPolicyType == UpdateStatsOnMiss {
			curStats = cache.getOrInsertStats(filename)
			curStats.updateStats(hit, size, currentTime)
			curStats.updateWeight(cache.SelFunctionType, cache.Exp, time.Time{})
			cache.moveStat(curStats, time.Time{})
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

// GetLatestDecision returns the latest decision of the cache
func (cache *WeightedLRU) GetLatestDecision() (bool, bool) {
	return cache.latestHitDecision, cache.latestAddDecision
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

	cache.latestHitDecision = hit
	cache.latestAddDecision = added

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
