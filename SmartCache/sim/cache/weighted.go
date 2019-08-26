package cache

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// FunctionType is used to select the weight function
type FunctionType int

const (
	// FuncFileGroupWeight indicates the simple function for weighted cache
	FuncFileGroupWeight FunctionType = iota
	// FuncFileGroupWeightAndTime indicates the function that uses time
	FuncFileGroupWeightAndTime
)

type weightedFile struct {
	filename string
	group    string
	weight   float32
}

type fileStats struct {
	size              float32
	numRequests       float32
	lastTimeRequested time.Time
}

type groupFiles struct {
	files map[string]*fileStats
}

// Weighted cache
type Weighted struct {
	files                                            map[string]float32
	groups                                           map[string]*groupFiles
	queue                                            []*weightedFile
	hit, miss, writtenData, readOnHit, size, MaxSize float32
	functionType                                     FunctionType
}

// Init the LRU struct
func (cache *Weighted) Init(vars ...interface{}) {
	cache.files = make(map[string]float32)
	cache.groups = make(map[string]*groupFiles)
	cache.queue = make([]*weightedFile, 0)
	cache.functionType = vars[0].(FunctionType)
}

// Clear the LRU struct
func (cache *Weighted) Clear() {
	cache.files = make(map[string]float32)
	for _, value := range cache.groups {
		value.files = make(map[string]*fileStats)
	}
	cache.groups = make(map[string]*groupFiles)
	cache.queue = make([]*weightedFile, 0)
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.size = 0.
}

func fileGroupWeight(size float32, numFiles float32, numRequests float32, exp float32) float32 {
	return float32(math.Pow(float64((size*numFiles)/numRequests), float64(exp)))
}

func fileGroupWeightAndTime(size float32, numFiles float32, numRequests float32, exp float32, lastTimeRequested time.Time) float32 {
	currentTime := time.Now()
	deltaLastTimeRequested := float32(currentTime.Sub(lastTimeRequested) / time.Second)
	return float32(math.Pow(float64((size*numFiles)/numRequests), float64(exp))) + deltaLastTimeRequested
}

// SimServiceGet updates the cache from a protobuf message
func (cache *Weighted) SimServiceGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.SimCacheStatus, error) {
	cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		WrittenData: cache.WrittenData(),
		Capacity:    cache.Capacity(),
	}, nil
}

// SimServiceClear deletes all cache content
func (cache *Weighted) SimServiceClear(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		WrittenData: cache.WrittenData(),
		Capacity:    cache.Capacity(),
	}, nil
}

// SimServiceGetInfoCacheFiles returns the content of the cache: filenames and sizes
func (cache *Weighted) SimServiceGetInfoCacheFiles(_ *empty.Empty, stream pb.SimService_SimServiceGetInfoCacheFilesServer) error {
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

// SimServiceGetInfoFilesWeights returns the file weights
func (cache *Weighted) SimServiceGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimServiceGetInfoFilesWeightsServer) error {
	for _, group := range cache.groups {
		numFiles := float32(len(group.files))
		for filename, stats := range group.files {
			var weight float32

			switch cache.functionType {
			case FuncFileGroupWeight:
				weight = fileGroupWeight(
					stats.size,
					numFiles,
					stats.numRequests,
					2.0,
				)
			case FuncFileGroupWeightAndTime:
				weight = fileGroupWeightAndTime(
					stats.size,
					numFiles,
					stats.numRequests,
					2.0,
					stats.lastTimeRequested,
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
	}
	return nil
}

func getGroup(filename string) string {
	components := strings.Split(filename, "/")
	var group = "/"
	for _, value := range components {
		if value != "" {
			group += value + "/"
		}
	}
	return group
}

func (cache *Weighted) getQueueSize() float32 {
	var size float32
	for _, curFile := range cache.queue {
		size += cache.groups[curFile.group].files[curFile.filename].size
	}
	return size
}

func (cache *Weighted) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	var currentTime = time.Now()

	group := getGroup(filename)

	if _, inMap := cache.groups[group]; !inMap {
		cache.groups[group] = &groupFiles{
			make(map[string]*fileStats, 0),
		}
	}

	if _, inMap := cache.groups[group].files[filename]; !inMap {
		cache.groups[group].files[filename] = &fileStats{
			size,
			0.,
			currentTime,
		}
	}

	cache.groups[group].files[filename].numRequests += 1.
	cache.groups[group].files[filename].lastTimeRequested = currentTime

	groupNumFiles := float32(len(cache.groups[group].files))

	if !hit {
		cache.queue = append(
			cache.queue,
			&weightedFile{
				filename,
				group,
				-1.,
			},
		)
		added = true
	}

	for _, curFile := range cache.queue {
		if curFile.group == group {
			switch cache.functionType {
			case FuncFileGroupWeight:
				curFile.weight = fileGroupWeight(
					cache.groups[group].files[curFile.filename].size,
					groupNumFiles,
					cache.groups[group].files[curFile.filename].numRequests,
					2.0,
				)
			case FuncFileGroupWeightAndTime:
				curFile.weight = fileGroupWeightAndTime(
					cache.groups[group].files[curFile.filename].size,
					groupNumFiles,
					cache.groups[group].files[curFile.filename].numRequests,
					2.0,
					cache.groups[group].files[curFile.filename].lastTimeRequested,
				)
			}
		}
	}

	sort.Slice(
		cache.queue,
		func(i, j int) bool { return cache.queue[i].weight < cache.queue[j].weight },
	)

	queueSize := cache.getQueueSize()
	if queueSize > cache.MaxSize {
		for {
			if queueSize <= cache.MaxSize {
				break
			}
			lastElm := cache.queue[len(cache.queue)-1]
			if lastElm.filename == filename {
				added = false
			}
			queueSize -= cache.groups[lastElm.group].files[lastElm.filename].size
			if _, inCache := cache.files[lastElm.filename]; inCache == true {
				cache.size -= cache.files[lastElm.filename]
				delete(cache.files, lastElm.filename)
			}
			cache.queue = cache.queue[:len(cache.queue)-1]
		}
	}

	if added {
		cache.files[filename] = size
		cache.size += size
	}

	return added
}

// Get a file from the cache updating the statistics
func (cache *Weighted) Get(filename string, size float32) bool {
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
func (cache Weighted) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// Size of the cache
func (cache Weighted) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache Weighted) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// WrittenData of the cache
func (cache Weighted) WrittenData() float32 {
	return cache.writtenData
}

// ReadOnHit of the cache
func (cache Weighted) ReadOnHit() float32 {
	return cache.readOnHit
}

func (cache Weighted) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
