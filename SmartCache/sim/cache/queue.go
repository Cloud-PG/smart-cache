package cache

import (
	"errors"
	"sort"
)

// Other policy utils

// FileSupportData is a struct used to manae files in cache (useful for the queues)
type FileSupportData struct {
	Filename  int64   `json:"filename"`
	Frequency int64   `json:"frequency"`
	Size      float64 `json:"size"`
	Recency   int64   `json:"recency"`
	Weight    float64 `json:"weight"`
}

// ByName implements sort.Interface based on the filename field.
type ByName []*FileSupportData

func (slice ByName) Len() int { return len(slice) }

// Order from the oldest to the yougest
func (slice ByName) Less(i, j int) bool { return slice[i].Filename < slice[j].Filename }
func (slice ByName) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

// ByRecency implements sort.Interface based on the frequency field.
type ByRecency []*FileSupportData

func (slice ByRecency) Len() int { return len(slice) }

// Order from the oldest to the youngest
func (slice ByRecency) Less(i, j int) bool { return slice[i].Recency > slice[j].Recency }
func (slice ByRecency) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

// ByFrequency implements sort.Interface based on the frequency field.
type ByFrequency []*FileSupportData

func (slice ByFrequency) Len() int { return len(slice) }

// Order from the lower frequent to the highest
func (slice ByFrequency) Less(i, j int) bool { return slice[i].Frequency < slice[j].Frequency }
func (slice ByFrequency) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

// ByBigSize implements sort.Interface based on the size field.
type ByBigSize []*FileSupportData

func (slice ByBigSize) Len() int { return len(slice) }

// Order from the biggest size to the smallest
func (slice ByBigSize) Less(i, j int) bool {
	return slice[i].Size > slice[j].Size || slice[i].Recency > slice[j].Recency
}
func (slice ByBigSize) Swap(i, j int) { slice[i], slice[j] = slice[j], slice[i] }

// BySmallSize implements sort.Interface based on the size field.
type BySmallSize []*FileSupportData

func (slice BySmallSize) Len() int { return len(slice) }

// Order from the smallest size to the biggest
func (slice BySmallSize) Less(i, j int) bool {
	return slice[i].Size < slice[j].Size || slice[i].Recency > slice[j].Recency
}
func (slice BySmallSize) Swap(i, j int) { slice[i], slice[j] = slice[j], slice[i] }

// ByWeight implements sort.Interface based on the size field.
type ByWeight []*FileSupportData

func (slice ByWeight) Len() int { return len(slice) }

// Order from the heaviest to the lightest
func (slice ByWeight) Less(i, j int) bool { return slice[i].Weight > slice[j].Weight }
func (slice ByWeight) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

type queueType int

const (
	// LRUQueue is the LRU queue type
	LRUQueue queueType = iota - 5
	// LFUQueue is the LFU queue type
	LFUQueue
	// SizeBigQueue is the SizeBig queue type
	SizeBigQueue
	// SizeSmallQueue is the SizeSmall queue type
	SizeSmallQueue
	// WeightQueue is the SizeSmall queue type
	WeightQueue
	// NoQueue to return only the files
	NoQueue
)

// Manager manages the files in cache
type Manager struct {
	files              map[int64]*FileSupportData
	queue              []*FileSupportData
	qType              queueType
	FrequencySum       float64
	FrequencySumSquare float64
	SizeSum            float64
	SizeSumSquare      float64
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileSupportData, 0)
	man.queue = make([]*FileSupportData, 0)
}

// Check if a file is in cache
func (man Manager) Check(file int64) bool {
	_, inCache := man.files[file]
	return inCache
}

// Len returns the number of files in cache
func (man Manager) Len() int {
	return len(man.files)
}

// GetFile returns a specific file support data
func (man Manager) GetFile(id int64) *FileSupportData {
	return man.files[id]
}

// Get values from a queue
func (man Manager) Get() chan *FileSupportData {
	ch := make(chan *FileSupportData)
	go func() {
		defer close(ch)
		// Filtering trick
		// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
		if man.qType != NoQueue {
			for _, file := range man.queue {
				ch <- file
			}
		} else {
			for _, file := range man.files {
				ch <- file
			}
		}
	}()
	return ch
}

// Remove a file already in queue
func (man *Manager) Remove(files []int64) {
	indexToRemove := make([]int, 0)
	for idx, queueFile := range man.queue {
		for _, file := range files {
			if queueFile.Filename == file {
				indexToRemove = append(indexToRemove, idx)
				// Remove and update from manager
				curFile := man.files[file]
				man.SizeSum -= curFile.Size
				man.SizeSumSquare -= (curFile.Size * curFile.Size)
				man.FrequencySum -= float64(curFile.Frequency)
				man.FrequencySumSquare -= float64(curFile.Frequency * curFile.Frequency)
				delete(man.files, file)
				break
			}
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(indexToRemove)))
	for _, idxValue := range indexToRemove {
		// Remove from queue
		copy(man.queue[idxValue:], man.queue[idxValue+1:])
		man.queue[len(man.queue)-1] = nil // or the zero value of T
		man.queue = man.queue[:len(man.queue)-1]
	}
}

// Insert a file into the queue manager
func (man *Manager) Insert(file FileSupportData) {
	man.files[file.Filename] = &file
	man.SizeSum += file.Size
	man.SizeSumSquare += (file.Size * file.Size)
	man.FrequencySum += float64(file.Frequency)
	man.FrequencySumSquare += float64(file.Frequency * file.Frequency)
	var insertIdx = -1
	switch man.qType {
	case LRUQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Recency > file.Recency })
	case LFUQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Frequency > file.Frequency })
	case SizeBigQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Size > file.Size || man.queue[idx].Recency > file.Recency })
	case SizeSmallQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Size < file.Size || man.queue[idx].Recency > file.Recency })
	case WeightQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Recency > file.Recency })
	}
	if insertIdx == len(man.queue) || insertIdx == -1 {
		man.queue = append(man.queue, &file)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		man.queue = append(man.queue, nil)
		copy(man.queue[insertIdx+1:], man.queue[insertIdx:])
		man.queue[insertIdx] = &file
	}
}

// Update a file into the queue manager
func (man *Manager) Update(file FileSupportData) error {
	curFile, inCache := man.files[file.Filename]
	if inCache {
		// Remove old stats
		man.SizeSum -= curFile.Size
		man.SizeSumSquare -= (curFile.Size * curFile.Size)
		man.FrequencySum -= float64(curFile.Frequency)
		man.FrequencySumSquare -= float64(curFile.Frequency * curFile.Frequency)
		// Add new stats
		curFile.Frequency = file.Frequency
		curFile.Recency = file.Recency
		curFile.Size = file.Size
		curFile.Weight = file.Weight
		// Update sums
		man.SizeSum += file.Size
		man.SizeSumSquare += (file.Size * file.Size)
		man.FrequencySum += float64(file.Frequency)
		man.FrequencySumSquare += float64(file.Frequency * file.Frequency)
		return nil
	}
	return errors.New("File not in manager")
}
