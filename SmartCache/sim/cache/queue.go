package cache

import (
	"errors"
	"sort"
)

// Other policy utils

// FileSupportData is a struct used to manae files in cache (useful for the queues)
type FileSupportData struct {
	Filename  int64   `json:"filename"`
	Frequency int64   `json:"Frequency"`
	Size      float64 `json:"Size"`
	Recency   int64   `json:"Recency"`
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

// Order from the oldest to the yougest
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
func (slice ByBigSize) Less(i, j int) bool { return slice[i].Size < slice[j].Size }
func (slice ByBigSize) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

// BySmallSize implements sort.Interface based on the size field.
type BySmallSize []*FileSupportData

func (slice BySmallSize) Len() int { return len(slice) }

// Order from the smallest size to the biggest
func (slice BySmallSize) Less(i, j int) bool { return slice[i].Size > slice[j].Size }
func (slice BySmallSize) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

type queueType int

const (
	// LRUQueue is the LRU queue type
	LRUQueue queueType = iota - 4
	// LFUQueue is the LFU queue type
	LFUQueue
	// SizeBigQueue is the SizeBig queue type
	SizeBigQueue
	// SizeSmallQueue is the SizeSmall queue type
	SizeSmallQueue
)

// Manager manages the files in cache
type Manager struct {
	files map[int64]*FileSupportData
}

// Init initialize the struct
func (man *Manager) Init(_ ...queueType) {
	man.files = make(map[int64]*FileSupportData, 0)
}

// Check if a file is in cache
func (man *Manager) Check(file int64) bool {
	_, inCache := man.files[file]
	return inCache
}

// Len returns the number of files in cache
func (man *Manager) Len() int {
	return len(man.files)
}

// Get values from a queue
func (man Manager) Get(queue queueType) chan *FileSupportData {
	ch := make(chan *FileSupportData)
	go func() {
		defer close(ch)

		switch queue {
		case LRUQueue:
			var curQueue ByRecency
			for _, file := range man.files {
				curQueue = append(curQueue, file)
			}
			sort.Sort(curQueue)
			for _, file := range curQueue {
				ch <- file
			}
		case LFUQueue:
			var curQueue ByFrequency
			for _, file := range man.files {
				curQueue = append(curQueue, file)
			}
			sort.Sort(curQueue)
			for _, file := range curQueue {
				ch <- file
			}
		case SizeBigQueue:
			var curQueue ByBigSize
			for _, file := range man.files {
				curQueue = append(curQueue, file)
			}
			sort.Sort(curQueue)
			for _, file := range curQueue {
				ch <- file
			}
		case SizeSmallQueue:
			var curQueue BySmallSize
			for _, file := range man.files {
				curQueue = append(curQueue, file)
			}
			sort.Sort(curQueue)
			for _, file := range curQueue {
				ch <- file
			}
		}
	}()
	return ch
}

// Remove a file already in queue
func (man *Manager) Remove(files []int64) {
	for _, file := range files {
		delete(man.files, file)
	}
}

// Insert a file into the queue manager
func (man *Manager) Insert(file FileSupportData) error {
	_, inCache := man.files[file.Filename]
	if !inCache {
		man.files[file.Filename] = &file
		return nil
	}
	return errors.New("File already in manager")
}

// Update a file into the queue manager
func (man *Manager) Update(file FileSupportData) error {
	_, inCache := man.files[file.Filename]
	if inCache {
		man.files[file.Filename] = &file
		return nil
	}
	return errors.New("File not in manager")
}
