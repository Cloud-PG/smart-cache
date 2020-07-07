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
	QueueIdx  int     `json:"queueIdx"`
}

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
	files        map[int64]*FileSupportData
	queue        []int64
	qType        queueType
	FrequencySum float64
	SizeSum      float64
	dataCh       chan *FileSupportData
	stopCh       chan bool
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileSupportData, 0)
	man.queue = make([]int64, 0)
	man.qType = qType
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
func (man Manager) Get(vars ...interface{}) chan *FileSupportData {
	var (
		totSize float64
		sended  float64
	)
	switch {
	case len(vars) > 1:
		panic("ERROR: too many passed arguments...")
	case len(vars) > 0:
		totSize = vars[0].(float64)
	}
	man.dataCh = make(chan *FileSupportData)
	go func() {
		defer close(man.dataCh)
		// Filtering trick
		// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
		if man.qType != NoQueue {
			for idx := len(man.queue) - 1; idx > -1; idx-- {
				filename := man.queue[idx]
				curFile := man.files[filename]
				man.dataCh <- curFile
				if totSize != 0. {
					sended += curFile.Size
					if sended >= totSize {
						break
					}
				}
			}
		} else {
			for _, fileSupportData := range man.files {
				man.dataCh <- fileSupportData
				if totSize != 0. {
					sended += fileSupportData.Size
					if sended >= totSize {
						break
					}
				}
			}
		}
	}()
	return man.dataCh
}

// Remove a file already in queue
func (man *Manager) Remove(files []int64) {
	for _, file := range files {
		curFile := man.files[file]
		man.SizeSum -= curFile.Size
		man.FrequencySum -= float64(curFile.Frequency)
		delete(man.files, file)
	}
	if man.qType != NoQueue {
		targetIdx := len(man.queue) - len(files)
		man.queue = man.queue[:targetIdx]
	}
}

// Insert a file into the queue manager
func (man *Manager) Insert(file *FileSupportData) {
	file.QueueIdx = -1
	man.files[file.Filename] = file
	man.SizeSum += file.Size
	man.FrequencySum += float64(file.Frequency)

	if man.qType != NoQueue {
		var insertIdx = -1
		switch man.qType {
		case LRUQueue:
			insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.files[man.queue[idx]].Recency < file.Recency })
		case LFUQueue:
			insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.files[man.queue[idx]].Frequency < file.Frequency })
		case SizeBigQueue:
			insertIdx = sort.Search(len(man.queue), func(idx int) bool {
				curFile := man.files[man.queue[idx]]
				if curFile.Size == file.Size {
					return curFile.Recency < file.Recency
				}
				return curFile.Size > file.Size

			})
		case SizeSmallQueue:
			insertIdx = sort.Search(len(man.queue), func(idx int) bool {
				curFile := man.files[man.queue[idx]]
				if curFile.Size == file.Size {
					return curFile.Recency < file.Recency
				}
				return curFile.Size < file.Size
			})
		case WeightQueue:
			insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.files[man.queue[idx]].Weight < file.Weight })
		}
		if insertIdx == len(man.queue) {
			file.QueueIdx = len(man.queue)
			man.queue = append(man.queue, file.Filename)
		} else {
			// Trick
			// https://github.com/golang/go/wiki/SliceTricks#insert
			man.queue = append(man.queue, -1)
			copy(man.queue[insertIdx+1:], man.queue[insertIdx:])
			man.queue[insertIdx] = file.Filename
			for idx := insertIdx; idx < len(man.queue); idx++ {
				man.files[man.queue[idx]].QueueIdx = idx
			}
		}
	}
}

// Update a file into the queue manager
func (man *Manager) Update(file *FileSupportData) error {
	curFile, inCache := man.files[file.Filename]
	if inCache {
		if man.qType != NoQueue {
			curQueueIdx := man.files[curFile.Filename].QueueIdx
			// Delete trick
			// -> https://github.com/golang/go/wiki/SliceTricks#delete
			man.queue = append(man.queue[:curQueueIdx], man.queue[curQueueIdx+1:]...)
		}
		man.SizeSum -= curFile.Size
		man.FrequencySum -= float64(curFile.Frequency)
		delete(man.files, curFile.Filename)
		man.Insert(file)
		return nil
	}
	return errors.New("File not in manager")
}
