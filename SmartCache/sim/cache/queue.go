package cache

import (
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
	files            map[int64]*FileSupportData
	queue            []*FileSupportData
	qType            queueType
	buffer           []*FileSupportData
	noQueueUpdateIdx int
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileSupportData)
	man.queue = make([]*FileSupportData, 0)
	man.buffer = make([]*FileSupportData, 0)
	man.qType = qType
	man.noQueueUpdateIdx = -1
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
func (man Manager) Get(vars ...interface{}) []*FileSupportData {
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

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	if man.qType != NoQueue {
		for idx := len(man.queue) - 1; idx > -1; idx-- {
			curFile := man.queue[idx]
			man.buffer = append(man.buffer, man.queue[idx])
			if totSize != 0. {
				sended += curFile.Size
				if sended >= totSize {
					break
				}
			}
		}
	} else {
		man.buffer = make([]*FileSupportData, len(man.queue))
		copy(man.buffer, man.queue)
	}

	return man.buffer
}

// Remove a file already in queue
func (man *Manager) Remove(files []int64, onUpdate bool) {
	// fmt.Println("REMOVE: ", files)
	if len(files) > 0 {
		if man.qType != NoQueue && !onUpdate {
			targetIdx := len(man.queue) - len(files)
			for idx := targetIdx; idx < len(man.queue); idx++ {
				man.queue[idx] = nil
			}
			man.queue = man.queue[:targetIdx]
		} else if man.qType == NoQueue && onUpdate && man.noQueueUpdateIdx != -1 {
			copy(man.queue[man.noQueueUpdateIdx:], man.queue[man.noQueueUpdateIdx+1:])
			man.queue[len(man.queue)-1] = nil // or the zero value of T
			man.queue = man.queue[:len(man.queue)-1]
			man.noQueueUpdateIdx = -1
			// fmt.Println("NOQUEUE --- ")
		} else {
			index2Remove := make([]int, len(files))
			for idx, filename := range files {
				// fmt.Println(man.files[filename].QueueIdx)
				index2Remove[idx] = man.files[filename].QueueIdx
			}
			sort.Sort(sort.Reverse(sort.IntSlice(index2Remove)))
			// fmt.Println(index2Remove)
			// for idx := 0; idx < len(man.queue); idx++ {
			// 	fmt.Println(man.queue[idx])
			// }
			// fmt.Println("---")
			for _, curIdx := range index2Remove {
				copy(man.queue[curIdx:], man.queue[curIdx+1:])
				man.queue[len(man.queue)-1] = nil // or the zero value of T
				man.queue = man.queue[:len(man.queue)-1]
			}
			if len(man.queue) != 0 {
				// fmt.Println("START UPDATE IDX")
				for idx := index2Remove[len(index2Remove)-1]; idx < len(man.queue); idx++ {
					// fmt.Println(man.queue[idx].Filename, "from:", man.queue[idx].QueueIdx, "->", idx)
					man.queue[idx].QueueIdx = idx
				}
				// fmt.Println("END UPDATE IDX")
			}
		}
		for _, file := range files {
			// fmt.Println("REMOVE MAP: ", file)
			delete(man.files, file)
		}
	}
}

// Insert a file into the queue manager
func (man *Manager) Insert(file *FileSupportData) {
	// Force inserto check
	// _, inCache := man.files[file.Filename]
	// if inCache {
	// 	panic("ERROR: File already in manager...")
	// }
	// fmt.Println("INSERT: ", file.Filename)

	man.files[file.Filename] = file

	var insertIdx = -1
	switch man.qType {
	case NoQueue:
		if man.noQueueUpdateIdx == -1 {
			insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Filename < file.Filename })
		} else {
			insertIdx = man.noQueueUpdateIdx
		}
	case LRUQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Recency < file.Recency })
	case LFUQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Frequency < file.Frequency })
	case SizeBigQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			curFile := man.queue[idx]
			if curFile.Size == file.Size {
				return curFile.Recency < file.Recency
			}
			return curFile.Size > file.Size

		})
	case SizeSmallQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			curFile := man.queue[idx]
			if curFile.Size == file.Size {
				return curFile.Recency < file.Recency
			}
			return curFile.Size < file.Size
		})
	case WeightQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool { return man.queue[idx].Weight < file.Weight })
	}
	if insertIdx == len(man.queue) {
		file.QueueIdx = len(man.queue)
		man.queue = append(man.queue, file)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		man.queue = append(man.queue, nil)
		copy(man.queue[insertIdx+1:], man.queue[insertIdx:])
		man.queue[insertIdx] = file
		if man.qType == NoQueue && man.noQueueUpdateIdx != -1 {
			man.queue[insertIdx].QueueIdx = insertIdx
		} else {
			for idx := insertIdx; idx < len(man.queue); idx++ {
				man.queue[idx].QueueIdx = idx
			}
		}
	}
}

// Update a file into the queue manager
func (man *Manager) Update(file *FileSupportData) {
	// fmt.Println("UPDATE: ", file.Filename)
	if man.qType == NoQueue {
		man.noQueueUpdateIdx = man.files[file.Filename].QueueIdx
	}
	man.Remove([]int64{file.Filename}, true)
	man.Insert(file)
}
