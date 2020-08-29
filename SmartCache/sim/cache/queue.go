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

func (sup *FileSupportData) Clean() {
	sup.QueueIdx = -1
}

type queueType int

const (
	// LRUQueue is the LRU queue type
	LRUQueue queueType = iota - 6
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
	files  map[int64]*FileSupportData
	queue  []*FileSupportData
	qType  queueType
	buffer []*FileSupportData
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileSupportData)
	man.queue = make([]*FileSupportData, 0)
	man.buffer = make([]*FileSupportData, 0)
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
func (man Manager) GetFileSupportData(id int64) *FileSupportData {
	return man.files[id]
}

// GetQueue values from a queue
func (man Manager) GetQueue() []*FileSupportData {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	man.buffer = make([]*FileSupportData, len(man.queue))
	copy(man.buffer, man.queue)

	return man.buffer
}

// GetFromWorst values from worst queue values
func (man Manager) GetFromWorst() []*FileSupportData {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	man.buffer = make([]*FileSupportData, len(man.queue))
	copy(man.buffer, man.queue)
	for left, right := 0, len(man.buffer)-1; left < right; left, right = left+1, right-1 {
		man.buffer[left], man.buffer[right] = man.buffer[right], man.buffer[left]
	}

	return man.buffer
}

// GetWorstFilesUp2Size values from a queue until size is reached
func (man Manager) GetWorstFilesUp2Size(totSize float64) []*FileSupportData {
	var sended float64

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

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

	return man.buffer
}

// Remove a file already in queue
func (man *Manager) Remove(files []int64) {
	if len(files) > 0 {
		// _, file, no, _ := runtime.Caller(1)
		// fmt.Printf("called from %s#%d\n", file, no)
		// fmt.Println("[QUEUE] REMOVE OnUpdate[", onUpdate, "]: ", files)
		index2Remove := make([]int, len(files))
		for idx, filename := range files {
			// fmt.Println(man.files[filename].QueueIdx)
			index2Remove[idx] = man.files[filename].QueueIdx
		}
		sort.Sort(sort.Reverse(sort.IntSlice(index2Remove)))
		for idx, curIdx := range index2Remove {
			copy(man.queue[curIdx:], man.queue[curIdx+1:])
			man.queue[len(man.queue)-(idx+1)] = nil // or the zero value of T
		}
		man.queue = man.queue[:len(man.queue)-len(index2Remove)]
		if len(man.queue) != 0 {
			// fmt.Println("START UPDATE IDX")
			for idx := index2Remove[len(index2Remove)-1]; idx < len(man.queue); idx++ {
				// fmt.Println(man.queue[idx].Filename, "from:", man.queue[idx].QueueIdx, "->", idx)
				man.queue[idx].QueueIdx = idx
			}
			// fmt.Println("END UPDATE IDX")
		}
		for _, filename := range files {
			// fmt.Println("REMOVE MAP: ", file)
			delete(man.files, filename)
		}
	}
}

// Insert a file into the queue manager
func (man *Manager) Insert(file *FileSupportData) {
	file.Clean()

	// Force inserto check
	_, inCache := man.files[file.Filename]
	if inCache {
		panic("ERROR: File already in manager...")
	}
	// fmt.Println("[QUEUE] INSERT: ", file.Filename)

	man.files[file.Filename] = file

	var insertIdx = -1
	switch man.qType {
	case NoQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.queue[idx].Filename < file.Filename
		})
	case LRUQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.queue[idx].Recency < file.Recency
		})
	case LFUQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.queue[idx].Frequency < file.Frequency
		})
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
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.queue[idx].Weight < file.Weight
		})
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
		for idx := insertIdx; idx < len(man.queue); idx++ {
			man.queue[idx].QueueIdx = idx
		}
	}
}

// Update a file into the queue manager
func (man *Manager) Update(file *FileSupportData) {
	file.Clean()

	curIdx := man.files[file.Filename].QueueIdx
	file.QueueIdx = curIdx

	man.files[file.Filename] = file

	if man.qType == NoQueue {
		man.queue[curIdx] = file
		return
	}

	newIdx := -1
	switch man.qType {
	case LRUQueue:
		// Get new index
		newIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.queue[idx].Recency < file.Recency
		})
	case LFUQueue:
		// Get new index
		newIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.queue[idx].Frequency < file.Frequency
		})
	case SizeBigQueue:
		// Get new index
		newIdx = sort.Search(len(man.queue), func(idx int) bool {
			curFile := man.queue[idx]
			if curFile.Size == file.Size {
				return curFile.Recency < file.Recency
			}
			return curFile.Size > file.Size

		})
	case SizeSmallQueue:
		// Get new index
		newIdx = sort.Search(len(man.queue), func(idx int) bool {
			curFile := man.queue[idx]
			if curFile.Size == file.Size {
				return curFile.Recency < file.Recency
			}
			return curFile.Size < file.Size
		})
	case WeightQueue:
		// Get new index
		newIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.queue[idx].Weight < file.Weight
		})
	}
	// fmt.Println("UPDATE:", file.Filename, curIdx, "->", newIdx)
	// fmt.Println("--- BEFORE ---")
	// for idx, sup := range man.queue {
	// 	fmt.Println("-[", idx, "]", sup.Filename, sup.Size)
	// }
	if newIdx != -1 {
		if newIdx != curIdx {
			if newIdx == len(man.queue) {
				file.QueueIdx = len(man.queue)
				man.queue = append(man.queue, file)
			} else {
				// Trick
				// https://github.com/golang/go/wiki/SliceTricks#insert
				man.queue = append(man.queue, nil)
				copy(man.queue[newIdx+1:], man.queue[newIdx:])
				man.queue[newIdx] = file
			}

			if newIdx < curIdx {
				curIdx++
			}

			// Remove file in old position
			copy(man.queue[curIdx:], man.queue[curIdx+1:])
			man.queue[len(man.queue)-1] = nil
			man.queue = man.queue[:len(man.queue)-1]

			startFrom := 0
			if newIdx > curIdx {
				startFrom = curIdx
			} else {
				startFrom = newIdx
			}
			for idx := startFrom; idx < len(man.queue); idx++ {
				man.queue[idx].QueueIdx = idx
			}
		} else {
			man.queue[curIdx] = file
		}
	}
	// fmt.Println("--- AFTER ---")
	// for idx, sup := range man.queue {
	// 	fmt.Println("-[", idx, "]", sup.Filename, sup.Size)
	// }
	// fmt.Println("--- DONE ---")
}
