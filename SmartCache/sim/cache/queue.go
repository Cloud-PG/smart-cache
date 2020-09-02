package cache

import (
	"sort"
)

// Other policy utils

// FileSupportData is a struct used to manae files in cache (useful for the queues)
type FileSupportData struct {
	Filename  int64       `json:"filename"`
	Frequency int64       `json:"frequency"`
	Size      float64     `json:"size"`
	Recency   int64       `json:"recency"`
	Weight    float64     `json:"weight"`
	QueueIdx  int         `json:"queueIdx"`
	QueueKey  interface{} `json:"queueKey"`
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
	files       map[int64]*FileSupportData
	queue       map[interface{}][]int64
	orderedKeys []interface{}
	qType       queueType
	buffer      []*FileSupportData
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileSupportData)
	man.queue = make(map[interface{}][]int64)
	man.orderedKeys = make([]interface{}, 0)
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

	for ordIdx := len(man.orderedKeys) - 1; ordIdx > -1; ordIdx-- {
		key := man.orderedKeys[ordIdx]
		curQueue := man.queue[key]
		for _, filename := range curQueue {
			man.buffer = append(man.buffer, man.files[filename])
		}
	}

	return man.buffer
}

// GetFromWorst values from worst queue values
func (man Manager) GetFromWorst() []*FileSupportData {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	for _, key := range man.orderedKeys {
		curQueue := man.queue[key]
		for idx := len(curQueue) - 1; idx > -1; idx-- {
			filename := curQueue[idx]
			man.buffer = append(man.buffer, man.files[filename])
		}
	}

	// for _, file := range man.buffer {
	// 	fmt.Printf("%d | ", file.Filename)
	// }
	// fmt.Println()
	return man.buffer
}

// GetWorstFilesUp2Size values from a queue until size is reached
func (man Manager) GetWorstFilesUp2Size(totSize float64) []*FileSupportData {
	// var sended float64

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	// for idx := len(man.queue) - 1; idx > -1; idx-- {
	// 	curFile := man.queue[idx]
	// 	man.buffer = append(man.buffer, man.queue[idx])
	// 	if totSize != 0. {
	// 		sended += curFile.Size
	// 		if sended >= totSize {
	// 			break
	// 		}
	// 	}
	// }

	return man.buffer
}

func (man *Manager) updateIndexes(queue []int64, startFrom int) {
	for idx := startFrom; idx < len(queue); idx++ {
		filename := queue[idx]
		man.files[filename].QueueIdx = idx
	}
}

// Remove a file already in queue
func (man *Manager) Remove(files []int64) {
	// fmt.Println("--- 2 REMOVE ---", files)
	// fmt.Println("--- BEFORE ---")
	// fmt.Println(man.orderedKeys)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// for key, file := range man.files {
	// 	fmt.Printf("-[ %d ] -> %#v\n", key, file)
	// }

	if len(files) > 0 {
		// _, file, no, _ := runtime.Caller(1)
		// fmt.Printf("called from %s#%d\n", file, no)
		// fmt.Println("[QUEUE] REMOVE OnUpdate[", onUpdate, "]: ", files)
		queue2update := make(map[interface{}]int)

		for _, filename := range files {
			// fmt.Println("--- Removing ->", filename)
			curFile := man.files[filename]
			// fmt.Printf("--- file -> %#v\n", curFile)
			key := curFile.QueueKey
			idx := curFile.QueueIdx
			// fmt.Println("--- Coords ->", key, idx)

			curQueue := man.queue[key]

			// fmt.Println(curQueue)

			// Remove
			if len(curQueue) == 1 {
				curQueue = curQueue[:0]
			} else {
				copy(curQueue[idx:], curQueue[idx+1:])
				curQueue = curQueue[:len(curQueue)-1]
			}
			man.queue[key] = curQueue

			curVal, inList := queue2update[key]
			if !inList {
				queue2update[key] = idx
			} else if curVal > idx {
				queue2update[key] = idx
			}

			delete(man.files, filename)
		}

		for key, startFrom := range queue2update {
			man.updateIndexes(man.queue[key], startFrom)
		}
	}
}

func (man *Manager) getKey(file *FileSupportData) interface{} {
	var key interface{}
	switch man.qType {
	case NoQueue:
		key = file.Filename
	case LRUQueue:
		key = file.Recency
	case LFUQueue:
		key = file.Frequency
	case SizeBigQueue:
		key = file.Size
	case SizeSmallQueue:
		key = file.Size
	case WeightQueue:
		key = file.Weight
	}
	return key
}

func (man *Manager) insertKey(key interface{}) {
	var insertIdx = -1
	switch man.qType {
	case NoQueue, LRUQueue, LFUQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.orderedKeys[idx].(int64) > key.(int64)
		})
	case SizeBigQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.orderedKeys[idx].(float64) < key.(float64)
		})
	case SizeSmallQueue, WeightQueue:
		insertIdx = sort.Search(len(man.queue), func(idx int) bool {
			return man.orderedKeys[idx].(float64) > key.(float64)
		})
	}
	if insertIdx == len(man.queue) {
		man.orderedKeys = append(man.orderedKeys, key)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		man.orderedKeys = append(man.orderedKeys, nil)
		copy(man.orderedKeys[insertIdx+1:], man.orderedKeys[insertIdx:])
		man.orderedKeys[insertIdx] = key
	}
	man.queue[key] = make([]int64, 0)
}

func (man *Manager) insertInQueue(key interface{}, filename int64) int {
	curQueue := man.queue[key]
	curQueue = append(curQueue, filename)
	man.queue[key] = curQueue
	return len(curQueue) - 1
}

// Insert a file into the queue manager
func (man *Manager) Insert(file *FileSupportData) {
	// Force inserto check
	_, inCache := man.files[file.Filename]
	if inCache {
		panic("ERROR: File already in manager...")
	}

	key := man.getKey(file)

	_, inQueue := man.queue[key]
	if !inQueue {
		man.insertKey(key)
	}

	idx := man.insertInQueue(key, file.Filename)
	file.QueueIdx = idx
	file.QueueKey = key

	man.files[file.Filename] = file
	// fmt.Println("[QUEUE] INSERT: ", file)
}

// Update a file into the queue manager
func (man *Manager) Update(file *FileSupportData) {
	// fmt.Println("UPDATE:", file.Filename)
	// fmt.Println("--- BEFORE ---")
	// fmt.Println(man.orderedKeys)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }

	if man.qType != NoQueue {
		man.Remove([]int64{file.Filename})
		man.Insert(file)
	} else {
		curFile := man.files[file.Filename]
		file.QueueKey = curFile.QueueKey
		file.QueueIdx = curFile.QueueIdx
		man.files[file.Filename] = file
	}

	switch man.qType {
	case NoQueue:
		return
	}

	// fmt.Println("--- AFTER ---")
	// fmt.Println(man.orderedKeys)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// fmt.Println("--- DONE ---")
}
