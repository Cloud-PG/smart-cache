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
	files         map[int64]*FileSupportData
	queue         map[interface{}][]int64
	orderedValues []interface{}
	qType         queueType
	buffer        []*FileSupportData
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileSupportData)
	man.queue = make(map[interface{}][]int64)
	man.orderedValues = make([]interface{}, 0)
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

	for queueIdx := len(man.orderedValues) - 1; queueIdx > -1; queueIdx-- {
		key := man.orderedValues[queueIdx]
		switch man.qType {
		case LRUQueue, LFUQueue, SizeBigQueue, SizeSmallQueue:
			curQueue := man.queue[key]
			for _, filename := range curQueue {
				man.buffer = append(man.buffer, man.files[filename])
			}
		case NoQueue:
			man.buffer = append(man.buffer, man.files[key.(int64)])
		}
	}

	return man.buffer
}

// GetFromWorst values from worst queue values
func (man Manager) GetFromWorst() []*FileSupportData {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	// fmt.Println(man.orderedValues)
	for _, key := range man.orderedValues {
		// fmt.Println("KEY->", key.(int64))
		switch man.qType {
		case LRUQueue, LFUQueue, SizeBigQueue, SizeSmallQueue:
			curQueue := man.queue[key]
			for idx := len(curQueue) - 1; idx > -1; idx-- {
				filename := curQueue[idx]
				man.buffer = append(man.buffer, man.files[filename])
			}
		case NoQueue:
			// fmt.Println(man.files[key.(int64)])
			man.buffer = append(man.buffer, man.files[key.(int64)])
		}
	}

	// for _, file := range man.buffer {	// 	fmt.Printf("%d | ", file.Filename)
	// }
	// fmt.Println()
	return man.buffer
}

// GetWorstFilesUp2Size values from a queue until size is reached
func (man Manager) GetWorstFilesUp2Size(totSize float64) []*FileSupportData {
	if totSize <= 0. {
		panic("ERROR: tot size is negative or equal to 0")
	}
	var sended float64

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	for _, key := range man.orderedValues {
		switch man.qType {
		case LRUQueue, LFUQueue, SizeBigQueue, SizeSmallQueue:
			curQueue := man.queue[key]
			for idx := len(curQueue) - 1; idx > -1; idx-- {
				filename := curQueue[idx]
				curFile := man.files[filename]
				man.buffer = append(man.buffer, man.files[filename])
				sended += curFile.Size
				if sended >= totSize {
					break
				}
			}
		case NoQueue:
			curFile := man.files[key.(int64)]
			man.buffer = append(man.buffer, curFile)
		}
		if sended >= totSize {
			break
		}
	}

	// fmt.Println(totSize, sended, len(man.buffer))

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
	// fmt.Println(man.orderedValues)
	// fmt.Println("#-> QUEUE")
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// fmt.Println("#-> ORDERED KEYS")
	// for key, val := range man.orderedValues {
	// 	fmt.Println("-[", key, "]", val)
	// }
	// fmt.Println("#-> FILES")
	// for key, file := range man.files {
	// 	fmt.Printf("-[ %d ] -> %#v\n", key, file)
	// }

	if len(files) > 0 {
		// _, file, no, _ := runtime.Caller(1)
		// fmt.Printf("called from %s#%d\n", file, no)
		queue2update := make(map[interface{}]int)

		for _, filename := range files {
			// fmt.Println("--- Removing ->", filename)

			switch man.qType {
			case LRUQueue, LFUQueue, SizeBigQueue, SizeSmallQueue:
				curFile := man.files[filename]
				// fmt.Printf("--- file -> %#v\n", curFile)
				queueKey := curFile.QueueKey
				queueFileIdx := curFile.QueueIdx
				// fmt.Println("--- Coords ->", key, queueFileIdx)
				curQueue := man.queue[queueKey]
				// fmt.Println(curQueue)

				// Remove
				copy(curQueue[queueFileIdx:], curQueue[queueFileIdx+1:])
				curQueue = curQueue[:len(curQueue)-1]

				if len(curQueue) > 0 {
					man.queue[queueKey] = curQueue
					curVal, inList := queue2update[queueKey]
					if !inList {
						queue2update[queueKey] = queueFileIdx
					} else if curVal > queueFileIdx {
						queue2update[queueKey] = queueFileIdx
					}
				} else {
					delete(man.queue, queueKey)
					switch man.qType {
					case LRUQueue:
						recency := curFile.Recency
						idx := sort.Search(len(man.orderedValues), func(i int) bool {
							return man.orderedValues[i].(int64) >= recency
						})
						if idx < len(man.orderedValues) && man.orderedValues[idx] == recency {
							copy(man.orderedValues[idx:], man.orderedValues[idx+1:])
							man.orderedValues = man.orderedValues[:len(man.orderedValues)-1]
						} else {
							panic("ERROR: size to delete was not in ordered keys...")
						}
					case LFUQueue:
						frequency := curFile.Frequency
						idx := sort.Search(len(man.orderedValues), func(i int) bool {
							return man.orderedValues[i].(int64) >= frequency
						})
						if idx < len(man.orderedValues) && man.orderedValues[idx] == frequency {
							copy(man.orderedValues[idx:], man.orderedValues[idx+1:])
							man.orderedValues = man.orderedValues[:len(man.orderedValues)-1]
						} else {
							panic("ERROR: size to delete was not in ordered keys...")
						}
					case SizeSmallQueue:
						size := curFile.Size
						idx := sort.Search(len(man.orderedValues), func(i int) bool {
							return man.orderedValues[i].(float64) >= size
						})
						if idx < len(man.orderedValues) && man.orderedValues[idx] == size {
							copy(man.orderedValues[idx:], man.orderedValues[idx+1:])
							man.orderedValues = man.orderedValues[:len(man.orderedValues)-1]
						} else {
							panic("ERROR: size to delete was not in ordered keys...")
						}
					case SizeBigQueue:
						size := curFile.Size
						idx := sort.Search(len(man.orderedValues), func(i int) bool {
							return man.orderedValues[i].(float64) <= size
						})
						if idx < len(man.orderedValues) && man.orderedValues[idx] == size {
							copy(man.orderedValues[idx:], man.orderedValues[idx+1:])
							man.orderedValues = man.orderedValues[:len(man.orderedValues)-1]
						} else {
							panic("ERROR: size to delete was not in ordered keys...")
						}
					}
				}
			case NoQueue:
				idx := sort.Search(len(man.orderedValues), func(i int) bool {
					return man.orderedValues[i].(int64) >= filename
				})
				if idx < len(man.orderedValues) && man.orderedValues[idx] == filename {
					copy(man.orderedValues[idx:], man.orderedValues[idx+1:])
					man.orderedValues = man.orderedValues[:len(man.orderedValues)-1]
				} else {
					panic("ERROR: filename to delete was not in ordered keys...")
				}
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
		insertIdx = sort.Search(len(man.orderedValues), func(idx int) bool {
			return man.orderedValues[idx].(int64) > key.(int64)
		})
	case SizeBigQueue:
		insertIdx = sort.Search(len(man.orderedValues), func(idx int) bool {
			return man.orderedValues[idx].(float64) < key.(float64)
		})
	case SizeSmallQueue, WeightQueue:
		insertIdx = sort.Search(len(man.orderedValues), func(idx int) bool {
			return man.orderedValues[idx].(float64) > key.(float64)
		})
	}
	if insertIdx == len(man.orderedValues) {
		man.orderedValues = append(man.orderedValues, key)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		man.orderedValues = append(man.orderedValues, nil)
		copy(man.orderedValues[insertIdx+1:], man.orderedValues[insertIdx:])
		man.orderedValues[insertIdx] = key
	}
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

	switch man.qType {
	case LRUQueue, LFUQueue, SizeBigQueue, SizeSmallQueue:
		_, inQueue := man.queue[key]
		if !inQueue {
			man.insertKey(key)
			man.queue[key] = make([]int64, 0)
		}

		idx := man.insertInQueue(key, file.Filename)
		file.QueueIdx = idx
		file.QueueKey = key

	case NoQueue:
		man.insertKey(key)
	}

	man.files[file.Filename] = file
	// fmt.Println("[QUEUE] INSERT: ", file, file.Filename, key)
}

// Update a file into the queue manager
func (man *Manager) Update(file *FileSupportData) {
	// fmt.Println("UPDATE:", file.Filename)
	// fmt.Println("--- BEFORE ---")
	// fmt.Println(man.orderedValues)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	switch man.qType {
	case LRUQueue, LFUQueue, SizeBigQueue, SizeSmallQueue:
		man.Remove([]int64{file.Filename})
		man.Insert(file)
	case NoQueue:
		man.files[file.Filename] = file
	}

	// fmt.Println("--- AFTER ---")
	// fmt.Println(man.orderedValues)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// fmt.Println("--- DONE ---")
}
