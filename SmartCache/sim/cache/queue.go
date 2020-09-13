package cache

import (
	"sort"
)

// Other policy utils

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
	files map[int64]*FileStats
	// Local scope to force the stack memory (hopefully)
	queueFilenames []int64
	queueI         []int64
	queueF         []float64
	qType          queueType
	buffer         []*FileStats
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileStats)
	man.queueFilenames = make([]int64, 0)
	man.queueI = make([]int64, 0)
	man.queueF = make([]float64, 0)
	man.buffer = make([]*FileStats, 0)
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
func (man Manager) GetFileStats(id int64) *FileStats {
	return man.files[id]
}

// GetQueue values from a queue
func (man Manager) GetQueue() []*FileStats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	for queueIdx := len(man.queueFilenames) - 1; queueIdx > -1; queueIdx-- {
		filename := man.queueFilenames[queueIdx]
		man.buffer = append(man.buffer, man.files[filename])
	}

	return man.buffer
}

// GetFromWorst values from worst queue values
func (man Manager) GetFromWorst() []*FileStats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	for _, filename := range man.queueFilenames {
		man.buffer = append(man.buffer, man.files[filename])
	}

	// for _, file := range man.buffer {	// 	fmt.Printf("%d | ", file.Filename)
	// }
	// fmt.Println()
	return man.buffer
}

// GetWorstFilesUp2Size values from a queue until size is reached
func (man Manager) GetWorstFilesUp2Size(totSize float64) []*FileStats {
	if totSize <= 0. {
		panic("ERROR: tot size is negative or equal to 0")
	}
	var sended float64

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	man.buffer = man.buffer[:0]

	for _, filename := range man.queueFilenames {
		curFile := man.files[filename]
		man.buffer = append(man.buffer, curFile)

		sended += curFile.Size
		if sended >= totSize {
			break
		}
	}

	// fmt.Println(totSize, sended, len(man.buffer))

	return man.buffer
}

func (man *Manager) updateIndexes(startFrom int) {
	// fmt.Println("UPDATE INDEXES", "len:", len(man.queueFilenames), "start:", startFrom)

	for idx := startFrom; idx < len(man.queueFilenames); idx++ {
		filename := man.queueFilenames[idx]

		// fmt.Println("FILENAME:", filename)

		curFile, inFiles := man.files[filename]
		if !inFiles {
			panic("ERROR: cannot update deleted file...")
		}
		curFile.QueueIdx = idx
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

		idx2Remove := make([]int, 0)
		for _, filename := range files {
			// fmt.Println("--- Removing ->", filename)

			idx2Remove = append(idx2Remove, man.files[filename].QueueIdx)
			delete(man.files, filename)
		}

		sort.Sort(sort.Reverse(sort.IntSlice(idx2Remove)))

		// fmt.Println(idx2Remove)
		// fmt.Println("BEFORE", man.queueFilenames)

		for _, curIdx := range idx2Remove {
			switch man.qType {
			case LRUQueue, LFUQueue:
				// Remove
				copy(man.queueI[curIdx:], man.queueI[curIdx+1:])
				man.queueI = man.queueI[:len(man.queueI)-1]
			case SizeBigQueue, SizeSmallQueue, WeightQueue:
				// Remove
				copy(man.queueF[curIdx:], man.queueF[curIdx+1:])
				man.queueF = man.queueF[:len(man.queueF)-1]
			}
			copy(man.queueFilenames[curIdx:], man.queueFilenames[curIdx+1:])
			man.queueFilenames = man.queueFilenames[:len(man.queueFilenames)-1]
		}

		// fmt.Println("AFTER", man.queueFilenames)

		man.updateIndexes(idx2Remove[len(idx2Remove)-1])

		// fmt.Println(man.Len(), len(man.files), len(man.queue), len(man.orderedValues))
	}
}

func (man *Manager) getFeature(file *FileStats) interface{} {
	var feature interface{}
	switch man.qType {
	case NoQueue:
		feature = file.Filename
	case LRUQueue:
		feature = file.Recency
	case LFUQueue:
		feature = file.Frequency
	case SizeBigQueue:
		feature = file.Size
	case SizeSmallQueue:
		feature = file.Size
	case WeightQueue:
		feature = file.Weight
	}
	return feature
}

// Insert a file into the queue manager
func (man *Manager) Insert(file *FileStats) {
	// fmt.Println(file.Filename, "->", file.Recency)
	// Force inserto check
	filename := file.Filename
	_, inCache := man.files[filename]
	if inCache {
		panic("ERROR: File already in manager...")
	}

	feature := man.getFeature(file)

	var insertIdx = -1
	switch man.qType {
	case NoQueue:
		intFeature := feature.(int64)
		insertIdx = sort.Search(len(man.queueFilenames), func(idx int) bool {
			return man.queueFilenames[idx] > intFeature
		})
	case LRUQueue, LFUQueue:
		intFeature := feature.(int64)
		insertIdx = sort.Search(len(man.queueI), func(idx int) bool {
			return man.queueI[idx] > intFeature
		})
	case SizeBigQueue:
		floatFeature := feature.(float64)
		insertIdx = sort.Search(len(man.queueF), func(idx int) bool {
			return man.queueF[idx] < floatFeature
		})
	case SizeSmallQueue, WeightQueue:
		floatFeature := feature.(float64)
		insertIdx = sort.Search(len(man.queueF), func(idx int) bool {
			return man.queueF[idx] > floatFeature
		})
	}

	switch man.qType {
	case LRUQueue, LFUQueue:
		intFeature := feature.(int64)
		if insertIdx == len(man.queueI) {
			man.queueI = append(man.queueI, intFeature)
		} else {
			// Trick
			// https://github.com/golang/go/wiki/SliceTricks#insert
			man.queueI = append(man.queueI, -1)
			copy(man.queueI[insertIdx+1:], man.queueI[insertIdx:])
			man.queueI[insertIdx] = intFeature
		}
	case SizeBigQueue, SizeSmallQueue, WeightQueue:
		floatFeature := feature.(float64)
		if insertIdx == len(man.queueF) {
			man.queueF = append(man.queueF, floatFeature)
		} else {
			// Trick
			// https://github.com/golang/go/wiki/SliceTricks#insert
			man.queueF = append(man.queueF, -1.)
			copy(man.queueF[insertIdx+1:], man.queueF[insertIdx:])
			man.queueF[insertIdx] = floatFeature
		}
	}

	if insertIdx == len(man.queueFilenames) {
		man.queueFilenames = append(man.queueFilenames, filename)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		man.queueFilenames = append(man.queueFilenames, -1)
		copy(man.queueFilenames[insertIdx+1:], man.queueFilenames[insertIdx:])
		man.queueFilenames[insertIdx] = filename
	}

	file.QueueIdx = insertIdx
	man.files[file.Filename] = file

	if insertIdx != len(man.queueFilenames) {
		man.updateIndexes(insertIdx)
	}
	// fmt.Println("[QUEUE] INSERT: ", file, file.Filename, key)
}

// Update a file into the queue manager
func (man *Manager) Update(file *FileStats) {
	// fmt.Println("UPDATE:", file.Filename, "->", file.Recency)
	// fmt.Println("--- BEFORE ---")
	// fmt.Println(man.orderedValues)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// curFileStats := man.files[file.Filename]
	// if curFileStats != file {
	// 	panic("Different file stats...")
	// }
	man.Remove([]int64{file.Filename})
	man.Insert(file)
	// fmt.Println("--- AFTER ---")
	// fmt.Println(man.orderedValues)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// fmt.Println("--- DONE ---")
}
