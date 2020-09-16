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
	fileIndexes    map[int64]int
	prevVal        map[int64]interface{}
	queueFilenames []int64
	queueI         []int64
	queueF         []float64
	dirtyFlag      []bool
	qType          queueType
	buffer         []*FileStats
}

// Init initialize the struct
func (man *Manager) Init(qType queueType) {
	man.files = make(map[int64]*FileStats)
	man.fileIndexes = make(map[int64]int)
	man.prevVal = make(map[int64]interface{})
	man.queueFilenames = make([]int64, 0)
	man.queueI = make([]int64, 0)
	man.queueF = make([]float64, 0)
	man.buffer = make([]*FileStats, 0)
	man.qType = qType
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

// GetFile returns a specific file support data
func (man *Manager) GetFileStats(id int64) *FileStats {
	return man.files[id]
}

// GetQueue values from a queue
func (man *Manager) GetQueue() []*FileStats {
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
func (man *Manager) GetFromWorst() []*FileStats {
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
func (man *Manager) GetWorstFilesUp2Size(totSize float64) []*FileStats {
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

func (man *Manager) getFileIndex(filename int64) int { //nolint:ignore,funlen
	var resultIdx int

	guessIdx := man.fileIndexes[filename]
	if guessIdx >= len(man.queueFilenames) {
		guessIdx = len(man.queueFilenames) - 1
	}
	guessedFilename := man.queueFilenames[guessIdx]

	// fmt.Println("Guessed filename ->", guessedFilename, "Wanted name ->", filename)

	prevVal := man.prevVal[filename]

	switch {
	case guessedFilename != filename:
		direction := 1

		switch man.qType {
		case NoQueue:
			idxVal := man.queueFilenames[guessIdx]
			// fmt.Println("target:", prevVal, "guess val:", idxVal)
			if idxVal >= prevVal.(int64) {
				direction = -1
			}
		case LRUQueue, LFUQueue:
			idxVal := man.queueI[guessIdx]
			// fmt.Println("target:", prevVal, "guess val:", idxVal)
			if idxVal >= prevVal.(int64) {
				direction = -1
			}
		case SizeSmallQueue, WeightQueue:
			idxVal := man.queueF[guessIdx]
			// fmt.Println("target:", prevVal, "guess val:", idxVal)
			if idxVal >= prevVal.(float64) {
				direction = -1
			}
		case SizeBigQueue:
			idxVal := man.queueF[guessIdx]
			// fmt.Println("target:", prevVal, "guess val:", idxVal)
			if idxVal <= prevVal.(float64) {
				direction = -1
			}
		}

		stop := -1
		if direction > 0 {
			stop = len(man.queueFilenames)
		}

		found := false
		// fmt.Println("Start:", guessIdx, "Stop:", stop, "Direction:", direction)
		// fmt.Println(man.queueFilenames)
		// fmt.Println(man.queueI)
		// fmt.Println(man.queueF)

		for idx := guessIdx; idx != stop; idx += direction {
			curFilename := man.queueFilenames[idx]
			// fmt.Println("Finding:", filename, "on index", idx, "found ->", curFilename)
			if man.dirtyFlag[idx] {
				man.fileIndexes[filename] = idx
				man.dirtyFlag[idx] = false
			}

			if curFilename == filename {
				// fmt.Println("FOUND at index", idx)
				resultIdx = idx
				found = true

				break
			}
		}

		if !found { //nolint:ignore,nestif
			direction = -direction

			stop = -1
			if direction > 0 {
				stop = len(man.queueFilenames)
			}

			for idx := guessIdx; idx != stop; idx += direction {
				curFilename := man.queueFilenames[idx]
				// fmt.Println("Finding:", filename, "on index", idx, "found ->", curFilename)
				if man.dirtyFlag[idx] {
					man.fileIndexes[filename] = idx
					man.dirtyFlag[idx] = false
				}

				if curFilename == filename {
					// fmt.Println("FOUND at index", idx)
					resultIdx = idx
					found = true

					break

				}
			}
		}

		if !found {
			panic("ERROR: file to remove not found with guess...")
		}
	case guessedFilename == filename:
		resultIdx = guessIdx
	default:
		panic("ERROR: file to remove not found...")
	}

	return resultIdx
}

// collapseIndexes return the lowest index if they are all coninuos in the slice
func collapseIndexes(indexes []int) int {
	collapsedIdx := indexes[0]

	if len(indexes) > 1 {
		// fmt.Println("INDEXES", indexes)
		for i := 0; i < len(indexes)-1; i++ {
			if indexes[i]-indexes[i+1] == 1 {
				continue
			} else {
				collapsedIdx = -1

				break
			}
		}
	}

	// fmt.Println("COLLAPSEDINDEX", collapsedIdx)
	return collapsedIdx
}

func (man *Manager) removeIndexes(idx2Remove []int) {
	sort.Sort(sort.Reverse(sort.IntSlice(idx2Remove)))

	// if len(idx2Remove) > 1 {
	// 	fmt.Println(idx2Remove)
	// 	fmt.Println("BEFORE", man.queueFilenames, len(man.queueFilenames))
	// }

	collapsedIdx := collapseIndexes(idx2Remove)
	if collapsedIdx != -1 && idx2Remove[len(idx2Remove)-1] == 0 {
		// fmt.Println("FILENAMES", man.queueFilenames)
		// fmt.Println("QUEUEI", man.queueI)
		// fmt.Println("QUEUEF", man.queueF)
		switch man.qType {
		case LRUQueue, LFUQueue:
			// Remove
			man.queueI = man.queueI[collapsedIdx+1:]
		case SizeBigQueue, SizeSmallQueue, WeightQueue:
			// Remove
			man.queueF = man.queueF[collapsedIdx+1:]
		}

		man.queueFilenames = man.queueFilenames[collapsedIdx+1:]
		man.dirtyFlag = man.dirtyFlag[collapsedIdx+1:]
	} else {
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
			copy(man.dirtyFlag[curIdx:], man.dirtyFlag[curIdx+1:])
			man.dirtyFlag = man.dirtyFlag[:len(man.dirtyFlag)-1]
		}
	}
}

// Remove a file already in queue
func (man *Manager) Remove(files []int64) { //nolint:ignore,funlen
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

	if len(files) > 0 { //nolint:ignore,nestif
		// _, file, no, _ := runtime.Caller(1)
		// fmt.Printf("called from %s#%d\n", file, no)

		idx2Remove := make([]int, 0)
		for _, filename := range files {
			// fmt.Println("--- Removing ->", filename)
			idx2Remove = append(idx2Remove, man.getFileIndex(filename))

			delete(man.files, filename)
			delete(man.fileIndexes, filename)
		}

		// fmt.Println("IDX 2 REMOVE:", idx2Remove)

		if len(idx2Remove) > 0 {
			man.removeIndexes(idx2Remove)
		}
		// fmt.Println("AFTER", man.queueFilenames)

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

func (man *Manager) getInsertIndex(feature interface{}, file *FileStats) int { //nolint: ignore,funlen
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

	return insertIdx
}

func (man *Manager) insertFeature(insertIdx int, feature interface{}) { //nolint: ignore,funlen
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
}

func (man *Manager) setDirtyFrom(index int) {
	for idx := index; idx < len(man.dirtyFlag); idx++ {
		man.dirtyFlag[idx] = true
	}
}

func (man *Manager) insertFilename(insertIdx int, filename int64) {
	if insertIdx == len(man.queueFilenames) {
		man.queueFilenames = append(man.queueFilenames, filename)
		man.dirtyFlag = append(man.dirtyFlag, true)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		man.queueFilenames = append(man.queueFilenames, -1)
		copy(man.queueFilenames[insertIdx+1:], man.queueFilenames[insertIdx:])
		man.queueFilenames[insertIdx] = filename
		man.setDirtyFrom(insertIdx + 1)
	}
}

// Insert a file into the queue manager
func (man *Manager) Insert(file *FileStats) { //nolint:ignore,funlen
	// fmt.Println(file.Filename, "->", file.Recency)
	// Force inserto check

	filename := file.Filename
	_, inCache := man.files[filename]
	if inCache {
		panic("ERROR: File already in manager...")
	}

	feature := man.getFeature(file)
	man.prevVal[filename] = feature

	insertIdx := man.getInsertIndex(feature, file)

	man.insertFeature(insertIdx, feature)
	man.insertFilename(insertIdx, filename)

	man.fileIndexes[file.Filename] = insertIdx
	man.files[file.Filename] = file

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
	if file != man.files[file.Filename] {
		panic("ERROR: update on different stat")
	}
	if man.qType != NoQueue {
		filename := file.Filename
		man.removeIndexes([]int{man.getFileIndex(filename)})

		feature := man.getFeature(file)
		man.prevVal[filename] = feature
		insertIdx := man.getInsertIndex(feature, file)
		man.insertFeature(insertIdx, feature)
		man.insertFilename(insertIdx, filename)

		man.fileIndexes[file.Filename] = insertIdx
	}
	// fmt.Println("--- AFTER ---")
	// fmt.Println(man.orderedValues)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// fmt.Println("--- DONE ---")
}
