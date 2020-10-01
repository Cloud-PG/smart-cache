package queue

import (
	"fmt"
	"sort"

	"simulator/v2/cache/files"
)

// Other policy utils

type QueueType int

const (
	// LRUQueue is the LRU queue type
	LRUQueue QueueType = iota - 6
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

const (
	estimatedNumFiles = 1 << 19
	bufferSize        = estimatedNumFiles >> 2
)

type Queue interface {
	init()
	check(file int64) bool
	len() int

	getFileStats(filename int64) *files.Stats
	getQueue() []*files.Stats
	getFromWorst() []*files.Stats
	getWorstFilesUp2Size(totSize float64) []*files.Stats

	insert(file *files.Stats) error
	update(file *files.Stats) error
	remove(files []int64) error
	removeWorst(files []int64) error
}

func Init(queue Queue) {
	queue.init()
}

func Check(queue Queue, file int64) bool {
	return queue.check(file)
}

func Len(queue Queue) int {
	return queue.len()
}

func GetFileStats(queue Queue, filename int64) *files.Stats {
	return queue.getFileStats(filename)
}

func Insert(queue Queue, file *files.Stats) {
	err := queue.insert(file)
	if err != nil {
		panic(err)
	}
}

func Update(queue Queue, file *files.Stats) {
	err := queue.update(file)
	if err != nil {
		panic(err)
	}
}

func Remove(queue Queue, files []int64) {
	err := queue.remove(files)
	if err != nil {
		panic(err)
	}
}

func RemoveWorst(queue Queue, files []int64) {
	err := queue.removeWorst(files)
	if err != nil {
		panic(err)
	}
}

func Get(queue Queue) []*files.Stats {
	return queue.getQueue()
}

func GetFromWorst(queue Queue) []*files.Stats {
	return queue.getFromWorst()
}

func GetWorstFilesUp2Size(queue Queue, totSize float64) []*files.Stats {
	return queue.getWorstFilesUp2Size(totSize)
}

// Manager manages the files in cache
type Manager struct {
	files          map[int64]*files.Stats
	fileIndexes    map[int64]int
	prevVal        map[int64]interface{}
	queueFilenames []int64
	queueI         []int64
	queueF         []float64
	qType          QueueType
	buffer         []*files.Stats
}

// Init initialize the struct
func (man *Manager) Init(qType QueueType) {
	man.files = make(map[int64]*files.Stats, estimatedNumFiles)
	man.fileIndexes = make(map[int64]int, estimatedNumFiles)
	man.prevVal = make(map[int64]interface{}, estimatedNumFiles)
	man.queueFilenames = make([]int64, 0, estimatedNumFiles)
	man.queueI = make([]int64, 0, estimatedNumFiles)
	man.queueF = make([]float64, 0, estimatedNumFiles)
	man.buffer = make([]*files.Stats, 0, estimatedNumFiles)
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

// Get values from a queue
func (man *Manager) Get() []*files.Stats {
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
func (man *Manager) GetFromWorst() []*files.Stats {
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
func (man *Manager) GetWorstFilesUp2Size(totSize float64) []*files.Stats {
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

	switch {
	case guessIdx >= len(man.queueFilenames):
		guessIdx = len(man.queueFilenames) >> 1
	case guessIdx < 0:
		panic("ERROR: negative guess index")
	}

	guessedFilename := man.queueFilenames[guessIdx]

	// fmt.Println("Guessed filename ->", guessedFilename, "Wanted name ->", filename)
	// fmt.Println(man.queueFilenames)
	// fmt.Println(man.queueF)
	// fmt.Println(man.queueI)

	switch {
	case guessedFilename != filename:
		prevVal, inPrevVal := man.prevVal[filename]

		// fmt.Println("PREVVAL:", prevVal)

		if !inPrevVal {
			panic(fmt.Sprintf("ERROR: file %d has no previous value", filename))
		}

		leftIdx := man.getGuessNewIndex(prevVal)
		rightIdx := man.getInsertIndex(prevVal)

		// fmt.Println("GUESSIDX:", guessIdx)

		if rightIdx == len(man.queueFilenames) {
			rightIdx--
		}

		found := false

		for leftIdx <= rightIdx {
			leftFilename := man.queueFilenames[leftIdx]
			// fmt.Println("Finding:", filename, "on index", idx, "idx found ->", leftFilename)
			man.fileIndexes[filename] = leftIdx

			if leftFilename == filename {
				// fmt.Println("FOUND at index", idx)
				resultIdx = leftIdx
				found = true

				break
			}

			rightFilename := man.queueFilenames[rightIdx]
			// fmt.Println("Finding:", filename, "on index", idx, "idx found ->", rightFilename)
			man.fileIndexes[filename] = rightIdx

			if rightFilename == filename {
				// fmt.Println("FOUND at index", idx)
				resultIdx = rightIdx
				found = true

				break
			}

			leftIdx++
			rightIdx--
		}

		if !found {
			panic("ERROR: file index not found")
		}
	case guessedFilename == filename:
		resultIdx = guessIdx
	default:
		panic("ERROR: file to remove not found")
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

func (man *Manager) getFeature(file *files.Stats) interface{} {
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

	if feature == nil {
		panic(fmt.Sprintf("ERROR: cannot extract feature from file %#v", file))
	}

	return feature
}

func (man *Manager) getGuessNewIndex(feature interface{}) int { //nolint: ignore,funlen
	var insertIdx = -1

	switch man.qType {
	case NoQueue:
		intFeature := feature.(int64)
		insertIdx = sort.Search(len(man.queueFilenames), func(idx int) bool {
			return man.queueFilenames[idx] >= intFeature
		})
	case LRUQueue, LFUQueue:
		intFeature := feature.(int64)
		insertIdx = sort.Search(len(man.queueI), func(idx int) bool {
			return man.queueI[idx] >= intFeature
		})
	case SizeBigQueue:
		floatFeature := feature.(float64)
		insertIdx = sort.Search(len(man.queueF), func(idx int) bool {
			return man.queueF[idx] <= floatFeature
		})
	case SizeSmallQueue, WeightQueue:
		floatFeature := feature.(float64)
		insertIdx = sort.Search(len(man.queueF), func(idx int) bool {
			return man.queueF[idx] >= floatFeature
		})
	}

	return insertIdx
}

func (man *Manager) getInsertIndex(feature interface{}) int { //nolint: ignore,funlen
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

func (man *Manager) insertFilename(insertIdx int, filename int64) {
	if insertIdx == len(man.queueFilenames) {
		man.queueFilenames = append(man.queueFilenames, filename)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		man.queueFilenames = append(man.queueFilenames, -1)
		copy(man.queueFilenames[insertIdx+1:], man.queueFilenames[insertIdx:])
		man.queueFilenames[insertIdx] = filename
	}
}

// Insert a file into the queue manager
func (man *Manager) Insert(file *files.Stats) { //nolint:ignore,funlen
	// fmt.Println(file.Filename, "->", file.Recency)
	// Force inserto check

	filename := file.Filename
	if man.Check(filename) {
		panic("ERROR: File already in manager")
	}

	feature := man.getFeature(file)

	insertIdx := man.getInsertIndex(feature)

	man.insertFeature(insertIdx, feature)
	man.insertFilename(insertIdx, filename)

	man.files[file.Filename] = file
	man.fileIndexes[file.Filename] = insertIdx
	man.prevVal[filename] = feature

	// fmt.Println("[QUEUE] INSERT: ", file, file.Filename, key)
}

// Update a file into the queue manager
func (man *Manager) Update(file *files.Stats) {
	// fmt.Println("UPDATE:", file.Filename, "->", file.Recency)
	// fmt.Println("--- BEFORE ---")
	// fmt.Println(man.orderedValues)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// curFileStats := man.files[file.Filename]
	// if curFileStats != file {
	// 	panic("Different file stats")
	// }
	oldStats, inMap := man.files[file.Filename]

	switch {
	case !inMap:
		panic("ERROR: file not stored")
	case file != oldStats:
		// fmt.Println(file, man.files[file.Filename])
		// fmt.Println(file.Filename, man.files[file.Filename].Filename)
		panic("ERROR: update on different stat")
	}

	if man.qType != NoQueue {
		filename := file.Filename
		man.removeIndexes([]int{man.getFileIndex(filename)})

		feature := man.getFeature(file)
		insertIdx := man.getInsertIndex(feature)
		man.insertFeature(insertIdx, feature)
		man.insertFilename(insertIdx, filename)

		man.fileIndexes[filename] = insertIdx
		man.prevVal[filename] = feature
	}
	// fmt.Println("--- AFTER ---")
	// fmt.Println(man.orderedValues)
	// for key, queue := range man.queue {
	// 	fmt.Println("-[", key, "]", queue)
	// }
	// fmt.Println("--- DONE ---")
}
