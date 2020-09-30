package cache

import (
	"fmt"
	"log"
	"sort"
)

type QueueSizeSmall struct {
	files         map[int64]*FileStats
	lastVal       map[int64]float64
	lastIndex     map[int64]int
	orderedValues []float64
	queue         map[float64][]int64
	buffer        []*FileStats
}

// init initialize the struct
func (q *QueueSizeSmall) init() {
	q.files = make(map[int64]*FileStats, estimatedNumFiles)
	q.lastVal = make(map[int64]float64, estimatedNumFiles)
	q.lastIndex = make(map[int64]int, estimatedNumFiles)
	q.orderedValues = make([]float64, 0)
	q.queue = make(map[float64][]int64, estimatedNumFiles)
	q.buffer = make([]*FileStats, 0, bufferSize)
}

// getFileStats from a file in queue
func (q *QueueSizeSmall) getFileStats(filename int64) *FileStats {
	stats, inQueue := q.files[filename]

	if !inQueue {
		log.Fatal(fmt.Errorf("size small getFileStats: file %d already in queue", filename))
	}

	return stats
}

// getQueue values from a queue
func (q *QueueSizeSmall) getQueue() []*FileStats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idxOVal := len(q.orderedValues) - 1; idxOVal > -1; idxOVal-- {
		curQueue := q.queue[q.orderedValues[idxOVal]]
		for idx := len(curQueue); idx > -1; idx-- {
			filename := curQueue[idx]
			fileStats, inQueue := q.files[filename]

			if inQueue {
				q.buffer = append(q.buffer, fileStats)
			} else {
				log.Fatal(fmt.Errorf("size small getQueue: file %d not in queue", filename))
			}
		}
	}

	return q.buffer
}

// getFromWorst values from worst queue values
func (q *QueueSizeSmall) getFromWorst() []*FileStats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idxOVal := 0; idxOVal < len(q.orderedValues); idxOVal++ {
		curQueue := q.queue[q.orderedValues[idxOVal]]
		for idx := 0; idx < len(curQueue); idx++ {
			filename := curQueue[idx]
			fileStats, inQueue := q.files[filename]

			if inQueue {
				q.buffer = append(q.buffer, fileStats)
			} else {
				log.Fatal(fmt.Errorf("size small getQueue: file %d not in queue", filename))
			}
		}
	}

	return q.buffer
}

// getWorstFilesUp2Size values from a queue until size is reached
func (q *QueueSizeSmall) getWorstFilesUp2Size(totSize float64) []*FileStats {
	if totSize <= 0. {
		panic("ERROR: tot size is negative or equal to 0")
	}

	var sended float64

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idxOVal := 0; idxOVal < len(q.orderedValues); idxOVal++ {
		curQueue := q.queue[q.orderedValues[idxOVal]]
		for idx := 0; idx < len(curQueue); idx++ {
			filename := curQueue[idx]
			fileStats, inQueue := q.files[filename]

			if inQueue {
				q.buffer = append(q.buffer, fileStats)
				sended += fileStats.Size
				if sended >= totSize {
					break
				}
			} else {
				log.Fatal(fmt.Errorf("size small getQueue: file %d not in queue", filename))
			}
		}
		if sended >= totSize {
			break
		}
	}

	// fmt.Println(totSize, sended, len(q.buffer))

	return q.buffer
}

// check if a file is in cache
func (q *QueueSizeSmall) check(file int64) bool {
	_, inQueue := q.files[file]

	return inQueue
}

// checkOVal if an ordered value is in cache
func (q *QueueSizeSmall) checkOVal(oVal float64) bool {
	_, present := q.queue[oVal]

	return present
}

// len returns the number of files in cache
func (q *QueueSizeSmall) len() int {
	return len(q.files)
}

func (q *QueueSizeSmall) insertOrderedValue(oVal float64) {
	insertIdx := sort.Search(len(q.orderedValues), func(idx int) bool {
		return q.orderedValues[idx] > oVal
	})

	if insertIdx == len(q.orderedValues) {
		q.orderedValues = append(q.orderedValues, oVal)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		q.orderedValues = append(q.orderedValues, -1)
		copy(q.orderedValues[insertIdx+1:], q.orderedValues[insertIdx:])
		q.orderedValues[insertIdx] = oVal
	}
}

// insert a file into the SizeSmall queue
func (q *QueueSizeSmall) insert(file *FileStats) (err error) {
	filename := file.Filename
	oVal := file.Size

	if q.check(filename) {
		return fmt.Errorf("size small insert: file %d already in queue", filename)
	}

	if !q.checkOVal(oVal) {
		q.queue[oVal] = make([]int64, 0)
		q.insertOrderedValue(oVal)
	}

	curQueue := q.queue[oVal]
	curIdx := len(curQueue)

	// fmt.Printf("INSERT -> %d in %d\n", file.Filename, curIdx)

	curQueue = append(curQueue, filename)

	q.queue[oVal] = curQueue
	q.lastVal[filename] = oVal
	q.lastIndex[filename] = curIdx
	q.files[filename] = file

	return nil
}

func (q *QueueSizeSmall) findIndex(filename int64, curQueue []int64, lastIdx int) int {
	newIdx := -1

	start := lastIdx

	if start >= len(curQueue) {
		start = len(curQueue) - 1
	}

	for idx := start; idx > -1; idx-- {
		curFilename := curQueue[idx]
		if curFilename == filename {
			newIdx = idx

			break
		} else {
			q.lastIndex[curFilename] = idx
		}
	}

	return newIdx
}

// removeWorst a file from the SizeSmall queue from worsts (head)
func (q *QueueSizeSmall) removeWorst(files []int64) (err error) {
	ordValues2remove := make([]float64, 0)

	for idxOVal := 0; idxOVal < len(q.orderedValues); idxOVal++ {
		curOVal := q.orderedValues[idxOVal]
		curQueue := q.queue[curOVal]

		// fmt.Println("FILES", files)
		// fmt.Println("QUEUE", curQueue)

		for idx := 0; idx < len(curQueue) && idx < len(files); idx++ {
			queueFilename := curQueue[idx]
			filename2remove := files[idx]

			if filename2remove != queueFilename {
				return fmt.Errorf("size small remove worst: file %d != %d", filename2remove, queueFilename)
			}

			delete(q.lastIndex, filename2remove)
			delete(q.lastVal, filename2remove)
			delete(q.files, filename2remove)
		}

		if len(files) >= len(curQueue) {
			delete(q.queue, curOVal)
			copy(files, files[len(curQueue):])
			files = files[:len(files)-len(curQueue)]

			ordValues2remove = append(ordValues2remove, curOVal)

			if len(files) == 0 {
				break
			}
		} else {
			copy(curQueue, curQueue[len(files):])
			curQueue = curQueue[:len(curQueue)-len(files)]
			q.queue[curOVal] = curQueue

			break
		}
	}

	for _, curOVal := range ordValues2remove {
		if err := q.removeOrderedValue(curOVal); err != nil {
			return err
		}
	}

	// fmt.Println("QUEUE", q.queue)

	return nil
}

func (q *QueueSizeSmall) removeOrderedValue(value2remove float64) error {
	ordValIdx := sort.Search(len(q.orderedValues), func(idx int) bool {
		return q.orderedValues[idx] >= value2remove
	})
	if ordValIdx < len(q.orderedValues) && q.orderedValues[ordValIdx] == value2remove {
		copy(q.orderedValues[ordValIdx:], q.orderedValues[ordValIdx+1:])
		q.orderedValues = q.orderedValues[:len(q.orderedValues)-1]
	} else {
		return fmt.Errorf("size small remove: freq %f not present", value2remove)
	}

	return nil
}

// remove a file from the SizeSmall queue
func (q *QueueSizeSmall) remove(files []int64) (err error) {

	for _, name := range files {
		filename := name

		idx2remove, inIndexes := q.lastIndex[filename]
		ordVal2remove, inOrdVal := q.lastVal[filename]

		// fmt.Printf("REMOVE -> %d from %d\n", filename, idx2remove)

		if !inIndexes {
			return fmt.Errorf("size small remove: file %d has no index", filename)
		} else if !inOrdVal {
			return fmt.Errorf("size small remove: file %d has no freq", filename)
		}

		curQueue := q.queue[ordVal2remove]

		if idx2remove >= len(curQueue) || curQueue[idx2remove] != filename {
			idx2remove = q.findIndex(filename, curQueue, idx2remove)
		}

		copy(curQueue[idx2remove:], curQueue[idx2remove+1:])
		curQueue = curQueue[:len(curQueue)-1]

		if len(curQueue) > 0 {
			q.queue[ordVal2remove] = curQueue
		} else {
			delete(q.queue, ordVal2remove)
			if err := q.removeOrderedValue(ordVal2remove); err != nil {
				return err
			}
		}

		delete(q.lastIndex, filename)
		delete(q.lastVal, filename)
		delete(q.files, filename)
	}

	return nil
}

// update a file of the SizeSmall queue
func (q *QueueSizeSmall) update(file *FileStats) (err error) {
	// fmt.Printf("UPDATE -> %d\n", file.Filename)
	filename := file.Filename

	stats, inMap := q.files[filename]

	switch {
	case !inMap:
		return fmt.Errorf("size small update: file %d not stored in queue", filename)
	case file != stats:
		// fmt.Println(file, man.files[file.Filename])
		// fmt.Println(file.Filename, man.files[file.Filename].Filename)
		return fmt.Errorf("size small update: different stats -> %v != %v", file, stats)
	}

	q.remove([]int64{filename})
	q.insert(file)

	return nil
}
