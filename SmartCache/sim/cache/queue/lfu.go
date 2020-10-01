package queue

import (
	"fmt"
	"log"
	"simulator/v2/cache/files"
	"sort"
)

type LFU struct {
	files         map[int64]*files.Stats
	lastVal       map[int64]int64
	lastIndex     map[int64]int
	orderedValues []int64
	queue         map[int64][]int64
	buffer        []*files.Stats
}

// init initialize the struct
func (q *LFU) init() {
	q.files = make(map[int64]*files.Stats, estimatedNumFiles)
	q.lastVal = make(map[int64]int64, estimatedNumFiles)
	q.lastIndex = make(map[int64]int, estimatedNumFiles)
	q.orderedValues = make([]int64, 0)
	q.queue = make(map[int64][]int64, estimatedNumFiles)
	q.buffer = make([]*files.Stats, 0, bufferSize)
}

// getfiles.Stats from a file in queue
func (q *LFU) getFileStats(filename int64) *files.Stats {
	stats, inQueue := q.files[filename]

	if !inQueue {
		log.Fatal(fmt.Errorf("lfu getfiles.Stats: file %d already in queue", filename))
	}

	return stats
}

// getQueue values from a queue
func (q *LFU) getQueue() []*files.Stats {
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
				log.Fatal(fmt.Errorf("lfu getQueue: file %d not in queue", filename))
			}
		}
	}

	return q.buffer
}

// getFromWorst values from worst queue values
func (q *LFU) getFromWorst() []*files.Stats {
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
				log.Fatal(fmt.Errorf("lfu getQueue: file %d not in queue", filename))
			}
		}
	}

	return q.buffer
}

// getWorstFilesUp2Size values from a queue until size is reached
func (q *LFU) getWorstFilesUp2Size(totSize float64) []*files.Stats {
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
				log.Fatal(fmt.Errorf("lfu getQueue: file %d not in queue", filename))
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
func (q *LFU) check(file int64) bool {
	_, inQueue := q.files[file]

	return inQueue
}

// checkOVal if an ordered value is in cache
func (q *LFU) checkOVal(oVal int64) bool {
	_, present := q.queue[oVal]

	return present
}

// len returns the number of files in cache
func (q *LFU) len() int {
	return len(q.files)
}

func (q *LFU) insertOrderedValue(oVal int64) {
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

// insert a file into the LFU queue
func (q *LFU) insert(file *files.Stats) (err error) {
	filename := file.Filename
	oVal := file.Frequency

	if q.check(filename) {
		return fmt.Errorf("lfu insert: file %d already in queue", filename)
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

func (q *LFU) findIndex(filename int64, curQueue []int64, lastIdx int) int {
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

// removeWorst a file from the LFU queue from worsts (head)
func (q *LFU) removeWorst(files []int64) (err error) {
	ordValues2remove := make([]int64, 0)

	for idxOVal := 0; idxOVal < len(q.orderedValues); idxOVal++ {
		curOVal := q.orderedValues[idxOVal]
		curQueue := q.queue[curOVal]

		// fmt.Println("FILES", files)
		// fmt.Println("QUEUE", curQueue)

		for idx := 0; idx < len(curQueue) && idx < len(files); idx++ {
			queueFilename := curQueue[idx]
			filename2remove := files[idx]

			if filename2remove != queueFilename {
				return fmt.Errorf("lfu remove worst: file %d != %d", filename2remove, queueFilename)
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

func (q *LFU) removeOrderedValue(value2remove int64) error {
	ordValIdx := sort.Search(len(q.orderedValues), func(idx int) bool {
		return q.orderedValues[idx] >= value2remove
	})
	if ordValIdx < len(q.orderedValues) && q.orderedValues[ordValIdx] == value2remove {
		copy(q.orderedValues[ordValIdx:], q.orderedValues[ordValIdx+1:])
		q.orderedValues = q.orderedValues[:len(q.orderedValues)-1]
	} else {
		return fmt.Errorf("lfu remove: freq %d not present", value2remove)
	}

	return nil
}

// remove a file from the LFU queue
func (q *LFU) remove(files []int64) (err error) {

	for _, name := range files {
		filename := name

		idx2remove, inIndexes := q.lastIndex[filename]
		ordVal2remove, inOrdVal := q.lastVal[filename]

		// fmt.Printf("REMOVE -> %d from %d\n", filename, idx2remove)

		if !inIndexes {
			return fmt.Errorf("lfu remove: file %d has no index", filename)
		} else if !inOrdVal {
			return fmt.Errorf("lfu remove: file %d has no freq", filename)
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

// update a file of the LFU queue
func (q *LFU) update(file *files.Stats) (err error) {
	// fmt.Printf("UPDATE -> %d\n", file.Filename)
	filename := file.Filename

	stats, inMap := q.files[filename]

	switch {
	case !inMap:
		return fmt.Errorf("lfu update: file %d not stored in queue", filename)
	case file != stats:
		// fmt.Println(file, man.files[file.Filename])
		// fmt.Println(file.Filename, man.files[file.Filename].Filename)
		return fmt.Errorf("lfu update: different stats -> %v != %v", file, stats)
	}

	err = q.remove([]int64{filename})
	if err != nil {
		return err
	}
	err = q.insert(file)
	if err != nil {
		return err
	}

	return nil
}
