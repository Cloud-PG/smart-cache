package queue

import (
	"fmt"
	"log"
	"simulator/v2/cache/files"
)

type LRU struct {
	files     map[int64]*files.Stats
	lastIndex map[int64]int
	queue     []int64
	buffer    []*files.Stats
}

// init initialize the struct
func (q *LRU) init() {
	q.files = make(map[int64]*files.Stats, estimatedNumFiles)
	q.lastIndex = make(map[int64]int, estimatedNumFiles)
	q.queue = make([]int64, 0, estimatedNumFiles)
	q.buffer = make([]*files.Stats, 0, bufferSize)
}

// getFileStats from a file in queue
func (q *LRU) getFileStats(filename int64) *files.Stats {
	stats, inQueue := q.files[filename]

	if !inQueue {
		log.Fatal(fmt.Errorf("lru getFileStats: file %d already in queue", filename))
	}

	return stats
}

// getQueue values from a queue
func (q *LRU) getQueue() []*files.Stats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idx := len(q.queue); idx > -1; idx-- {
		filename := q.queue[idx]
		fileStats, inQueue := q.files[filename]

		if inQueue {
			q.buffer = append(q.buffer, fileStats)
		} else {
			log.Fatal(fmt.Errorf("lru getQueue: file %d not in queue", filename))
		}
	}

	return q.buffer
}

// getFromWorst values from worst queue values
func (q *LRU) getFromWorst() []*files.Stats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idx := 0; idx < len(q.queue); idx++ {
		filename := q.queue[idx]
		fileStats, inQueue := q.files[filename]

		if inQueue {
			q.buffer = append(q.buffer, fileStats)
		} else {
			log.Fatal(fmt.Errorf("lru getQueue: file %d not in queue", filename))
		}
	}

	return q.buffer
}

// getWorstFilesUp2Size values from a queue until size is reached
func (q *LRU) getWorstFilesUp2Size(totSize float64) []*files.Stats {
	if totSize <= 0. {
		panic("ERROR: tot size is negative or equal to 0")
	}

	var sended float64

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idx := 0; idx < len(q.queue); idx++ {
		filename := q.queue[idx]
		fileStats, inQueue := q.files[filename]

		if inQueue {
			q.buffer = append(q.buffer, fileStats)
			sended += fileStats.Size
			if sended >= totSize {
				break
			}
		} else {
			log.Fatal(fmt.Errorf("lru getQueue: file %d not in queue", filename))
		}
	}

	// fmt.Println(totSize, sended, len(q.buffer))

	return q.buffer
}

// check if a file is in cache
func (q *LRU) check(file int64) bool {
	_, inQueue := q.files[file]

	return inQueue
}

// len returns the number of files in cache
func (q *LRU) len() int {
	if len(q.queue) != len(q.files) {
		panic("lru len: queue len differ from files")
	}
	return len(q.files)
}

// insert a file into the LRU queue
func (q *LRU) insert(file *files.Stats) (err error) {
	filename := file.Filename

	if q.check(filename) {
		return fmt.Errorf("lru insert: file %d already in queue", filename)
	}

	curIdx := len(q.queue)

	// fmt.Printf("INSERT -> %d in %d\n", file.Filename, curIdx)

	q.queue = append(q.queue, filename)
	q.lastIndex[filename] = curIdx
	q.files[filename] = file

	return nil
}

func (q *LRU) findIndex(filename int64, lastIdx int) int {
	newIdx := -1

	start := lastIdx

	if start >= len(q.queue) {
		start = len(q.queue) - 1
	}

	for idx := start; idx > -1; idx-- {
		curFilename := q.queue[idx]
		if curFilename == filename {
			newIdx = idx

			break
		} else {
			q.lastIndex[curFilename] = idx
		}
	}

	return newIdx
}

// removeWorst a file from the LRU queue from worsts (head)
func (q *LRU) removeWorst(files []int64) (err error) {
	for idx, name := range files {
		filename := name
		queueFilename := q.queue[idx]

		if filename != queueFilename {
			return fmt.Errorf("lru remove worst: file %d != %d", filename, queueFilename)
		}

		delete(q.lastIndex, filename)
		delete(q.files, filename)
	}

	// fmt.Println("QUEUE", q.queue)
	// fmt.Println("2REMOVE", files)

	copy(q.queue, q.queue[len(files):])
	q.queue = q.queue[:len(q.queue)-len(files)]

	// fmt.Println("QUEUE", q.queue)

	return nil
}

// remove a file from the LRU queue
func (q *LRU) remove(files []int64) (err error) {

	for _, name := range files {
		filename := name

		idx2remove, inIndexes := q.lastIndex[filename]

		// fmt.Printf("REMOVE -> %d from %d\n", filename, idx2remove)

		if !inIndexes {
			return fmt.Errorf("lru remove: file %d has no index", filename)
		}

		if idx2remove >= len(q.queue) || q.queue[idx2remove] != filename {
			idx2remove = q.findIndex(filename, idx2remove)
		}

		copy(q.queue[idx2remove:], q.queue[idx2remove+1:])
		q.queue = q.queue[:len(q.queue)-1]

		delete(q.lastIndex, filename)
		delete(q.files, filename)
	}

	return nil
}

// update a file of the LRU queue
func (q *LRU) update(file *files.Stats) (err error) {
	// fmt.Printf("UPDATE -> %d\n", file.Filename)
	filename := file.Filename

	lastIdx, inIndexes := q.lastIndex[filename]
	stats, inMap := q.files[filename]

	if lastIdx >= len(q.queue) || q.queue[lastIdx] != filename {
		lastIdx = q.findIndex(filename, lastIdx)
	}

	switch {
	case !inIndexes:
		return fmt.Errorf("lru update: file %d not stored in indexes", filename)
	case !inMap:
		return fmt.Errorf("lru update: file %d not stored in queue", filename)
	case file != stats:
		// fmt.Println(file, man.files[file.Filename])
		// fmt.Println(file.Filename, man.files[file.Filename].Filename)
		return fmt.Errorf("lru update: different stats -> %v != %v", file, stats)
	}

	copy(q.queue[lastIdx:], q.queue[lastIdx+1:])

	newIndex := len(q.queue) - 1

	q.queue[newIndex] = filename
	q.lastIndex[filename] = newIndex

	return nil
}
