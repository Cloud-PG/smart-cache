package cache

import (
	"fmt"
	"log"
	"sort"
)

type QueueLFU struct {
	files       map[int64]*FileStats
	lastFreq    map[int64]int64
	lastIndex   map[int64]int
	frequencies []int64
	queue       map[int64][]int64
	buffer      []*FileStats
}

// init initialize the struct
func (q *QueueLFU) init() {
	q.files = make(map[int64]*FileStats, estimatedNumFiles)
	q.lastFreq = make(map[int64]int64, estimatedNumFiles)
	q.lastIndex = make(map[int64]int, estimatedNumFiles)
	q.frequencies = make([]int64, 0)
	q.queue = make(map[int64][]int64, estimatedNumFiles)
	q.buffer = make([]*FileStats, 0, bufferSize)
}

// getFileStats from a file in queue
func (q *QueueLFU) getFileStats(filename int64) *FileStats {
	stats, inQueue := q.files[filename]

	if !inQueue {
		log.Fatal(fmt.Errorf("lfu getFileStats: file %d already in queue", filename))
	}

	return stats
}

// getQueue values from a queue
func (q *QueueLFU) getQueue() []*FileStats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idxFreq := len(q.frequencies) - 1; idxFreq > -1; idxFreq-- {
		curQueue := q.queue[q.frequencies[idxFreq]]
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
func (q *QueueLFU) getFromWorst() []*FileStats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idxFreq := 0; idxFreq < len(q.frequencies); idxFreq++ {
		curQueue := q.queue[q.frequencies[idxFreq]]
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
func (q *QueueLFU) getWorstFilesUp2Size(totSize float64) []*FileStats {
	if totSize <= 0. {
		panic("ERROR: tot size is negative or equal to 0")
	}

	var sended float64

	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for idxFreq := 0; idxFreq < len(q.frequencies); idxFreq++ {
		curQueue := q.queue[q.frequencies[idxFreq]]
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
func (q *QueueLFU) check(file int64) bool {
	_, inQueue := q.files[file]

	return inQueue
}

// checkFreq if a freq is in cache
func (q *QueueLFU) checkFreq(freq int64) bool {
	_, present := q.queue[freq]

	return present
}

// len returns the number of files in cache
func (q *QueueLFU) len() int {
	return len(q.files)
}

func (q *QueueLFU) insertFreq(freq int64) {
	insertIdx := sort.Search(len(q.frequencies), func(idx int) bool {
		return q.frequencies[idx] > freq
	})

	if insertIdx == len(q.frequencies) {
		q.frequencies = append(q.frequencies, freq)
	} else {
		// Trick
		// https://github.com/golang/go/wiki/SliceTricks#insert
		q.frequencies = append(q.frequencies, -1)
		copy(q.frequencies[insertIdx+1:], q.frequencies[insertIdx:])
		q.frequencies[insertIdx] = freq
	}
}

// insert a file into the LFU queue
func (q *QueueLFU) insert(file *FileStats) (err error) {
	filename := file.Filename
	freq := file.Frequency

	if q.check(filename) {
		return fmt.Errorf("lfu insert: file %d already in queue", filename)
	}

	if !q.checkFreq(freq) {
		q.queue[freq] = make([]int64, 0)
		q.insertFreq(freq)
	}

	curQueue := q.queue[freq]
	curIdx := len(curQueue)

	// fmt.Printf("INSERT -> %d in %d\n", file.Filename, curIdx)

	curQueue = append(curQueue, filename)

	q.queue[freq] = curQueue
	q.lastFreq[filename] = freq
	q.lastIndex[filename] = curIdx
	q.files[filename] = file

	return nil
}

func (q *QueueLFU) findIndex(filename int64, curQueue []int64, lastIdx int) int {
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

// // removeWorst a file from the LFU queue from worsts (head)
func (q *QueueLFU) removeWorst(files []int64) (err error) {

	for idxFreq := 0; idxFreq < len(q.frequencies); idxFreq++ {
		curFreq := q.frequencies[idxFreq]
		curQueue := q.queue[curFreq]

		for idx, name := range files {
			filename := name
			queueFilename := curQueue[idx]

			if filename != queueFilename {
				return fmt.Errorf("lfu remove worst: file %d != %d", filename, queueFilename)
			}

			delete(q.lastIndex, filename)
			delete(q.lastFreq, filename)
			delete(q.files, filename)
		}

		if len(files) >= len(curQueue) {
			delete(q.queue, curFreq)
			copy(files, files[len(curQueue):])
			files = files[:len(files)-len(curQueue)]
			if len(files) == 0 {
				break
			}
		} else {
			copy(curQueue, curQueue[len(files):])
			curQueue = curQueue[:len(curQueue)-len(files)]
			q.queue[curFreq] = curQueue
			break
		}
	}

	// fmt.Println("QUEUE", q.queue)

	return nil
}

// remove a file from the LFU queue
func (q *QueueLFU) remove(files []int64) (err error) {

	for _, name := range files {
		filename := name

		idx2remove, inIndexes := q.lastIndex[filename]
		freq2remove, inFreq := q.lastFreq[filename]

		// fmt.Printf("REMOVE -> %d from %d\n", filename, idx2remove)

		if !inIndexes {
			return fmt.Errorf("lfu remove: file %d has no index", filename)
		} else if !inFreq {
			return fmt.Errorf("lfu remove: file %d has no freq", filename)
		}

		curQueue := q.queue[freq2remove]

		if idx2remove >= len(curQueue) || curQueue[idx2remove] != filename {
			idx2remove = q.findIndex(filename, curQueue, idx2remove)
		}

		copy(curQueue[idx2remove:], curQueue[idx2remove+1:])
		curQueue = curQueue[:len(curQueue)-1]

		if len(curQueue) > 0 {
			q.queue[freq2remove] = curQueue
		} else {
			delete(q.queue, freq2remove)

			freqDelIdx := sort.Search(len(q.frequencies), func(idx int) bool {
				return q.frequencies[idx] >= freq2remove
			})
			if freqDelIdx < len(q.frequencies) && q.frequencies[freqDelIdx] == freq2remove {
				copy(q.frequencies[freqDelIdx:], q.frequencies[freqDelIdx+1:])
				q.frequencies = q.frequencies[:len(q.frequencies)-1]
			} else {
				return fmt.Errorf("lfu remove: freq %d not present", freq2remove)
			}
		}

		delete(q.lastIndex, filename)
		delete(q.lastFreq, filename)
		delete(q.files, filename)
	}

	return nil
}

// update a file of the LFU queue
func (q *QueueLFU) update(file *FileStats) (err error) {
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

	q.remove([]int64{filename})
	q.insert(file)

	return nil
}
