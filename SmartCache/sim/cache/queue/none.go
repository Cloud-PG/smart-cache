package queue

import (
	"fmt"
	"log"
	"simulator/v2/cache/files"
)

type QueueNone struct {
	files  map[int64]*files.Stats
	buffer []*files.Stats
}

// init initialize the struct
func (q *QueueNone) init() {
	q.files = make(map[int64]*files.Stats, estimatedNumFiles)
	q.buffer = make([]*files.Stats, 0, bufferSize)
}

// getFileStats from a file in queue
func (q *QueueNone) getFileStats(filename int64) *files.Stats {
	stats, inQueue := q.files[filename]

	if !inQueue {
		log.Fatal(fmt.Errorf("none queue getFileStats: file %d already in queue", filename))
	}

	return stats
}

// getQueue values from a queue
func (q *QueueNone) getQueue() []*files.Stats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	q.buffer = q.buffer[:0]

	for _, fileStats := range q.files {
		q.buffer = append(q.buffer, fileStats)
	}

	return q.buffer
}

// getFromWorst values from worst queue values
func (q *QueueNone) getFromWorst() []*files.Stats {
	// Filtering trick
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating

	return q.getQueue()
}

// getWorstFilesUp2Size values from a queue until size is reached
func (q *QueueNone) getWorstFilesUp2Size(totSize float64) []*files.Stats {
	panic("this is not necessary for a non queue algorithm")
}

// check if a file is in cache
func (q *QueueNone) Check(file int64) bool {
	_, inQueue := q.files[file]

	return inQueue
}

// len returns the number of files in cache
func (q *QueueNone) len() int {
	return len(q.files)
}

// insert a file into the LRU queue
func (q *QueueNone) insert(file *files.Stats) (err error) {
	filename := file.Filename

	if q.Check(filename) {
		return fmt.Errorf("none queue insert: file %d already in queue", filename)
	}

	q.files[filename] = file

	return nil
}

// removeWorst a file from the LRU queue from worsts (head)
func (q *QueueNone) removeWorst(files []int64) (err error) {
	panic("this is not necessary in a non queue algorithm")
}

// remove a file from the LRU queue
func (q *QueueNone) remove(files []int64) (err error) {
	for _, name := range files {
		filename := name
		delete(q.files, filename)
	}

	return nil
}

// update a file of the LRU queue
func (q *QueueNone) update(file *files.Stats) (err error) {
	// fmt.Printf("UPDATE -> %d\n", file.Filename)
	filename := file.Filename

	stats, inMap := q.files[filename]

	switch {
	case !inMap:
		return fmt.Errorf("none queue update: file %d not stored in queue", filename)
	case file != stats:
		// fmt.Println(file, man.files[file.Filename])
		// fmt.Println(file.Filename, man.files[file.Filename].Filename)
		return fmt.Errorf("none queue update: different stats -> %v != %v", file, stats)
	}

	return nil
}
