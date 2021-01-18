package queue

import (
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
	// Unassigned
	Unassigned
)

const (
	estimatedNumFiles = 1 << 19
	bufferSize        = estimatedNumFiles >> 2
)

type Queue interface {
	init()
	Check(file int64) bool
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
	return queue.Check(file)
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
