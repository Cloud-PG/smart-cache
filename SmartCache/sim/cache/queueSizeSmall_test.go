package cache

import (
	"math/rand"
	"testing"
)

func TestSizeSmallQueueBehavior(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	q := &QueueSizeSmall{}
	QueueInit(q)

	files := make([]int64, numFiles)
	for idx := 0; idx < numFiles; idx++ {
		files[idx] = 100000 + int64(idx)
	}

	stats := make(map[int64]*FileStats)

	for _, filename := range files {
		curStat := FileStats{
			Filename:  filename,
			Frequency: -1,
			Recency:   r.Int63n(numFiles),
			Size:      r.Float64() * 1024.,
		}
		stats[curStat.Filename] = &curStat
		QueueInsert(q, stats[curStat.Filename])
	}

	toRemove := make([]int64, len(files)/2)
	toRemove = toRemove[:0]

	// fmt.Println("INSERT")
	for _, curFile := range QueueGetFromWorst(q) {
		// fmt.Println(curFile.Filename, "->", curFile.Recency)
		inserted := false
		for _, filename := range files {
			if filename == curFile.Filename {
				inserted = true
				break
			}
		}
		if !inserted {
			t.Log("ERROR: File '", curFile.Filename, "' not inserted...")
			t.Fatal()
		}
		if len(toRemove) != cap(toRemove) && r.Int31n(2) == 1 {
			toRemove = append(toRemove, curFile.Filename)
		}
	}

	QueueRemove(q, toRemove)

	toUpdate := make([]int64, QueueLen(q))
	toUpdate = toUpdate[:0]

	// fmt.Println("REMOVE ->", toRemove)
	for _, curFile := range QueueGetFromWorst(q) {
		// fmt.Println(curFile.Filename, "->", curFile.Size)
		notDeleted := true
		for _, filename := range toRemove {
			if filename == curFile.Filename {
				notDeleted = false
				break
			}
		}
		if !notDeleted {
			t.Log("ERROR: File '", curFile.Filename, "' not deleted...")
			t.Fatal()
		}
		if len(toUpdate) != cap(toUpdate) && r.Int31n(2) == 1 {
			toUpdate = append(toUpdate, curFile.Filename)
		}
	}

	for numUpdate := 1; numUpdate < 11; numUpdate++ {
		oldValues := make(map[int64]float64)
		// fmt.Println("UPDATE ->", toUpdate)
		for _, filename := range toUpdate {
			oldValues[filename] = QueueGetFileStats(q, filename).Size
			stats[filename].Recency = r.Int63n(numFiles) + numFiles*int64(numUpdate)
			stats[filename].Size = oldValues[filename] + r.Float64()*1024. + 1.0
			QueueUpdate(q, stats[filename])
		}

		var prevSize float64 = -1.
		for _, curFile := range QueueGetFromWorst(q) {
			// fmt.Println(curFile.Filename, "->", curFile.Size)
			_, inToUpdate := oldValues[curFile.Filename]
			if inToUpdate && curFile.Size <= oldValues[curFile.Filename] {
				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
				t.Fatal()
			}
			if prevSize != -1 && prevSize > curFile.Size {
				t.Log("SizeSmall order not valid")
				t.Fatal()
			}
			prevSize = curFile.Size
		}
	}
}
