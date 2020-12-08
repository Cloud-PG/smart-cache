package queue

import (
	"math/rand"
	"testing"

	cacheFiles "simulator/v2/cache/files"
)

func TestLFUQueueBehaviorRandomActions(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	q := &LFU{}
	Init(q)

	stats := make([]*cacheFiles.Stats, 0)

	for idx := 0; idx < numFiles; idx++ {
		filename := 100000 + int64(idx)

		stats = append(stats, &cacheFiles.Stats{
			Filename:  filename,
			Frequency: 0,
			Recency:   -1,
			Size:      1.,
		},
		)
	}

	inserted := make([]int, 0)

	for i := 0; i < numSteps; i++ {
		opDone := false

		// fmt.Printf("%d\r", i)

		for opDone == false {
			switch r.Intn(3) {
			case 0: // INSERT
				curIdx := r.Intn(len(stats))
				// fmt.Printf("[%d] INSERT -> %d\n", i, curIdx)

				found := false
				for _, idx := range inserted {
					if idx == curIdx {
						found = true
					}
				}

				if found {
					break
				}

				curStat := stats[curIdx]

				curStat.Recency = int64(i)
				curStat.Frequency++

				Insert(q, curStat)

				inserted = append(inserted, curIdx)

				opDone = true
			case 1: // UPDATE
				if len(inserted) > 0 {
					curIdx := r.Intn(len(inserted))
					// fmt.Printf("[%d] UPDATE -> %d\n", i, inserted[curIdx])

					curStat := stats[inserted[curIdx]]

					curStat.Recency = int64(i)
					curStat.Frequency++

					Update(q, curStat)

					opDone = true
				}
			case 2: // REMOVE
				if len(inserted) > 0 {
					curIdx := r.Intn(len(inserted))
					// fmt.Printf("[%d] REMOVE -> %d\n", i, inserted[curIdx])

					curStat := stats[inserted[curIdx]]
					curStat.Frequency = 0

					Remove(q, []int64{curStat.Filename})

					copy(inserted[curIdx:], inserted[curIdx+1:])
					inserted = inserted[:len(inserted)-1]

					opDone = true
				}
			}
		}
	}

	for _, idx := range inserted {
		curStat := stats[idx]
		if !Check(q, curStat.Filename) {
			t.Log("ERROR: File '", curStat.Filename, "' not found...")
			t.Fatal()
		}
	}

	var prevFreq int64 = -1
	for _, file := range GetFromWorst(q) {
		if prevFreq > file.Frequency {
			t.Log("ERROR: Frequency order not correct...")
			t.Fatal()
		}
		prevFreq = file.Frequency
	}

	if Len(q) != len(inserted) {
		t.Log("ERROR: Len manager != len inserted")
		t.Fatal()
	}
}

func TestLFUQueueBehavior(t *testing.T) {
	r := rand.New(rand.NewSource(28))
	q := &LFU{}
	Init(q)

	files := make([]int64, numFiles)
	for idx := 0; idx < numFiles; idx++ {
		files[idx] = 100000 + int64(idx)
	}

	stats := make(map[int64]*cacheFiles.Stats)

	for _, filename := range files {
		curStat := cacheFiles.Stats{
			Filename:  filename,
			Frequency: r.Int63n(numFiles),
			Recency:   -1,
			Size:      1.,
		}
		stats[curStat.Filename] = &curStat
		Insert(q, stats[curStat.Filename])
	}

	toRemove := make([]int64, len(files)/2)
	toRemove = toRemove[:0]

	// fmt.Println("INSERT")
	for _, curFile := range GetFromWorst(q) {
		// fmt.Println(curFile.Filename, "->", curFile.Frequency)
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

	// fmt.Println(QueuequeueFilenames)q,
	// fmt.Println(QueuequeueI)q,
	// fmt.Println(QueuefileIndexes)q,

	Remove(q, toRemove)

	toUpdate := make([]int64, Len(q))
	toUpdate = toUpdate[:0]

	// fmt.Println("REMOVE ->", toRemove)
	for _, curFile := range GetFromWorst(q) {
		// fmt.Println(curFile.Filename, "->", curFile.Frequency)
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
		oldValues := make(map[int64]int64)
		// fmt.Println("UPDATE ->", toUpdate)
		for _, filename := range toUpdate {
			oldValues[filename] = GetFileStats(q, filename).Frequency
			stats[filename].Frequency = r.Int63n(numFiles) + numFiles*int64(numUpdate)
			Update(q, stats[filename])
		}

		var prevFrequency int64 = -1
		for _, curFile := range GetFromWorst(q) {
			// fmt.Println(curFile.Filename, "->", curFile.Frequency)
			_, inToUpdate := oldValues[curFile.Filename]
			if inToUpdate && curFile.Frequency == oldValues[curFile.Filename] {
				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
				t.Fatal()
			}
			if prevFrequency != -1 && prevFrequency > curFile.Frequency {
				t.Log("LFU order not valid")
				t.Fatal()
			}
			prevFrequency = curFile.Frequency
		}
	}

	for Len(q) > 0 {
		// fmt.Println(q.queue)
		stats := GetWorstFilesUp2Size(q, 2.0)
		toRemove := make([]int64, 0, len(stats))
		// fmt.Println("--- To REMOVE ---")
		for _, curFile := range stats {
			// fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
			toRemove = append(toRemove, curFile.Filename)
		}
		RemoveWorst(q, toRemove)
		if Len(q) > 0 && len(toRemove) != 2 {
			panic("ERROR: GetWorstFilesUp2Size not work properly")
		}
	}
}
