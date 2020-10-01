package queue

import (
	"math/rand"
	"testing"

	cacheFiles "simulator/v2/cache/files"
)

// func BenchmarkQueue(b *testing.B) {
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			r := rand.New(rand.NewSource(42))

// 			man := Manager{}
// 			man.Init(LRUQueue)

// 			for idx := 0; idx < numTests; idx++ {
// 				man.Insert(
// 					&Stats{
// 						Filename:  int64(idx),
// 						Frequency: r.Int63n(maxInt),
// 						Recency:   int64(numTests - idx),
// 						Size:      r.Float64() * maxFloat,
// 					},
// 				)
// 			}
// 			for idx := 0; idx < numTests; idx++ {
// 				man.Update(
// 					&Stats{
// 						Filename:  int64(numTests),
// 						Frequency: r.Int63n(maxInt),
// 						Recency:   r.Int63n(maxInt),
// 						Size:      r.Float64() * maxFloat,
// 					},
// 				)
// 			}
// 		}
// 	})
// }

func TestLRUQueueBehaviorRandomActions(t *testing.T) { //nolint:ignore, funlen
	r := rand.New(rand.NewSource(42))
	q := &LRU{}

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
				Insert(q, curStat)

				inserted = append(inserted, curIdx)
				opDone = true

			case 1: // UPDATE
				if len(inserted) > 0 {
					curIdx := r.Intn(len(inserted))
					// fmt.Printf("[%d] UPDATE -> %d\n", i, inserted[curIdx])

					curStat := stats[inserted[curIdx]]
					curStat.Recency = int64(i)
					Update(q, curStat)
					opDone = true
				}
			case 2: // REMOVE
				if len(inserted) > 0 {
					curIdx := r.Intn(len(inserted))
					// fmt.Printf("[%d] REMOVE -> %d\n", i, inserted[curIdx])

					curStat := stats[inserted[curIdx]]
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

	var prevRecency int64 = -1
	for _, file := range GetFromWorst(q) {
		if prevRecency > file.Recency {
			t.Log("ERROR: Recency order not correct...")
			t.Fatal()
		}
		prevRecency = file.Recency
	}

	if Len(q) != len(inserted) {
		t.Log("ERROR: Len manager != len inserted")
		t.Fatal()
	}
}

func TestLRUQueueBehavior(t *testing.T) { //nolint:ignore, funlen
	r := rand.New(rand.NewSource(42))
	q := &LRU{}
	Init(q)

	files := make([]int64, numFiles)
	for idx := 0; idx < numFiles; idx++ {
		files[idx] = 100000 + int64(idx)
	}

	stats := make(map[int64]*cacheFiles.Stats)

	var lastRecency int64 = 0
	for _, filename := range files {
		curStat := cacheFiles.Stats{
			Filename:  filename,
			Frequency: -1,
			Recency:   lastRecency,
			Size:      1.,
		}
		stats[curStat.Filename] = &curStat
		Insert(q, stats[curStat.Filename])
		lastRecency++
	}

	toRemove := make([]int64, len(files)/2)
	toRemove = toRemove[:0]

	// fmt.Println("INSERT")
	for _, curFile := range GetFromWorst(q) {
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

	Remove(q, toRemove)

	toUpdate := make([]int64, Len(q))
	toUpdate = toUpdate[:0]

	// fmt.Println("REMOVE ->", toRemove)
	for _, curFile := range GetFromWorst(q) {
		// fmt.Println(curFile.Filename, "->", curFile.Recency)
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

	// fmt.Println(man.queueFilenames)
	// fmt.Println(man.queueI)
	// fmt.Println(man.fileIndexes)

	for numUpdate := 1; numUpdate < 11; numUpdate++ {
		oldValues := make(map[int64]int64)
		// fmt.Println("UPDATE ->", toUpdate)
		// fmt.Println(man.queueFilenames)
		// fmt.Println(man.queueI)
		// fmt.Println(man.fileIndexes)

		var lastRecency int64
		for _, filename := range toUpdate {
			oldValues[filename] = GetFileStats(q, filename).Recency
			stats[filename].Recency = lastRecency + numFiles*int64(numUpdate)
			Update(q, stats[filename])
			lastRecency++
		}

		var prevRecency int64 = -1
		for _, curFile := range GetFromWorst(q) {
			// fmt.Println(curFile.Filename, "->", curFile.Recency)
			_, inToUpdate := oldValues[curFile.Filename]
			if inToUpdate && curFile.Recency == oldValues[curFile.Filename] {
				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
				t.Fatal()
			}
			if prevRecency != -1 && prevRecency > curFile.Recency {
				t.Log("LRU order not valid")
				t.Fatal()
			}
			prevRecency = curFile.Recency
		}
	}

	// fmt.Printf("%#v\n", q.files)
	// fmt.Printf("%#v\n", q.indexes)
	// fmt.Printf("%#v\n", q.queue)
	// fmt.Printf("%#v\n", q.least)
	// fmt.Printf("%#v\n", q.next)

	remainFiles := make([]*cacheFiles.Stats, 0)
	remainFiles = append(remainFiles, GetFromWorst(q)...)

	if len(remainFiles) == 0 {
		panic("ERROR: empty ...")
	}

	// fmt.Println("--- Remain Files ---")
	// for idx, curFile := range remainFiles {
	// 	fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
	// }
	curRemainIdx := 0
	sizeToRemove := 1.0
	for Len(q) != curRemainIdx {
		toRemove := GetWorstFilesUp2Size(q, sizeToRemove)
		// fmt.Println("--- To REMOVE ---")
		// for idx, curFile := range toRemove {
		// 	fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
		// }
		if toRemove[curRemainIdx].Filename != remainFiles[curRemainIdx].Filename {
			panic("ERROR: GetWorstFilesUp2Size not work properly")
		}
		curRemainIdx++
		sizeToRemove += 1.0
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
