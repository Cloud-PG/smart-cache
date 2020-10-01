package queue

const (
	numTests = 1000
	maxInt   = 1000
	maxFloat = 10000.
	numFiles = 9999
	numSteps = 100000
)

// func BenchmarkQueue(b *testing.B) {
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			r := rand.New(rand.NewSource(42))

// 			man := Manager{}
// 			man.Init(LRUQueue)

// 			for idx := 0; idx < numTests; idx++ {
// 				man.Insert(
// 					&FileStats{
// 						Filename:  int64(idx),
// 						Frequency: r.Int63n(maxInt),
// 						Recency:   int64(numTests - idx),
// 						Size:      r.Float64() * maxFloat,
// 					},
// 				)
// 			}
// 			for idx := 0; idx < numTests; idx++ {
// 				man.Update(
// 					&FileStats{
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

// func TestLRUQueueBehaviorRandomActions(t *testing.T) {
// 	r := rand.New(rand.NewSource(42))
// 	q := &QueueLRU{}

// 	QueueInit(q)

// 	stats := make([]*FileStats, 0)

// 	for idx := 0; idx < numFiles; idx++ {
// 		filename := 100000 + int64(idx)

// 		stats = append(stats, &FileStats{
// 			Filename:  filename,
// 			Frequency: 0,
// 			Recency:   -1,
// 			Size:      1.,
// 		},
// 		)
// 	}

// 	inserted := make([]int, 0)

// 	for i := 0; i < numSteps; i++ {
// 		opDone := false

// 		// fmt.Printf("%d\r", i)

// 		for opDone == false {
// 			switch r.Intn(3) {
// 			case 0: // INSERT
// 				curIdx := r.Intn(len(stats))
// 				// fmt.Printf("[%d] INSERT -> %d\n", i, curIdx)

// 				found := false
// 				for _, idx := range inserted {
// 					if idx == curIdx {
// 						found = true
// 					}
// 				}

// 				if found {
// 					break
// 				}

// 				curStat := stats[curIdx]

// 				curStat.Recency = int64(i)
// 				QueueInsert(q, curStat)

// 				inserted = append(inserted, curIdx)
// 				opDone = true

// 			case 1: // UPDATE
// 				if len(inserted) > 0 {
// 					curIdx := r.Intn(len(inserted))
// 					// fmt.Printf("[%d] UPDATE -> %d\n", i, inserted[curIdx])

// 					curStat := stats[inserted[curIdx]]
// 					curStat.Recency = int64(i)
// 					QueueUpdate(q, curStat)
// 					opDone = true
// 				}
// 			case 2: // REMOVE
// 				if len(inserted) > 0 {
// 					curIdx := r.Intn(len(inserted))
// 					// fmt.Printf("[%d] REMOVE -> %d\n", i, inserted[curIdx])

// 					curStat := stats[inserted[curIdx]]
// 					QueueRemove(q, []int64{curStat.Filename})

// 					copy(inserted[curIdx:], inserted[curIdx+1:])
// 					inserted = inserted[:len(inserted)-1]

// 					opDone = true
// 				}
// 			}
// 		}
// 	}

// 	for _, idx := range inserted {
// 		curStat := stats[idx]
// 		if !QueueCheck(q, curStat.Filename) {
// 			t.Log("ERROR: File '", curStat.Filename, "' not found...")
// 			t.Fatal()
// 		}
// 	}

// 	var prevRecency int64 = -1
// 	for _, file := range QueueGetFromWorst(q) {
// 		if prevRecency > file.Recency {
// 			t.Log("ERROR: Recency order not correct...")
// 			t.Fatal()
// 		}
// 		prevRecency = file.Recency
// 	}

// 	if QueueLen(q) != len(inserted) {
// 		t.Log("ERROR: Len manager != len inserted")
// 		t.Fatal()
// 	}
// }

// func TestLFUQueueBehaviorRandomActions(t *testing.T) {
// 	r := rand.New(rand.NewSource(42))
// 	man := Manager{}
// 	man.Init(LFUQueue)

// 	stats := make([]*FileStats, 0)

// 	for idx := 0; idx < numFiles; idx++ {
// 		filename := 100000 + int64(idx)

// 		stats = append(stats, &FileStats{
// 			Filename:  filename,
// 			Frequency: 0,
// 			Recency:   -1,
// 			Size:      1.,
// 		},
// 		)
// 	}

// 	inserted := make([]int, 0)

// 	for i := 0; i < numSteps; i++ {
// 		opDone := false

// 		// fmt.Printf("%d\r", i)

// 		for opDone == false {
// 			switch r.Intn(3) {
// 			case 0: // INSERT
// 				curIdx := r.Intn(len(stats))
// 				// fmt.Printf("[%d] INSERT -> %d\n", i, curIdx)

// 				found := false
// 				for _, idx := range inserted {
// 					if idx == curIdx {
// 						found = true
// 					}
// 				}

// 				if found {
// 					break
// 				}

// 				curStat := stats[curIdx]

// 				curStat.Recency = int64(i)
// 				curStat.Frequency++

// 				man.Insert(curStat)

// 				inserted = append(inserted, curIdx)

// 				opDone = true
// 			case 1: // UPDATE
// 				if len(inserted) > 0 {
// 					curIdx := r.Intn(len(inserted))
// 					// fmt.Printf("[%d] UPDATE -> %d\n", i, inserted[curIdx])

// 					curStat := stats[inserted[curIdx]]

// 					curStat.Recency = int64(i)
// 					curStat.Frequency++

// 					man.Update(curStat)

// 					opDone = true
// 				}
// 			case 2: // REMOVE
// 				if len(inserted) > 0 {
// 					curIdx := r.Intn(len(inserted))
// 					// fmt.Printf("[%d] REMOVE -> %d\n", i, inserted[curIdx])

// 					curStat := stats[inserted[curIdx]]
// 					curStat.Frequency = 0

// 					man.Remove([]int64{curStat.Filename})

// 					copy(inserted[curIdx:], inserted[curIdx+1:])
// 					inserted = inserted[:len(inserted)-1]

// 					opDone = true
// 				}
// 			}
// 		}
// 	}

// 	for _, idx := range inserted {
// 		curStat := stats[idx]
// 		if !man.Check(curStat.Filename) {
// 			t.Log("ERROR: File '", curStat.Filename, "' not found...")
// 			t.Fatal()
// 		}
// 	}

// 	var prevFreq int64 = -1
// 	for _, file := range man.GetFromWorst() {
// 		if prevFreq > file.Frequency {
// 			t.Log("ERROR: Frequency order not correct...")
// 			t.Fatal()
// 		}
// 		prevFreq = file.Frequency
// 	}

// 	if man.Len() != len(inserted) {
// 		t.Log("ERROR: Len manager != len inserted")
// 		t.Fatal()
// 	}
// }

// func TestLRUQueueBehavior(t *testing.T) {
// 	r := rand.New(rand.NewSource(42))
// 	man := Manager{}
// 	man.Init(LRUQueue)

// 	files := make([]int64, numFiles)
// 	for idx := 0; idx < numFiles; idx++ {
// 		files[idx] = 100000 + int64(idx)
// 	}

// 	stats := make(map[int64]*FileStats)

// 	var lastRecency int64 = 0
// 	for _, filename := range files {
// 		curStat := FileStats{
// 			Filename:  filename,
// 			Frequency: -1,
// 			Recency:   lastRecency,
// 			Size:      1.,
// 		}
// 		stats[curStat.Filename] = &curStat
// 		man.Insert(stats[curStat.Filename])
// 		lastRecency++
// 	}

// 	toRemove := make([]int64, len(files)/2)
// 	toRemove = toRemove[:0]

// 	// fmt.Println("INSERT")
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Recency)
// 		inserted := false
// 		for _, filename := range files {
// 			if filename == curFile.Filename {
// 				inserted = true

// 				break
// 			}
// 		}
// 		if !inserted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not inserted...")
// 			t.Fatal()
// 		}
// 		if len(toRemove) != cap(toRemove) && r.Int31n(2) == 1 {
// 			toRemove = append(toRemove, curFile.Filename)
// 		}
// 	}

// 	// fmt.Println(man.queueFilenames)
// 	// fmt.Println(man.queueI)
// 	// fmt.Println(man.fileIndexes)

// 	man.Remove(toRemove)

// 	toUpdate := make([]int64, man.Len())
// 	toUpdate = toUpdate[:0]

// 	// fmt.Println("REMOVE ->", toRemove)
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Recency)
// 		notDeleted := true
// 		for _, filename := range toRemove {
// 			if filename == curFile.Filename {
// 				notDeleted = false
// 				break
// 			}
// 		}
// 		if !notDeleted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not deleted...")
// 			t.Fatal()
// 		}
// 		if len(toUpdate) != cap(toUpdate) && r.Int31n(2) == 1 {
// 			toUpdate = append(toUpdate, curFile.Filename)
// 		}
// 	}

// 	// fmt.Println(man.queueFilenames)
// 	// fmt.Println(man.queueI)
// 	// fmt.Println(man.fileIndexes)

// 	for numUpdate := 1; numUpdate < 11; numUpdate++ {
// 		oldValues := make(map[int64]int64)
// 		// fmt.Println("UPDATE ->", toUpdate)
// 		// fmt.Println(man.queueFilenames)
// 		// fmt.Println(man.queueI)
// 		// fmt.Println(man.fileIndexes)

// 		var lastRecency int64
// 		for _, filename := range toUpdate {
// 			oldValues[filename] = man.GetFileStats(filename).Recency
// 			stats[filename].Recency = lastRecency + numFiles*int64(numUpdate)
// 			man.Update(stats[filename])
// 			lastRecency++
// 		}

// 		var prevRecency int64 = -1
// 		for _, curFile := range man.GetFromWorst() {
// 			// fmt.Println(curFile.Filename, "->", curFile.Recency)
// 			_, inToUpdate := oldValues[curFile.Filename]
// 			if inToUpdate && curFile.Recency == oldValues[curFile.Filename] {
// 				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
// 				t.Fatal()
// 			}
// 			if prevRecency != -1 && prevRecency > curFile.Recency {
// 				t.Log("LRU order not valid")
// 				t.Fatal()
// 			}
// 			prevRecency = curFile.Recency
// 		}
// 	}

// 	remainFiles := make([]*FileStats, 0)
// 	remainFiles = append(remainFiles, man.GetFromWorst()...)

// 	if len(remainFiles) == 0 {
// 		panic("ERROR: empty queue...")
// 	}

// 	// fmt.Println("--- Remain Files ---")
// 	// for idx, curFile := range remainFiles {
// 	// 	fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
// 	// }
// 	curRemainIdx := 0
// 	sizeToRemove := 1.0
// 	for man.Len() != curRemainIdx {
// 		toRemove := man.GetWorstFilesUp2Size(sizeToRemove)
// 		// fmt.Println("--- To REMOVE ---")
// 		// for idx, curFile := range toRemove {
// 		// 	fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
// 		// }
// 		if toRemove[curRemainIdx].Filename != remainFiles[curRemainIdx].Filename {
// 			panic("ERROR: GetWorstFilesUp2Size not work properly")
// 		}
// 		curRemainIdx++
// 		sizeToRemove += 1.0
// 	}

// 	for man.Len() > 0 {
// 		toRemove := man.GetWorstFilesUp2Size(2.0)
// 		// fmt.Println("--- To REMOVE ---")
// 		// for idx, curFile := range toRemove {
// 		// 	fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
// 		// }
// 		for _, file := range toRemove {
// 			man.Remove([]int64{file.Filename})
// 		}
// 		if man.Len() > 0 && len(toRemove) != 2 {
// 			panic("ERROR: GetWorstFilesUp2Size not work properly")
// 		}
// 	}
// }

// func TestLFUQueueBehavior(t *testing.T) {
// 	r := rand.New(rand.NewSource(28))
// 	man := Manager{}
// 	man.Init(LFUQueue)

// 	files := make([]int64, numFiles)
// 	for idx := 0; idx < numFiles; idx++ {
// 		files[idx] = 100000 + int64(idx)
// 	}

// 	stats := make(map[int64]*FileStats)

// 	for _, filename := range files {
// 		curStat := FileStats{
// 			Filename:  filename,
// 			Frequency: r.Int63n(numFiles),
// 			Recency:   -1,
// 			Size:      -1.,
// 		}
// 		stats[curStat.Filename] = &curStat
// 		man.Insert(stats[curStat.Filename])
// 	}

// 	toRemove := make([]int64, len(files)/2)
// 	toRemove = toRemove[:0]

// 	// fmt.Println("INSERT")
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Frequency)
// 		inserted := false
// 		for _, filename := range files {
// 			if filename == curFile.Filename {
// 				inserted = true
// 				break
// 			}
// 		}
// 		if !inserted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not inserted...")
// 			t.Fatal()
// 		}
// 		if len(toRemove) != cap(toRemove) && r.Int31n(2) == 1 {
// 			toRemove = append(toRemove, curFile.Filename)
// 		}
// 	}

// 	// fmt.Println(man.queueFilenames)
// 	// fmt.Println(man.queueI)
// 	// fmt.Println(man.fileIndexes)

// 	man.Remove(toRemove)

// 	toUpdate := make([]int64, man.Len())
// 	toUpdate = toUpdate[:0]

// 	// fmt.Println("REMOVE ->", toRemove)
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Frequency)
// 		notDeleted := true
// 		for _, filename := range toRemove {
// 			if filename == curFile.Filename {
// 				notDeleted = false
// 				break
// 			}
// 		}
// 		if !notDeleted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not deleted...")
// 			t.Fatal()
// 		}
// 		if len(toUpdate) != cap(toUpdate) && r.Int31n(2) == 1 {
// 			toUpdate = append(toUpdate, curFile.Filename)
// 		}
// 	}

// 	for numUpdate := 1; numUpdate < 11; numUpdate++ {
// 		oldValues := make(map[int64]int64)
// 		// fmt.Println("UPDATE ->", toUpdate)
// 		for _, filename := range toUpdate {
// 			oldValues[filename] = man.GetFileStats(filename).Frequency
// 			stats[filename].Frequency = r.Int63n(numFiles) + numFiles*int64(numUpdate)
// 			man.Update(stats[filename])
// 		}

// 		var prevFrequency int64 = -1
// 		for _, curFile := range man.GetFromWorst() {
// 			// fmt.Println(curFile.Filename, "->", curFile.Frequency)
// 			_, inToUpdate := oldValues[curFile.Filename]
// 			if inToUpdate && curFile.Frequency == oldValues[curFile.Filename] {
// 				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
// 				t.Fatal()
// 			}
// 			if prevFrequency != -1 && prevFrequency > curFile.Frequency {
// 				t.Log("LFU order not valid")
// 				t.Fatal()
// 			}
// 			prevFrequency = curFile.Frequency
// 		}
// 	}
// }

// func TestSizeSmallQueueBehavior(t *testing.T) {
// 	r := rand.New(rand.NewSource(42))
// 	man := Manager{}
// 	man.Init(SizeSmallQueue)

// 	files := make([]int64, numFiles)
// 	for idx := 0; idx < numFiles; idx++ {
// 		files[idx] = 100000 + int64(idx)
// 	}

// 	stats := make(map[int64]*FileStats)

// 	for _, filename := range files {
// 		curStat := FileStats{
// 			Filename:  filename,
// 			Frequency: -1,
// 			Recency:   r.Int63n(numFiles),
// 			Size:      r.Float64() * 1024.,
// 		}
// 		stats[curStat.Filename] = &curStat
// 		man.Insert(stats[curStat.Filename])
// 	}

// 	toRemove := make([]int64, len(files)/2)
// 	toRemove = toRemove[:0]

// 	// fmt.Println("INSERT")
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Recency)
// 		inserted := false
// 		for _, filename := range files {
// 			if filename == curFile.Filename {
// 				inserted = true
// 				break
// 			}
// 		}
// 		if !inserted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not inserted...")
// 			t.Fatal()
// 		}
// 		if len(toRemove) != cap(toRemove) && r.Int31n(2) == 1 {
// 			toRemove = append(toRemove, curFile.Filename)
// 		}
// 	}

// 	man.Remove(toRemove)

// 	toUpdate := make([]int64, man.Len())
// 	toUpdate = toUpdate[:0]

// 	// fmt.Println("REMOVE ->", toRemove)
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Size)
// 		notDeleted := true
// 		for _, filename := range toRemove {
// 			if filename == curFile.Filename {
// 				notDeleted = false
// 				break
// 			}
// 		}
// 		if !notDeleted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not deleted...")
// 			t.Fatal()
// 		}
// 		if len(toUpdate) != cap(toUpdate) && r.Int31n(2) == 1 {
// 			toUpdate = append(toUpdate, curFile.Filename)
// 		}
// 	}

// 	for numUpdate := 1; numUpdate < 11; numUpdate++ {
// 		oldValues := make(map[int64]float64)
// 		// fmt.Println("UPDATE ->", toUpdate)
// 		for _, filename := range toUpdate {
// 			oldValues[filename] = man.GetFileStats(filename).Size
// 			stats[filename].Recency = r.Int63n(numFiles) + numFiles*int64(numUpdate)
// 			stats[filename].Size = oldValues[filename] + r.Float64()*1024. + 1.0
// 			man.Update(stats[filename])
// 		}

// 		var prevSize float64 = -1.
// 		for _, curFile := range man.GetFromWorst() {
// 			// fmt.Println(curFile.Filename, "->", curFile.Size)
// 			_, inToUpdate := oldValues[curFile.Filename]
// 			if inToUpdate && curFile.Size <= oldValues[curFile.Filename] {
// 				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
// 				t.Fatal()
// 			}
// 			if prevSize != -1 && prevSize > curFile.Size {
// 				t.Log("SizeSmall order not valid")
// 				t.Fatal()
// 			}
// 			prevSize = curFile.Size
// 		}
// 	}
// }

// func TestSizeBigQueueBehavior(t *testing.T) {
// 	r := rand.New(rand.NewSource(73))
// 	man := Manager{}
// 	man.Init(SizeBigQueue)

// 	files := make([]int64, numFiles)
// 	for idx := 0; idx < numFiles; idx++ {
// 		files[idx] = 100000 + int64(idx)
// 	}

// 	stats := make(map[int64]*FileStats)

// 	for _, filename := range files {
// 		curStat := FileStats{
// 			Filename:  filename,
// 			Frequency: -1,
// 			Recency:   r.Int63n(numFiles),
// 			Size:      r.Float64() * 1024.,
// 		}
// 		stats[curStat.Filename] = &curStat
// 		man.Insert(stats[curStat.Filename])
// 	}

// 	toRemove := make([]int64, len(files)/2)
// 	toRemove = toRemove[:0]

// 	// fmt.Println("INSERT")
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Recency)
// 		inserted := false
// 		for _, filename := range files {
// 			if filename == curFile.Filename {
// 				inserted = true
// 				break
// 			}
// 		}
// 		if !inserted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not inserted...")
// 			t.Fatal()
// 		}
// 		if len(toRemove) != cap(toRemove) && r.Int31n(2) == 1 {
// 			toRemove = append(toRemove, curFile.Filename)
// 		}
// 	}

// 	man.Remove(toRemove)

// 	toUpdate := make([]int64, man.Len())
// 	toUpdate = toUpdate[:0]

// 	// fmt.Println("REMOVE ->", toRemove)
// 	for _, curFile := range man.GetFromWorst() {
// 		// fmt.Println(curFile.Filename, "->", curFile.Size)
// 		notDeleted := true
// 		for _, filename := range toRemove {
// 			if filename == curFile.Filename {
// 				notDeleted = false
// 				break
// 			}
// 		}
// 		if !notDeleted {
// 			t.Log("ERROR: File '", curFile.Filename, "' not deleted...")
// 			t.Fatal()
// 		}
// 		if len(toUpdate) != cap(toUpdate) && r.Int31n(2) == 1 {
// 			toUpdate = append(toUpdate, curFile.Filename)
// 		}
// 	}

// 	for numUpdate := 1; numUpdate < 11; numUpdate++ {
// 		oldValues := make(map[int64]float64)
// 		// fmt.Println("UPDATE ->", toUpdate)
// 		for _, filename := range toUpdate {
// 			oldValues[filename] = man.GetFileStats(filename).Size
// 			stats[filename].Recency = r.Int63n(numFiles) + numFiles*int64(numUpdate)
// 			stats[filename].Size = oldValues[filename] + r.Float64()*1024. + 1.0
// 			man.Update(stats[filename])
// 		}

// 		var prevSize float64 = -1.
// 		for _, curFile := range man.GetFromWorst() {
// 			// fmt.Println(curFile.Filename, "->", curFile.Size)
// 			_, inToUpdate := oldValues[curFile.Filename]
// 			if inToUpdate && curFile.Size <= oldValues[curFile.Filename] {
// 				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
// 				t.Fatal()
// 			}
// 			if prevSize != -1 && prevSize < curFile.Size {
// 				t.Log("SizeBig order not valid")
// 				t.Fatal()
// 			}
// 			prevSize = curFile.Size
// 		}
// 	}
// }
