package cache

import (
	_ "fmt"
	"math/rand"
	"testing"
)

const (
	numTests = 1000
	maxInt   = 1000
	maxFloat = 10000.
	numFiles = 10
)

// func BenchmarkQueue(b *testing.B) {
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			r := rand.New(rand.NewSource(42))

// 			man := Manager{}
// 			man.Init(LRUQueue)

// 			for idx := 0; idx < numTests; idx++ {
// 				man.Insert(
// 					&FileSupportData{
// 						Filename:  int64(idx),
// 						Frequency: r.Int63n(maxInt),
// 						Recency:   int64(numTests - idx),
// 						Size:      r.Float64() * maxFloat,
// 					},
// 				)
// 			}
// 			for idx := 0; idx < numTests; idx++ {
// 				man.Update(
// 					&FileSupportData{
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

func TestLRUQueueBehavior(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	man := Manager{}
	man.Init(LRUQueue)

	files := make([]int64, numFiles)
	for idx := 0; idx < numFiles; idx++ {
		files[idx] = 100000 + int64(idx)
	}

	var lastRecency int64 = 0
	for _, filename := range files {
		man.Insert(
			&FileSupportData{
				Filename:  filename,
				Frequency: -1,
				Recency:   lastRecency,
				Size:      1.,
			},
		)
		lastRecency++
	}

	toRemove := make([]int64, len(files)/2)
	toRemove = toRemove[:0]

	// fmt.Println("INSERT")
	for _, curFile := range man.GetFromWorst() {
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

	man.Remove(toRemove)

	toUpdate := make([]int64, man.Len())
	toUpdate = toUpdate[:0]

	// fmt.Println("REMOVE ->", toRemove)
	for _, curFile := range man.GetFromWorst() {
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

	for numUpdate := 1; numUpdate < 11; numUpdate++ {
		oldValues := make(map[int64]int64)
		// fmt.Println("UPDATE ->", toUpdate)
		var lastRecency int64 = 0
		for _, filename := range toUpdate {
			oldValues[filename] = man.GetFileSupportData(filename).Recency
			man.Update(
				&FileSupportData{
					Filename:  filename,
					Frequency: -1,
					Recency:   lastRecency + numFiles*int64(numUpdate),
					Size:      1.,
				},
			)
			lastRecency++
		}

		var prevRecency int64 = -1
		for _, curFile := range man.GetFromWorst() {
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

	remainFiles := make([]*FileSupportData, 0)
	remainFiles = append(remainFiles, man.GetFromWorst()...)

	if len(remainFiles) == 0 {
		panic("ERROR: empty queue...")
	}

	// fmt.Println("--- Remain Files ---")
	// for idx, curFile := range remainFiles {
	// 	fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
	// }
	curRemainIdx := 0
	for man.Len() > 0 {
		toRemove := man.GetWorstFilesUp2Size(1.0)
		// fmt.Println("--- To REMOVE ---")
		// for idx, curFile := range toRemove {
		// 	fmt.Printf("[%d]->%d\n", idx, curFile.Filename)
		// }
		if toRemove[0].Filename != remainFiles[curRemainIdx].Filename {
			panic("ERROR: GetWorstFilesUp2Size not work properly")
		}
		man.Remove([]int64{toRemove[0].Filename})
		curRemainIdx++
	}
}

func TestLFUQueueBehavior(t *testing.T) {
	r := rand.New(rand.NewSource(28))
	man := Manager{}
	man.Init(LFUQueue)

	files := make([]int64, numFiles)
	for idx := 0; idx < numFiles; idx++ {
		files[idx] = 100000 + int64(idx)
	}

	for _, filename := range files {
		man.Insert(
			&FileSupportData{
				Filename:  filename,
				Frequency: r.Int63n(numFiles),
				Recency:   -1,
				Size:      -1.,
			},
		)
	}

	toRemove := make([]int64, len(files)/2)
	toRemove = toRemove[:0]

	// fmt.Println("INSERT")
	for _, curFile := range man.GetFromWorst() {
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

	man.Remove(toRemove)

	toUpdate := make([]int64, man.Len())
	toUpdate = toUpdate[:0]

	// fmt.Println("REMOVE ->", toRemove)
	for _, curFile := range man.GetFromWorst() {
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
			oldValues[filename] = man.GetFileSupportData(filename).Frequency
			man.Update(
				&FileSupportData{
					Filename:  filename,
					Frequency: r.Int63n(numFiles) + numFiles*int64(numUpdate),
					Recency:   -1,
					Size:      -1.,
				},
			)
		}

		var prevFrequency int64 = -1
		for _, curFile := range man.GetFromWorst() {
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
}

func TestSizeSmallQueueBehavior(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	man := Manager{}
	man.Init(SizeSmallQueue)

	files := make([]int64, numFiles)
	for idx := 0; idx < numFiles; idx++ {
		files[idx] = 100000 + int64(idx)
	}

	for _, filename := range files {
		man.Insert(
			&FileSupportData{
				Filename:  filename,
				Frequency: -1,
				Recency:   r.Int63n(numFiles),
				Size:      r.Float64() * 1024.,
			},
		)
	}

	toRemove := make([]int64, len(files)/2)
	toRemove = toRemove[:0]

	// fmt.Println("INSERT")
	for _, curFile := range man.GetFromWorst() {
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

	man.Remove(toRemove)

	toUpdate := make([]int64, man.Len())
	toUpdate = toUpdate[:0]

	// fmt.Println("REMOVE ->", toRemove)
	for _, curFile := range man.GetFromWorst() {
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
			oldValues[filename] = man.GetFileSupportData(filename).Size
			man.Update(
				&FileSupportData{
					Filename:  filename,
					Frequency: -1,
					Recency:   r.Int63n(numFiles) + numFiles*int64(numUpdate),
					Size:      oldValues[filename] + r.Float64()*1024. + 1.0,
				},
			)
		}

		var prevSize float64 = -1.
		for _, curFile := range man.GetFromWorst() {
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

func TestSizeBigQueueBehavior(t *testing.T) {
	r := rand.New(rand.NewSource(73))
	man := Manager{}
	man.Init(SizeBigQueue)

	files := make([]int64, numFiles)
	for idx := 0; idx < numFiles; idx++ {
		files[idx] = 100000 + int64(idx)
	}

	for _, filename := range files {
		man.Insert(
			&FileSupportData{
				Filename:  filename,
				Frequency: -1,
				Recency:   r.Int63n(numFiles),
				Size:      r.Float64() * 1024.,
			},
		)
	}

	toRemove := make([]int64, len(files)/2)
	toRemove = toRemove[:0]

	// fmt.Println("INSERT")
	for _, curFile := range man.GetFromWorst() {
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

	man.Remove(toRemove)

	toUpdate := make([]int64, man.Len())
	toUpdate = toUpdate[:0]

	// fmt.Println("REMOVE ->", toRemove)
	for _, curFile := range man.GetFromWorst() {
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
			oldValues[filename] = man.GetFileSupportData(filename).Size
			man.Update(
				&FileSupportData{
					Filename:  filename,
					Frequency: -1,
					Recency:   r.Int63n(numFiles) + numFiles*int64(numUpdate),
					Size:      oldValues[filename] + r.Float64()*1024. + 1.0,
				},
			)
		}

		var prevSize float64 = -1.
		for _, curFile := range man.GetFromWorst() {
			// fmt.Println(curFile.Filename, "->", curFile.Size)
			_, inToUpdate := oldValues[curFile.Filename]
			if inToUpdate && curFile.Size <= oldValues[curFile.Filename] {
				t.Log("ERROR: File '", curFile.Filename, "' not updated...")
				t.Fatal()
			}
			if prevSize != -1 && prevSize < curFile.Size {
				t.Log("SizeBig order not valid")
				t.Fatal()
			}
			prevSize = curFile.Size
		}
	}
}
