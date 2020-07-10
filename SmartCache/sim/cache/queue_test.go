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
)

func BenchmarkQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := rand.New(rand.NewSource(42))

			man := Manager{}
			man.Init(LRUQueue)

			for idx := 0; idx < numTests; idx++ {
				man.Insert(
					&FileSupportData{
						Filename:  int64(idx),
						Frequency: r.Int63n(maxInt),
						Recency:   int64(numTests - idx),
						Size:      r.Float64() * maxFloat,
					},
				)
			}
			for idx := 0; idx < numTests; idx++ {
				man.Update(
					&FileSupportData{
						Filename:  int64(numTests),
						Frequency: r.Int63n(maxInt),
						Recency:   r.Int63n(maxInt),
						Size:      r.Float64() * maxFloat,
					},
				)
			}
		}
	})
}

func TestLRUQueueBehavior(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	man := Manager{}
	man.Init(LRUQueue)

	files := []int64{1101, 1102, 1103, 1104, 1105, 1106}

	for _, filename := range files {
		man.Insert(
			&FileSupportData{
				Filename:  filename,
				Frequency: 0,
				Recency:   r.Int63n(100),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	toRemove := []int64{}

	for _, curFile := range man.Get() {
		// fmt.Println(curFile.Filename, curFile.Recency)
		inserted := false
		for _, filename := range files {
			if filename == curFile.Filename {
				inserted = true
				break
			}
		}
		if !inserted {
			t.Log("File '", curFile.Filename, "' not inserted")
			t.Fatal()
		}
		if len(toRemove) < 2 {
			toRemove = append(toRemove, curFile.Filename)
		}
	}

	man.Remove(toRemove)

	toUpdate := []int64{}
	for _, curFile := range man.Get() {
		// fmt.Println(curFile.Filename, curFile.Recency)
		inserted := false
		for _, filename := range files {
			if filename == curFile.Filename {
				inserted = true
				break
			}
		}
		if !inserted {
			t.Log("File '", curFile.Filename, "' not inserted")
			t.Fatal()
		}
		if len(toUpdate) < 2 {
			toUpdate = append(toUpdate, curFile.Filename)
		}
	}

	// fmt.Println("UPDATE")
	for _, filename := range toUpdate {
		man.Update(
			&FileSupportData{
				Filename:  filename,
				Frequency: 0,
				Recency:   r.Int63n(100),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	var prevRecency int64 = -1
	for _, curFile := range man.Get() {
		// fmt.Println(curFile.Filename, curFile.Recency)
		inserted := false
		for _, filename := range files {
			if filename == curFile.Filename {
				inserted = true
				break
			}
		}
		if !inserted {
			t.Log("File '", curFile.Filename, "' not inserted")
			t.Fatal()
		}
		if prevRecency != -1 && prevRecency > curFile.Recency {
			t.Log("LRU order not valid")
			t.Fatal()
		}
		prevRecency = curFile.Recency
	}

}

func TestLRUQueue(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	man := Manager{}
	man.Init(LRUQueue)

	insertedFiles := []int64{}

	for idx := 0; idx < numTests; idx++ {
		curFile := int64(idx)
		insertedFiles = append(insertedFiles, curFile)
		man.Insert(
			&FileSupportData{
				Filename:  curFile,
				Frequency: 0,
				Recency:   int64(numTests - idx),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	man.Update(&FileSupportData{
		Filename:  0,
		Frequency: 0,
		Recency:   0,
		Size:      r.Float64() * maxFloat,
	})

	prevRecency := int64(-1)
	for _, curFile := range man.Get() {
		// fmt.Println(curFile.Filename, curFile.Recency, prevRecency)
		if prevRecency != -1 && prevRecency > curFile.Recency {
			t.Log("LRU order not valid")
			t.Fatal()
		}
		prevRecency = curFile.Recency
	}

	man.Remove(insertedFiles)

	if man.Len() != 0 {
		t.Log("Manager didn't remove all the files")
		t.Fatal()
	}

}

func TestLFUQueue(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	man := Manager{}
	man.Init(LFUQueue)

	insertedFiles := []int64{}

	for idx := 0; idx < numTests; idx++ {
		curFile := int64(idx)
		insertedFiles = append(insertedFiles, curFile)
		man.Insert(
			&FileSupportData{
				Filename:  curFile,
				Frequency: r.Int63n(100),
				Recency:   int64(numTests - idx),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	man.Update(&FileSupportData{
		Filename:  0,
		Frequency: 1000000,
		Recency:   0,
		Size:      r.Float64() * maxFloat,
	})

	prevFrequency := int64(-1)
	for _, file := range man.Get() {
		// fmt.Println(file.Filename, file.Frequency, prevFrequency)
		if prevFrequency > file.Frequency {
			t.Log("LFU order not valid")
			t.Fatal()
		}
		prevFrequency = file.Frequency
	}

	man.Remove(insertedFiles)

	if man.Len() != 0 {
		t.Log("Manager didn't remove all the files")
		t.Fatal()
	}

}

func TestSizeQueue(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	man := Manager{}
	man.Init(SizeBigQueue)

	insertedFiles := []int64{}

	for idx := 0; idx < numTests; idx++ {
		curFile := int64(idx)
		insertedFiles = append(insertedFiles, curFile)
		man.Insert(
			&FileSupportData{
				Filename:  curFile,
				Frequency: r.Int63n(100),
				Recency:   int64(numTests - idx),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	man.Update(&FileSupportData{
		Filename:  0,
		Frequency: 0,
		Recency:   0,
		Size:      maxFloat + 100.,
	})

	prevSize := float64(-1.0)
	for _, curFile := range man.Get() {
		// fmt.Println(curFile.Filename, curFile.Size, prevSize)
		if prevSize != -1.0 && prevSize < curFile.Size {
			t.Log("LRU order not valid")
			t.Fatal()
		}
		prevSize = curFile.Size
	}

	man.Remove(insertedFiles)

	if man.Len() != 0 {
		t.Log("Manager didn't remove all the files")
		t.Fatal()
	}

}
