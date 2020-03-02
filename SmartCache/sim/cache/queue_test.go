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
			man.Init()

			for idx := 0; idx < numTests; idx++ {
				man.Insert(
					FileSupportData{
						Filename:  int64(idx),
						Frequency: r.Int63n(maxInt),
						Recency:   int64(numTests - idx),
						Size:      r.Float64() * maxFloat,
					},
				)
			}
			for idx := 0; idx < numTests; idx++ {
				man.Update(
					FileSupportData{
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

func TestLRUQueue(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	man := Manager{}
	man.Init()

	insertedFiles := []int64{}

	for idx := 0; idx < numTests; idx++ {
		curFile := int64(idx)
		insertedFiles = append(insertedFiles, curFile)
		man.Insert(
			FileSupportData{
				Filename:  curFile,
				Frequency: 0,
				Recency:   int64(numTests - idx),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	man.Update(FileSupportData{
		Filename:  0,
		Frequency: 0,
		Recency:   0,
		Size:      r.Float64() * maxFloat,
	})

	prevRecency := int64(numTests + 1)
	for file := range man.Get(LRUQueue) {
		// fmt.Println(file.Filename, file.Recency, prevRecency)
		if prevRecency < file.Recency {
			t.Log("LRU order not valid")
			t.Fatal()
		}
		prevRecency = file.Recency
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
			FileSupportData{
				Filename:  curFile,
				Frequency: r.Int63n(100),
				Recency:   int64(numTests - idx),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	man.Update(FileSupportData{
		Filename:  0,
		Frequency: 1000000,
		Recency:   0,
		Size:      r.Float64() * maxFloat,
	})

	prevFrequency := int64(-1)
	for file := range man.Get(LFUQueue) {
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
			FileSupportData{
				Filename:  curFile,
				Frequency: r.Int63n(100),
				Recency:   int64(numTests - idx),
				Size:      r.Float64() * maxFloat,
			},
		)
	}

	man.Update(FileSupportData{
		Filename:  0,
		Frequency: 0,
		Recency:   0,
		Size:      maxFloat + 100.,
	})

	prevSize := float64(-1.0)
	for file := range man.Get(SizeBigQueue) {
		// fmt.Println(file.Filename, file.Size, prevSize)
		if prevSize > file.Size {
			t.Log("Big size order not valid")
			t.Fatal()
		}
		prevSize = file.Size
	}
	for file := range man.Get(SizeSmallQueue) {
		// fmt.Println(file.Filename, file.Size, prevSize)
		if prevSize < file.Size {
			t.Log("Small size order not valid")
			t.Fatal()
		}
		prevSize = file.Size
	}

	man.Remove(insertedFiles)

	if man.Len() != 0 {
		t.Log("Manager didn't remove all the files")
		t.Fatal()
	}

}
