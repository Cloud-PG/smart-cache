package cache

import (
	"math/rand"
	"testing"
	// "fmt"
)

const (
	EXP float32 = 2.0
)

func TestWeightedLRUBaseMultipleInsert(t *testing.T) {
	testCache := WeightedLRU{
		MaxSize: 3.0,
	}
	testCache.Init(FuncWeightedRequests, EXP)

	res := testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)

	if !res {
		t.Fatalf("First insert error -> Expected %t but got %t", true, res)
	} else if testCache.HitRate() != 75. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 75., testCache.HitRate())
	} else if testCache.WeightedHitRate() != 75. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 75., testCache.WeightedHitRate())
	} else if testCache.Size() != 1.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 1.0, testCache.Size())
	} else if testCache.WrittenData() != 1.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 1.0, testCache.WrittenData())
	}
}

func TestWeightedLRUClear(t *testing.T) {
	testCache := WeightedLRU{
		MaxSize: 3.0,
	}
	testCache.Init(FuncWeightedRequests, EXP)

	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)

	testCache.Clear()

	if testCache.HitRate() != 0. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 0., testCache.HitRate())
	} else if testCache.Size() != 0. {
		t.Fatalf("Size error -> Expected %f but got %f", 0., testCache.Size())
	} else if testCache.WrittenData() != 0. {
		t.Fatalf("Written data error -> Expected %f but got %f", 0., testCache.WrittenData())
	} else if testCache.ReadOnHit() != 3. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 0., testCache.ReadOnHit())
	} else if len(testCache.queue) != 0 {
		t.Fatalf("Queue error -> Expected %d but got %d", 0, len(testCache.queue))
	} else if len(testCache.files) != 0 {
		t.Fatalf("Cache error -> Expected %d but got %d", 0, len(testCache.files))
	}
}

func TestWeightedLRUInsert(t *testing.T) {
	testCache := WeightedLRU{
		MaxSize: 3.0,
	}
	testCache.Init(FuncWeightedRequests, EXP)

	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file1", 2.0)
	testCache.Get("/a/b/c/d/file2", 1.0)
	testCache.Get("/a/b/c/d/file3", 1.0)
	testCache.Get("/a/b/c/d/file1", 2.0)
	testCache.Get("/a/b/c/d/file4", 1.0)
	testCache.Get("/a/b/c/d/file3", 1.0)
	testCache.Get("/a/b/c/d/file4", 1.0)

	if testCache.HitRate() != 12.5 {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 12.5, testCache.HitRate())
	} else if testCache.Size() != 3.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 3.0, testCache.Size())
	} else if testCache.WrittenData() != 5.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 5.0, testCache.WrittenData())
	} else if testCache.ReadOnHit() != 2. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 2., testCache.ReadOnHit())
	}
}

func BenchmarkWeightedLRU(b *testing.B) {
	var maxSize float32 = 1024. * 1024. * 10.
	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	genRandomFilePath := func(num int32) string {
		filepath := make([]rune, num)
		for idx := range filepath {
			filepath[idx] = LetterRunes[rand.Intn(len(LetterRunes))]
		}
		return string(filepath)
	}

	testCache := WeightedLRU{
		MaxSize: maxSize,
	}
	testCache.Init(FuncWeightedRequests, EXP)

	for n := 0; n < b.N; n++ {
		testCache.Get(genRandomFilePath(5), rand.Float32()*maxSize)
	}
}
