package cache

import (
	"math/rand"
	"testing"
	// "fmt"
)

const (
	WeightedCacheEXP float32 = 2.0
)

func TestWeightedCacheBaseMultipleInsert(t *testing.T) {
	testCache := WeightedCache{
		MaxSize:         3.0,
		SelFunctionType: FuncWeightedRequests,
		Exp:             WeightedCacheEXP,
	}
	testCache.Init()

	res := testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)

	if !res {
		t.Fatalf("First insert error -> Expected %t but got %t", true, res)
	} else if testCache.HitRate() != 75. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 75., testCache.HitRate())
	} else if testCache.WeightedHitRate() != 225. {
		t.Fatalf("Weighted hit rate error -> Expected %f but got %f", 225., testCache.WeightedHitRate())
	} else if testCache.Size() != 1.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 1.0, testCache.Size())
	} else if testCache.DataWritten() != 1.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 1.0, testCache.DataWritten())
	}
}

func TestWeightedCacheClear(t *testing.T) {
	testCache := WeightedCache{
		MaxSize:         3.0,
		SelFunctionType: FuncWeightedRequests,
		Exp:             WeightedCacheEXP,
	}
	testCache.Init()

	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)

	testCache.Clear()

	if testCache.HitRate() != 0. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 0., testCache.HitRate())
	} else if testCache.Size() != 0. {
		t.Fatalf("Size error -> Expected %f but got %f", 0., testCache.Size())
	} else if testCache.DataWritten() != 0. {
		t.Fatalf("Written data error -> Expected %f but got %f", 0., testCache.DataWritten())
	} else if testCache.DataReadOnHit() != 0. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 0., testCache.DataReadOnHit())
	} else if len(testCache.queue) != 0 {
		t.Fatalf("Queue error -> Expected %d but got %d", 0, len(testCache.queue))
	} else if len(testCache.files) != 0 {
		t.Fatalf("Cache error -> Expected %d but got %d", 0, len(testCache.files))
	}
}

func TestWeightedCacheInsert(t *testing.T) {
	testCache := WeightedCache{
		MaxSize:         3.0,
		SelFunctionType: FuncWeightedRequests,
		Exp:             WeightedCacheEXP,
	}
	testCache.Init()

	testCache.Get("/a/b/c/d/file0", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file1", 2.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file2", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file3", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file1", 2.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file1", 2.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file1", 2.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file4", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file3", 1.0, 0.0, 0.0)
	testCache.Get("/a/b/c/d/file4", 1.0, 0.0, 0.0)

	if testCache.HitRate() != 40. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 40., testCache.HitRate())
	} else if testCache.Size() != 3. {
		t.Fatalf("Size error -> Expected %f but got %f", 3., testCache.Size())
	} else if testCache.DataWritten() != 5. {
		t.Fatalf("Written data error -> Expected %f but got %f", 5., testCache.DataWritten())
	} else if testCache.DataReadOnHit() != 7. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 7., testCache.DataReadOnHit())
	}
}

func BenchmarkWeightedCache(b *testing.B) {
	var maxSize float32 = 1024. * 1024. * 10.
	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	genRandomFilePath := func(num int32) string {
		filepath := make([]rune, num)
		for idx := range filepath {
			filepath[idx] = LetterRunes[rand.Intn(len(LetterRunes))]
		}
		return string(filepath)
	}

	testCache := WeightedCache{
		MaxSize: maxSize,
	}
	testCache.Init(FuncWeightedRequests, WeightedCacheEXP)

	for n := 0; n < b.N; n++ {
		testCache.Get(genRandomFilePath(5), rand.Float32()*maxSize, 0.0, 0.0)
	}
}
