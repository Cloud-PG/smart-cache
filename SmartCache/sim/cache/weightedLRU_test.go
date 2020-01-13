package cache

import (
	"math/rand"
	"testing"
	"time"
	// "fmt"
)

const (
	WeightedLRUEXP   float32 = 2.0
	WeightedCacheEXP float32 = 2.0
)

func TestWeightedLRUBaseMultipleInsert(t *testing.T) {
	testCache := &WeightedLRU{
		LRUCache: LRUCache{
			MaxSize: 3.0,
		},
		SelFunctionType: FuncWeightedRequests,
		Exp:             WeightedCacheEXP,
	}
	testCache.Init()

	res := GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())

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

func TestWeightedLRUClear(t *testing.T) {
	testCache := &WeightedLRU{
		LRUCache: LRUCache{
			MaxSize: 3.0,
		},
		SelFunctionType: FuncWeightedRequests,
		Exp:             WeightedCacheEXP,
	}
	testCache.Init()

	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())

	testCache.Clear()

	if testCache.HitRate() != 0. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 0., testCache.HitRate())
	} else if testCache.Size() != 0. {
		t.Fatalf("Size error -> Expected %f but got %f", 0., testCache.Size())
	} else if testCache.DataWritten() != 0. {
		t.Fatalf("Written data error -> Expected %f but got %f", 0., testCache.DataWritten())
	} else if testCache.DataReadOnHit() != 0. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 0., testCache.DataReadOnHit())
	} else if testCache.queue.Len() != 0 {
		t.Fatalf("Queue error -> Expected %d but got %d", 0, testCache.queue.Len())
	} else if len(testCache.files) != 0 {
		t.Fatalf("Cache error -> Expected %d but got %d", 0, len(testCache.files))
	}
}

func TestWeightedLRUInsert(t *testing.T) {
	testCache := &WeightedLRU{
		LRUCache: LRUCache{
			MaxSize: 5.0,
		},
		SelFunctionType: FuncWeightedRequests,
		Exp:             WeightedCacheEXP,
	}
	testCache.Init()

	GetFile(testCache, "/a/b/c/d/file0", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", 2.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file2", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file3", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", 2.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", 2.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", 2.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file4", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file3", 1.0, 0.0, 0.0, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file4", 1.0, 0.0, 0.0, time.Now().Unix())

	if testCache.HitRate() != 30.000002 {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 30.000002, testCache.HitRate())
	} else if testCache.Size() != 5.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 5.0, testCache.Size())
	} else if testCache.DataWritten() != 6. {
		t.Fatalf("Written data error -> Expected %f but got %f", 6., testCache.DataWritten())
	} else if testCache.DataReadOnHit() != 5. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 5., testCache.DataReadOnHit())
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

	testCache := &WeightedLRU{
		LRUCache: LRUCache{
			MaxSize: maxSize,
		},
	}
	testCache.Init()

	for n := 0; n < b.N; n++ {
		GetFile(testCache, genRandomFilePath(5), rand.Float32()*maxSize, 0.0, 0.0)
	}
}
