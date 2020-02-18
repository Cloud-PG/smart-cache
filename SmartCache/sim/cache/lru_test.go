package cache

import (
	_ "fmt"
	"math/rand"
	"testing"
	"time"
)

const (
	floatZero float32 = 0.0
	size1     float32 = 1.0
	size2     float32 = 2.0
)

func TestLRUCacheBaseMultipleInsert(t *testing.T) {
	testCache := &LRUCache{
		MaxSize: 3.0,
	}
	testCache.Init()

	res := GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())

	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())

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

func TestLRUCacheClear(t *testing.T) {
	testCache := &LRUCache{
		MaxSize: 3.0,
	}
	testCache.Init()

	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())

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

func TestLRUCacheInsert(t *testing.T) {
	testCache := &LRUCache{
		MaxSize: 5.0,
	}
	testCache.Init()

	GetFile(testCache, "/a/b/c/d/file0", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file2", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file3", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file1", size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file4", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file3", size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, "/a/b/c/d/file4", size1, floatZero, floatZero, time.Now().Unix())

	// for tmpVal := testCache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
	// 	println(tmpVal.Value.(string))
	// }
	// println()

	if testCache.HitRate() != 50. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 50., testCache.HitRate())
	} else if testCache.Size() != 4.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 4.0, testCache.Size())
	} else if testCache.DataWritten() != 6.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 6.0, testCache.DataWritten())
	} else if testCache.DataReadOnHit() != 8. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 8., testCache.DataReadOnHit())
	} else if testCache.queue.Front().Value.(string) != "/a/b/c/d/file1" {
		t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file1", testCache.queue.Front().Value.(string))
	} else if testCache.queue.Back().Value.(string) != "/a/b/c/d/file4" {
		t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file4", testCache.queue.Back().Value.(string))
	}
}

func BenchmarkLRUCache(b *testing.B) {
	var maxSize float64 = 1024. * 1024. * 10.
	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	genRandomFilePath := func(num int32) string {
		filepath := make([]rune, num)
		for idx := range filepath {
			filepath[idx] = LetterRunes[rand.Intn(len(LetterRunes))]
		}
		return string(filepath)
	}

	testCache := &LRUCache{
		MaxSize: maxSize,
	}
	testCache.Init()

	for n := 0; n < b.N; n++ {
		GetFile(testCache, genRandomFilePath(5), rand.Float64()*maxSize, 0.0, 0.0, time.Now().Unix())
	}
}
