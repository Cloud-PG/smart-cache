package cache

import (
	"math/rand"
	"testing"
	// "fmt"
)

func TestLRUCacheBaseMultipleInsert(t *testing.T) {
	testCache := LRU{
		MaxSize: 3.0,
	}
	testCache.Init()

	res := testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file0", 1.0)

	if !res {
		t.Fatalf("First insert error -> Expected %t but got %t", true, res)
	} else if testCache.HitRate() != 75. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 75., testCache.HitRate())
	} else if testCache.Size() != 1.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 1.0, testCache.Size())
	} else if testCache.WrittenData() != 1.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 1.0, testCache.WrittenData())
	}
}

func TestLRUCacheClear(t *testing.T) {
	testCache := LRU{
		MaxSize: 3.0,
	}
	testCache.Init()

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
		t.Fatalf("Read on hit error -> Expected %f but got %f", 3., testCache.ReadOnHit())
	} else if testCache.queue.Len() != 0 {
		t.Fatalf("Queue error -> Expected %d but got %d", 0, testCache.queue.Len())
	} else if len(testCache.files) != 0 {
		t.Fatalf("Cache error -> Expected %d but got %d", 0, len(testCache.files))
	}
}

func TestLRUCacheInsert(t *testing.T) {
	testCache := LRU{
		MaxSize: 3.0,
	}
	testCache.Init()

	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file1", 1.0)
	testCache.Get("/a/b/c/d/file2", 1.0)
	testCache.Get("/a/b/c/d/file3", 1.0)
	testCache.Get("/a/b/c/d/file1", 1.0)
	testCache.Get("/a/b/c/d/file4", 1.0)
	testCache.Get("/a/b/c/d/file3", 1.0)
	testCache.Get("/a/b/c/d/file4", 1.0)

	// for tmpVal := testCache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
	// 	println(tmpVal.Value.(string))
	// }
	// println()

	if testCache.HitRate() != 37.5 {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 37.5, testCache.HitRate())
	} else if testCache.Size() != 3.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 3.0, testCache.Size())
	} else if testCache.WrittenData() != 5.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 5.0, testCache.WrittenData())
	} else if testCache.ReadOnHit() != 3. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 3., testCache.ReadOnHit())
	} else if testCache.queue.Front().Value.(string) != "/a/b/c/d/file1" {
		t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file1", testCache.queue.Front().Value.(string))
	} else if testCache.queue.Back().Value.(string) != "/a/b/c/d/file4" {
		t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file4", testCache.queue.Back().Value.(string))
	}
}

func BenchmarkLRUCache(b *testing.B) {
	var maxSize float32 = 1024. * 1024. * 10.
	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	genRandomFilePath := func(num int32) string {
		filepath := make([]rune, num)
		for idx := range filepath {
			filepath[idx] = LetterRunes[rand.Intn(len(LetterRunes))]
		}
		return string(filepath)
	}

	testCache := LRU{
		MaxSize: maxSize,
	}
	testCache.Init()

	for n := 0; n < b.N; n++ {
		testCache.Get(genRandomFilePath(5), rand.Float32()*maxSize)
	}
}
