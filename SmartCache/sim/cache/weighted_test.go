package cache

import (
	"math/rand"
	"testing"
	// "fmt"
)

func TestWeightedCacheBaseMultipleInsert(t *testing.T) {
	testCache := Weighted{
		MaxSize: 3.0,
	}
	testCache.Init(FuncFileGroupWeight)

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

func TestWeightedCacheClear(t *testing.T) {
	testCache := Weighted{
		MaxSize: 3.0,
	}
	testCache.Init(FuncFileGroupWeight)

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
	} else if len(testCache.queue) != 0 {
		t.Fatalf("Queue error -> Expected %d but got %d", 0, len(testCache.queue))
	} else if len(testCache.files) != 0 {
		t.Fatalf("Cache error -> Expected %d but got %d", 0, len(testCache.files))
	}
}

func TestWeightedCacheInsert(t *testing.T) {
	testCache := Weighted{
		MaxSize: 3.0,
	}
	testCache.Init(FuncFileGroupWeight)

	testCache.Get("/a/b/c/d/file0", 1.0)
	testCache.Get("/a/b/c/d/file1", 2.0)
	testCache.Get("/a/b/c/d/file2", 1.0)
	testCache.Get("/a/b/c/d/file3", 1.0)
	testCache.Get("/a/b/c/d/file1", 2.0)
	testCache.Get("/a/b/c/d/file4", 1.0)
	testCache.Get("/a/b/c/d/file3", 1.0)
	testCache.Get("/a/b/c/d/file4", 1.0)

	// for tmpVal := testCache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
	// 	println(tmpVal.Value.(string))
	// }
	// println()

	if testCache.HitRate() != 12.5 {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 12.5, testCache.HitRate())
	} else if testCache.Size() != 3.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 3.0, testCache.Size())
	} else if testCache.WrittenData() != 6.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 6.0, testCache.WrittenData())
	}
	// else if testCache.queue.Front().Value.(string) != "/a/b/c/d/file1" {
	// 	t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file1", testCache.queue.Front().Value.(string))
	// } else if testCache.queue.Back().Value.(string) != "/a/b/c/d/file4" {
	// 	t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file4", testCache.queue.Back().Value.(string))
	// }
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

	testCache := Weighted{
		MaxSize: maxSize,
	}
	testCache.Init(FuncFileGroupWeight)

	for n := 0; n < b.N; n++ {
		testCache.Get(genRandomFilePath(5), rand.Float32()*maxSize)
	}
}
