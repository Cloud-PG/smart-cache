package cache

import (
	"testing"
	"math/rand"
	// "fmt"
)

func TestLRUCacheBaseMultipleInsert(t *testing.T) {
	testCache := LRU{
		MaxSize: 3.0,
	}
	testCache.Init()

	res := testCache.Update("/a/b/c/d/file0", 1.0)
	testCache.Update("/a/b/c/d/file0", 1.0)
	testCache.Update("/a/b/c/d/file0", 1.0)
	testCache.Update("/a/b/c/d/file0", 1.0)

	if !res{
		t.Fatalf("First insert error -> Expected %t but got %t", true, res)
	} else if testCache.HitRate() != 0.75 {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 0.75, testCache.HitRate())
	} else if testCache.Size() != 1.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 1.0, testCache.Size())
	} else if testCache.WrittenData() != 1.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 1.0, testCache.WrittenData())
	}
}

func TestLRUCacheInsert(t *testing.T) {
	testCache := LRU{
		MaxSize: 3.0,
	}
	testCache.Init()

	testCache.Update("/a/b/c/d/file0", 1.0)
	testCache.Update("/a/b/c/d/file1", 1.0)
	testCache.Update("/a/b/c/d/file2", 1.0)
	testCache.Update("/a/b/c/d/file3", 1.0)
	testCache.Update("/a/b/c/d/file1", 1.0)
	testCache.Update("/a/b/c/d/file4", 1.0)
	testCache.Update("/a/b/c/d/file3", 1.0)
	testCache.Update("/a/b/c/d/file4", 1.0)

	// for tmpVal := testCache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
	// 	println(tmpVal.Value.(string))
	// }
	// println()

	if testCache.HitRate() != 0.375 {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 0.375, testCache.HitRate())
	} else if testCache.Size() != 3.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 3.0, testCache.Size())
	} else if testCache.WrittenData() != 5.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 5.0, testCache.WrittenData())
	} else if testCache.queue.Front().Value.(string) != "/a/b/c/d/file1" {
		t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file1", testCache.queue.Front().Value.(string))
	} else if testCache.queue.Back().Value.(string) != "/a/b/c/d/file4" {
		t.Fatalf("Written data error -> Expected %s but got %s", "/a/b/c/d/file4", testCache.queue.Back().Value.(string))
	}
}



func BenchmarkLRUCache(b *testing.B) {
	var maxSize float32 = 1024. * 1024. * 10. 
	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	genRandomFilePath := func (num int32) string {
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
		testCache.Update(genRandomFilePath(5), rand.Float32() * maxSize)
	}
}