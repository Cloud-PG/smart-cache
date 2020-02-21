package cache

import (
	"fmt"
	_ "fmt"
	"testing"
	"time"
)

const (
	floatZero float64 = 0.0
	size1     float64 = 1.0
	size2     float64 = 2.0
)

func TestLRUCacheBaseMultipleInsert(t *testing.T) {
	testCache := &LRUCache{
		MaxSize:       3.0,
		HighWaterMark: 100.,
		LowWaterMark:  100.,
	}
	testCache.Init()

	res := GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())

	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())

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
		MaxSize:       3.0,
		HighWaterMark: 100.,
		LowWaterMark:  100.,
	}
	testCache.Init()

	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())

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

func TestLRUCacheInsert(t *testing.T) {
	testCache := &LRUCache{
		MaxSize:       5.0,
		HighWaterMark: 100.,
		LowWaterMark:  100.,
	}
	testCache.Init()

	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 0", testCache.queue)
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 1", testCache.queue)
	GetFile(testCache, int64(2), size1, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 2", testCache.queue)
	GetFile(testCache, int64(3), size1, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 3", testCache.queue)
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 4", testCache.queue)
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 5", testCache.queue)
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 6", testCache.queue)
	GetFile(testCache, int64(4), size1, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 7", testCache.queue)
	GetFile(testCache, int64(3), size1, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 8", testCache.queue)
	GetFile(testCache, int64(4), size1, floatZero, floatZero, time.Now().Unix())
	fmt.Println("step 10", testCache.queue)

	// for tmpVal := testCache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
	// 	println(tmpVal.Value.(string))
	// }
	// println()

	if testCache.HitRate() != 50. {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 50., testCache.HitRate())
	} else if testCache.Size() != 5.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 5.0, testCache.Size())
	} else if testCache.DataWritten() != 6.0 {
		t.Fatalf("Written data error -> Expected %f but got %f", 6.0, testCache.DataWritten())
	} else if testCache.DataReadOnHit() != 8. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 8., testCache.DataReadOnHit())
	} else if testCache.queue[len(testCache.queue)-1] != 4 {
		t.Fatalf("Element error -> Expected %d but got %d", 4, testCache.queue[len(testCache.queue)-1])
	} else if testCache.queue[0] != 2 {
		t.Fatalf("Element error -> Expected %d but got %d", 2, testCache.queue[0])
	}
}

// func BenchmarkLRUCache(b *testing.B) {
// 	var maxSize float64 = 1024. * 1024. * 10.
// 	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// 	genRandomFilePath := func(num int32) string {
// 		filepath := make([]rune, num)
// 		for idx := range filepath {
// 			filepath[idx] = LetterRunes[rand.Intn(len(LetterRunes))]
// 		}
// 		return string(filepath)
// 	}

// 	testCache := &LRUCache{
// 		MaxSize: maxSize,
// 	}
// 	testCache.Init()

// 	for n := 0; n < b.N; n++ {
// 		GetFile(testCache, genRandomFilePath(5), rand.Float64()*maxSize, 0.0, 0.0, time.Now().Unix())
// 	}
// }
