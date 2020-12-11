package cache

import (
	_ "fmt"
	"testing"
	"time"

	"simulator/v2/cache/queue"
)

const (
	floatZero float64 = 0.0
	size1     float64 = 1.0
	size2     float64 = 2.0
)

func TestSimpleCacheBaseMultipleInsert(t *testing.T) {
	testCache := &SimpleCache{
		MaxSize: 3.0,
	}
	testCache.Init(InitParameters{QueueType: queue.LRUQueue, HighWatermark: 100., LowWatermark: 100.})

	res, _ := GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())

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

func TestSimpleCacheClear(t *testing.T) {
	testCache := &SimpleCache{
		MaxSize: 3.0,
	}
	testCache.Init(InitParameters{QueueType: queue.LRUQueue, HighWatermark: 100., LowWatermark: 100.})

	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())

	testCache.Clear()

	// if testCache.HitRate() != 0. {
	// 	t.Fatalf("Hit rate error -> Expected %f but got %f", 0., testCache.HitRate())
	// } else if testCache.Size() != 0. {
	// 	t.Fatalf("Size error -> Expected %f but got %f", 0., testCache.Size())
	// } else if testCache.DataWritten() != 0. {
	// 	t.Fatalf("Written data error -> Expected %f but got %f", 0., testCache.DataWritten())
	// } else if testCache.DataReadOnHit() != 0. {
	// 	t.Fatalf("Read on hit error -> Expected %f but got %f", 0., testCache.DataReadOnHit())
	// } else if testCache.files.Len() != 0 {
	// 	t.Fatalf("Queue error -> Expected %d but got %d", 0, testCache.files.Len())
	// } else if testCache.files.Len() != 0 {
	// 	t.Fatalf("Cache error -> Expected %d but got %d", 0, testCache.files.Len())
	// }
}

func TestSimpleCacheInsert(t *testing.T) {
	testCache := &SimpleCache{
		MaxSize: 5.0,
	}
	testCache.Init(InitParameters{QueueType: queue.LRUQueue, HighWatermark: 100., LowWatermark: 100.})

	GetFile(testCache, int64(0), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(2), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(3), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(1), size2, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(4), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(3), size1, floatZero, floatZero, time.Now().Unix())
	GetFile(testCache, int64(4), size1, floatZero, floatZero, time.Now().Unix())

	// for tmpVal := testCache.policyLRU.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
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
	}
}

// func BenchmarkSimpleCache(b *testing.B) {
// 	var maxSize float64 = 1024. * 1024. * 10.
// 	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// 	genRandomFilePath := func(num int32) string {
// 		filepath := make([]rune, num)
// 		for idx := range filepath {
// 			filepath[idx] = LetterRunes[rand.Intn(len(LetterRunes))]
// 		}
// 		return string(filepath)
// 	}

// 	testCache := &SimpleCache{
// 		MaxSize: maxSize,
// 	}
// 	testCache.Init()

// 	for n := 0; n < b.N; n++ {
// 		GetFile(testCache, genRandomFilePath(5), rand.Float64()*maxSize, 0.0, 0.0, time.Now().Unix())
// 	}
// }
