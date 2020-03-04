package cache

import (
	"testing"
	"time"
	// "fmt"
)

const (
	WeightedLRUEXP   float64 = 2.0
	WeightedCacheEXP float64 = 2.0
)

func TestWeightedLRUBaseMultipleInsert(t *testing.T) {
	testCache := &WeightedLRU{
		SimpleCache: SimpleCache{
			MaxSize:       3.0,
			HighWaterMark: 100.,
			LowWaterMark:  100.,
		},
		SelFunctionType: FuncWeightedRequests,
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

func TestWeightedLRUClear(t *testing.T) {
	testCache := &WeightedLRU{
		SimpleCache: SimpleCache{
			MaxSize:       3.0,
			HighWaterMark: 100.,
			LowWaterMark:  100.,
		},
		SelFunctionType: FuncWeightedRequests,
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
	} else if testCache.files.Len() != 0 {
		t.Fatalf("Queue error -> Expected %d but got %d", 0, testCache.files.Len())
	}
}

func TestWeightedLRUInsert(t *testing.T) {
	testCache := &WeightedLRU{
		SimpleCache: SimpleCache{
			MaxSize:       5.0,
			HighWaterMark: 100.,
			LowWaterMark:  100.,
		},
		SelFunctionType: FuncWeightedRequests,
	}
	testCache.Init()

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

	if testCache.HitRate() != 30.0 {
		t.Fatalf("Hit rate error -> Expected %f but got %f", 30.0, testCache.HitRate())
	} else if testCache.Size() != 5.0 {
		t.Fatalf("Size error -> Expected %f but got %f", 5.0, testCache.Size())
	} else if testCache.DataWritten() != 6. {
		t.Fatalf("Written data error -> Expected %f but got %f", 6., testCache.DataWritten())
	} else if testCache.DataReadOnHit() != 5. {
		t.Fatalf("Read on hit error -> Expected %f but got %f", 5., testCache.DataReadOnHit())
	}
}

// func BenchmarkWeightedLRU(b *testing.B) {
// 	var maxSize float64 = 1024. * 1024. * 10.
// 	var LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// 	genRandomFilePath := func(num int32) string {
// 		filepath := make([]rune, num)
// 		for idx := range filepath {
// 			filepath[idx] = LetterRunes[rand.Intn(len(LetterRunes))]
// 		}
// 		return string(filepath)
// 	}

// 	testCache := &WeightedLRU{
// 		SimpleCache: SimpleCache{
// 			MaxSize: maxSize,
// 		},
// 	}
// 	testCache.Init()

// 	for n := 0; n < b.N; n++ {
// 		GetFile(testCache, genRandomFilePath(5), rand.Float64()*maxSize, 0.0, 0.0)
// 	}
// }
