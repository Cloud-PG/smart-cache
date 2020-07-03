package cache

import (
	_ "fmt"
	"testing"
	"time"
)

func TestCreateStats(t *testing.T) {
	stats := Stats{}
	stats.Init()

	curTime := time.Now()
	fileStats, newFile := stats.GetOrCreate(0, float64(32.0), curTime)
	if newFile == false {
		t.Fatalf("File have not to be already present in stats")
	} else if fileStats.Size != 32.0 {
		t.Fatalf("Size have to be %f and not %f", 32.0, fileStats.Size)
	}

	fileStatsAfter, newFileAfter := stats.GetOrCreate(0, float64(42.0))
	if newFileAfter == true {
		t.Fatalf("File have to be in the stats!")
	} else if fileStats.Size != 42.0 {
		t.Fatalf("Size have to be %f and not %f", 42.0, fileStats.Size)
	} else if fileStats != fileStatsAfter {
		t.Fatalf("Bad struct returned")
	}

	curTime = time.Now()
	fileStats.addInCache(0, &curTime)
	fileStats.updateStats(true, float64(42.0), int64(555), int64(0), curTime)
	fileStats.updateStats(true, float64(42.0), int64(555), int64(1), curTime)
	fileStats.updateStats(true, float64(42.0), int64(111), int64(2), curTime)
	fileStats.updateStats(false, float64(42.0), int64(111), int64(0), curTime)
	fileStats.updateStats(false, float64(42.0), int64(111), int64(1), curTime)
}
