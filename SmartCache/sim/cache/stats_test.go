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
	fileStats, newFile := stats.GetOrCreate(0, float32(32.0), curTime)
	if newFile == false {
		t.Fatalf("File have not to be already present in stats")
	} else if fileStats.Size != 32.0 {
		t.Fatalf("Size have to be %f and not %f", 32.0, fileStats.Size)
	}

	fileStatsAfter, newFileAfter := stats.GetOrCreate(0, float32(42.0))
	if newFileAfter == true {
		t.Fatalf("File have to be in the stats!")
	} else if fileStats.Size != 42.0 {
		t.Fatalf("Size have to be %f and not %f", 42.0, fileStats.Size)
	} else if fileStats != fileStatsAfter {
		t.Fatalf("Bad struct returned")
	}

	curTime = time.Now()
	fileStats.addInCache(&curTime)
	fileStats.updateStats(true, float32(42.0), 555, 0, curTime)
	fileStats.updateStats(true, float32(42.0), 555, 1, curTime)
	fileStats.updateStats(true, float32(42.0), 111, 2, curTime)
	fileStats.updateStats(false, float32(42.0), 111, 0, curTime)
	fileStats.updateStats(false, float32(42.0), 111, 1, curTime)

	numReqs, numUsers, numSites := fileStats.getStats()
	if numReqs != 5. {
		t.Fatalf("Num. requests have to be %f and not %f", 5., numReqs)
	} else if numUsers != 2. {
		t.Fatalf("Num. users have to be %f and not %f", 2., numUsers)
	} else if numSites != 3. {
		t.Fatalf("Num. sites have to be %f and not %f", 3., numSites)
	}

	fileStats.updateFilePoints(&curTime)

	if fileStats.Points < 3000. {
		t.Fatalf("File point have to be higher than 3000 and you have %f", fileStats.Points)
	}
}
