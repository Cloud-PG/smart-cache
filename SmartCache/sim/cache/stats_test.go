package cache

import (
	"fmt"
	_ "fmt"
	"testing"
	"time"
)

func TestCreateStats(t *testing.T) {
	stats := Stats{}
	stats.Init()

	fileStats, newFile := stats.GetOrCreate("fileA", float32(32.0))
	if newFile == false {
		t.Fatalf("File have not to be already present in stats")
	} else if fileStats.Size != 32.0 {
		t.Fatalf("Size have to be %f and not %f", 32.0, fileStats.Size)
	}

	fileStatsAfter, newFileAfter := stats.GetOrCreate("fileA", float32(42.0))
	if newFileAfter == true {
		t.Fatalf("File have to be in the stats!")
	} else if fileStats.Size != 42.0 {
		t.Fatalf("Size have to be %f and not %f", 42.0, fileStats.Size)
	} else if fileStats != fileStatsAfter {
		t.Fatalf("Bad struct returned")
	}

	curTime := time.Now()
	fileStats.updateStats(true, float32(42.0), 555, "siteA", &curTime)
	fileStats.updateStats(true, float32(42.0), 555, "siteB", &curTime)
	fileStats.updateStats(true, float32(42.0), 111, "siteC", &curTime)
	fileStats.updateStats(false, float32(42.0), 111, "siteA", &curTime)
	fileStats.updateStats(false, float32(42.0), 111, "siteB", &curTime)

	numReqs, numUsers, numSites := fileStats.getStats()
	if numReqs != 5. {
		t.Fatalf("Num. requests have to be %f and not %f", 5., numReqs)
	} else if numUsers != 2. {
		t.Fatalf("Num. users have to be %f and not %f", 2., numUsers)
	} else if numSites != 3. {
		t.Fatalf("Num. sites have to be %f and not %f", 3., numSites)
	}

	fmt.Println(fileStats.Points)
	fileStats.updateFilePoints(&curTime)
	fmt.Println(fileStats.Points)
}
