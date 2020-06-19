package featuremap

import (
	_ "fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

const (
	featureMapString = `{
		"size": {
		  "buckets": [
			2000.0,
			2500.0,
			3000.0,
			3500.0,
			4000.0
		  ],
		  "openRight": true
		},
		"numReq": {
		  "buckets": [
			1.0,
			4.0
		  ],
		  "openRight2": true
		},
		"deltaLastRequest": {
		  "buckets": [
			10000,
			100000,
			200000
		  ],
		  "openRight": true
		}
	  }
	  
	  `
)

func TestFeatureMapParse(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "featureMap.*.json")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(featureMapString)

	Parse(tmpFile.Name())

	// for entryName, entryLen := range allKeys {
	// 	if _, inMap := entries[entryName]; !inMap {
	// 		t.Fatalf("Expected key '%s' in map but is not there...", entryName)
	// 	}
	// 	// if entryLen != entries[entryName].GetLenKeys() {
	// 	// 	t.Fatalf("Expected key with len '%d' but got len '%d'", entryLen, entries[entryName].GetLenKeys())
	// 	// }
	// 	// for entryKey := range entries[entryName].GetKeys() {
	// 	// 	if !(reflect.TypeOf(entryKey).String() == "featuremap.Key") {
	// 	// 		t.Fatalf("Expected type '%s' but got type '%s'", "featuremap.Key", reflect.TypeOf(entryKey))
	// 	// 	}
	// 	// }
	// }
}
