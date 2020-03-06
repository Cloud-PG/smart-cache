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
		"class": {
		  "feature": "class",
		  "type": "bool",
		  "keys": [
			false,
			true
		  ],
		  "values": {
			"False": 0,
			"True": 1
		  },
		  "unknown_values": false,
		  "buckets": false,
		  "bucket_open_right": false
		},
		"size": {
		  "feature": "size",
		  "type": "int",
		  "keys": [
			5,
			10,
			50,
			100
		  ],
		  "values": {
			"5": 0,
			"10": 1,
			"50": 2,
			"100": 3,
			"max": 10
		  },
		  "unknown_values": false,
		  "buckets": true,
		  "bucket_open_right": true
		},
		"sizeF": {
			"feature": "sizeF",
			"type": "float",
			"keys": [
			  5.0,
			  10.0,
			  50.0,
			  100.0
			],
			"values": {
			  "5.0": 0,
			  "10.0": 1,
			  "50.0": 2,
			  "100.0": 3
			},
			"unknown_values": false,
			"buckets": true,
			"bucket_open_right": false
		},
		"dataType": {
			"feature": "dataType",
			"type": "string",
			"keys": [
			  "data",
			  "mc"
			],
			"values": {
			  "data": 1,
			  "mc": 2
			},
			"unknown_values": true,
			"buckets": false,
			"bucket_open_right": false
		}
	}`
)

func TestFeatureMapParse(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "featureMap.*.json")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(featureMapString)

	allKeys := map[string]int{
		"class":    2,
		"size":     4,
		"sizeF":    4,
		"dataType": 2,
	}
	entries := Parse(tmpFile.Name())

	for entryName, entryLen := range allKeys {
		if _, inMap := entries[entryName]; !inMap {
			t.Fatalf("Expected key '%s' in map but is not there...", entryName)
		}
		if entryLen != entries[entryName].GetLenKeys() {
			t.Fatalf("Expected key with len '%d' but got len '%d'", entryLen, entries[entryName].GetLenKeys())
		}
		// for entryKey := range entries[entryName].GetKeys() {
		// 	if !(reflect.TypeOf(entryKey).String() == "featuremap.Key") {
		// 		t.Fatalf("Expected type '%s' but got type '%s'", "featuremap.Key", reflect.TypeOf(entryKey))
		// 	}
		// }
	}
}
