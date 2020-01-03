package cache

import (
	_ "fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

const (
	featureMapStringRL = `{
		"size": {
		  "feature": "size",
		  "type": "int",
		  "keys": [
			10,
			100,
			250,
			500,
			1000,
			2000,
			4000
		  ],
		  "values": {
			"10": 0,
			"100": 1,
			"250": 2,
			"500": 3,
			"1000": 4,
			"2000": 5,
			"4000": 6,
			"max": 7
		  },
		  "unknown_values": false,
		  "buckets": true,
		  "bucket_open_right": true
		},
		"numReq": {
		  "feature": "numReq",
		  "type": "int",
		  "keys": [
			1,
			2,
			4,
			8,
			16,
			32
		  ],
		  "values": {
			"1": 0,
			"2": 1,
			"4": 2,
			"8": 3,
			"16": 4,
			"32": 5,
			"max": 6
		  },
		  "unknown_values": false,
		  "buckets": true,
		  "bucket_open_right": true
		},
		"cacheUsage": {
		  "feature": "cacheUsage",
		  "type": "float",
		  "keys": [
			10.0,
			20.0,
			30.0,
			40.0,
			50.0,
			60.0,
			70.0,
			80.0,
			90.0
		  ],
		  "values": {
			"10.0": 0,
			"20.0": 1,
			"30.0": 2,
			"40.0": 3,
			"50.0": 4,
			"60.0": 5,
			"70.0": 6,
			"80.0": 7,
			"90.0": 8,
			"max": 9
		  },
		  "unknown_values": false,
		  "buckets": true,
		  "bucket_open_right": true
		},
		"siteType": {
		  "feature": "siteType",
		  "type": "string",
		  "keys": [
			"T1",
			"T2",
			"T3"
		  ],
		  "values": {
			"T1": 1,
			"T2": 2,
			"T3": 3
		  },
		  "unknown_values": true,
		  "buckets": false,
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

func TestAIRLInit(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "featureMap.*.json")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(featureMapStringRL)

	ai := AIRL{
		LRUCache: LRUCache{
			MaxSize: 5.0,
		},
	}
	ai.Init(tmpFile.Name())

}
