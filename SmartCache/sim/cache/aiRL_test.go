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
			100,
			250,
			500,
			1000,
			2000,
			4000
		  ],
		  "values": {
			"100": 0,
			"250": 1,
			"500": 2,
			"1000": 3,
			"2000": 4,
			"4000": 5,
			"max": 6
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
			16
		  ],
		  "values": {
			"1": 0,
			"2": 1,
			"4": 2,
			"8": 3,
			"16": 4,
			"max": 5
		  },
		  "unknown_values": false,
		  "buckets": true,
		  "bucket_open_right": true
		},
		"cacheUsage": {
		  "feature": "cacheUsage",
		  "type": "float",
		  "keys": [
			50.0,
			75.0,
			90.0
		  ],
		  "values": {
			"50.0": 0,
			"75.0": 1,
			"90.0": 2,
			"max": 3
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
	_, writeErr := tmpFile.WriteString(featureMapStringRL)
	if writeErr != nil {
		panic(writeErr)
	}

	ai := AIRL{
		SimpleCache: SimpleCache{
			MaxSize: 5.0,
		},
	}
	ai.Init(tmpFile.Name())

}
