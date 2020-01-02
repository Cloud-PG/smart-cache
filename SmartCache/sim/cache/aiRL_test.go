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
			5,
			10,
			50,
			100,
			250,
			500,
			1000,
			2000,
			4000,
			10000
		  ],
		  "values": {
			"5": 0,
			"10": 1,
			"50": 2,
			"100": 3,
			"250": 4,
			"500": 5,
			"1000": 6,
			"2000": 7,
			"4000": 8,
			"10000": 9,
			"max": 10
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
			3,
			4,
			5,
			10,
			25,
			50,
			75,
			100,
			200
		  ],
		  "values": {
			"1": 0,
			"2": 1,
			"3": 2,
			"4": 3,
			"5": 4,
			"10": 5,
			"25": 6,
			"50": 7,
			"75": 8,
			"100": 9,
			"200": 10,
			"max": 11
		  },
		  "unknown_values": false,
		  "buckets": true,
		  "bucket_open_right": true
		},
		"siteName": {
		  "feature": "siteName",
		  "type": "string",
		  "keys": [
			"T3_IT_Trieste",
			"T2_IT_Bari",
			"T3_IT_Perugia",
			"T1_IT_CNAF",
			"T2_IT_Pisa",
			"T2_IT_Legnaro",
			"T2_IT_Rome"
		  ],
		  "values": {
			"T3_IT_Trieste": 1,
			"T2_IT_Bari": 2,
			"T3_IT_Perugia": 3,
			"T1_IT_CNAF": 4,
			"T2_IT_Pisa": 5,
			"T2_IT_Legnaro": 6,
			"T2_IT_Rome": 7
		  },
		  "unknown_values": true,
		  "buckets": false,
		  "bucket_open_right": false
		},
		"fileType": {
		  "feature": "fileType",
		  "type": "string",
		  "keys": [
			"ALCARECO",
			"MINIAODSIM",
			"GEN-SIM-RECO",
			"AOD",
			"AODSIM",
			"MINIAOD",
			"RECO",
			"GEN-SIM",
			"RAW-RECO",
			"NANOAODSIM",
			"GEN-SIM-DIGI-RAW",
			"GEN-SIM-RAW",
			"RAW"
		  ],
		  "values": {
			"ALCARECO": 1,
			"MINIAODSIM": 2,
			"GEN-SIM-RECO": 3,
			"AOD": 4,
			"AODSIM": 5,
			"MINIAOD": 6,
			"RECO": 7,
			"GEN-SIM": 8,
			"RAW-RECO": 9,
			"NANOAODSIM": 10,
			"GEN-SIM-DIGI-RAW": 11,
			"GEN-SIM-RAW": 12,
			"RAW": 13
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
