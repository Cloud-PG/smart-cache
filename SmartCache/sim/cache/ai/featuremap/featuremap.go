package featuremap

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

type mapType int

const (
	// TypeBool is the type bool for the feature map
	TypeBool mapType = iota
	// TypeInt is the type int for the feature map
	TypeInt
	// TypeFloat is the type float for the feature map
	TypeFloat
	// TypeString is the type string for the feature map
	TypeString
)

// Obj represents a map object
type Obj struct {
	Feature         string
	Type            mapType
	Keys            []interface{}
	KeysB           []bool
	KeysI           []int64
	KeysF           []float64
	KeysS           []string
	Values          map[string]int
	UnknownValues   bool
	Buckets         bool
	BucketOpenRight bool
	channel         chan interface{}
}

// Key is a key of the map
type Key struct {
	ValueB bool
	ValueI int64
	ValueF float64
	ValueS string
}

// Entry is a parsed entry of the feature map
type Entry struct {
	Key   string
	Value Obj
}

// Parse a feature map file and returns the map of keys and objects
func Parse(featureMapFilePath string) map[string]Obj {
	tmpMap := make(map[string]Obj, 0)

	for entry := range GetEntries(featureMapFilePath) {
		tmpMap[entry.Key] = entry.Value
	}

	return tmpMap
}

// GetEntries returns the entries of a feature map
func GetEntries(featureMapFilePath string) chan Entry {
	logger := zap.L()

	var tmpMap interface{}
	fileExtension := filepath.Ext(featureMapFilePath)

	featureMapFile, errOpenFile := os.Open(featureMapFilePath)
	if errOpenFile != nil {
		logger.Error("Cannot open file", zap.Error(errOpenFile))
		os.Exit(-1)
	}

	if fileExtension == ".gzip" || fileExtension == ".gz" {
		featureMapFileGz, errOpenZipFile := gzip.NewReader(featureMapFile)
		if errOpenZipFile != nil {
			logger.Error("Cannot open zip stream from file", zap.String("filename", featureMapFilePath), zap.Error(errOpenZipFile))
			os.Exit(-1)
		}

		errJSONUnmarshal := json.NewDecoder(featureMapFileGz).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			logger.Error("Cannot unmarshal gzipped json from file", zap.String("filename", featureMapFilePath), zap.Error(errJSONUnmarshal))
			os.Exit(-1)
		}
	} else if fileExtension == ".json" {
		errJSONUnmarshal := json.NewDecoder(featureMapFile).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			logger.Error("Cannot unmarshal plain json from file", zap.String("filename", featureMapFilePath), zap.Error(errJSONUnmarshal))
			os.Exit(-1)
		}
	} else {
		logger.Error("Cannot unmarshal", zap.String("filename", featureMapFilePath), zap.String("extension", fileExtension))
		os.Exit(-1)
	}

	channel := make(chan Entry)
	go func() {

		defer close(channel)
		defer featureMapFile.Close()

		lvl0 := tmpMap.(map[string]interface{})
		for k0, v0 := range lvl0 {
			curObj := v0.(map[string]interface{})
			curStruct := Obj{}
			for objK, objV := range curObj {
				switch objK {
				case "feature":
					curStruct.Feature = objV.(string)
				case "type":
					curType := objV.(string)
					switch curType {
					case "int":
						curStruct.Type = TypeInt
					case "float":
						curStruct.Type = TypeFloat
					case "string":
						curStruct.Type = TypeString
					case "bool":
						curStruct.Type = TypeBool
					}
				case "keys":
					curStruct.Keys = objV.([]interface{})
				case "values":
					curValues := objV.(map[string]interface{})
					curStruct.Values = make(map[string]int)
					for vK, vV := range curValues {
						curStruct.Values[vK] = int(vV.(float64))
					}
				case "unknown_values":
					curStruct.UnknownValues = objV.(bool)
				case "buckets":
					curStruct.Buckets = objV.(bool)
				case "bucket_open_right":
					curStruct.BucketOpenRight = objV.(bool)
				}
			}

			for _, elm := range curStruct.Keys {
				switch curStruct.Type {
				case TypeInt:
					curStruct.KeysI = append(curStruct.KeysI, int64(elm.(float64)))
				case TypeFloat:
					curStruct.KeysF = append(curStruct.KeysF, elm.(float64))
				case TypeString:
					curStruct.KeysS = append(curStruct.KeysS, elm.(string))
				case TypeBool:
					curStruct.KeysB = append(curStruct.KeysB, elm.(bool))
				}
			}

			// Output the structure
			// fmt.Println(curStruct)

			channel <- Entry{
				Key:   k0,
				Value: curStruct,
			}
		}
	}()

	return channel
}

// GetLenKeys returns the length of a key
func (curMap Obj) GetLenKeys() int {
	var lenght int
	switch curMap.Type {
	case TypeInt:
		lenght = len(curMap.KeysI)
	case TypeFloat:
		lenght = len(curMap.KeysF)
	case TypeString:
		lenght = len(curMap.KeysS)
	case TypeBool:
		lenght = len(curMap.KeysB)
	}
	return lenght
}

// GetKeys returns all the keys
func (curMap *Obj) GetKeys() chan interface{} {
	curMap.channel = make(chan interface{})
	go func() {
		defer close(curMap.channel)
		numKeys := curMap.GetLenKeys()
		for idx := 0; idx < numKeys; idx++ {
			switch curMap.Type {
			case TypeBool:
				curMap.channel <- curMap.KeysB[idx]
			case TypeInt:
				curMap.channel <- curMap.KeysI[idx]
			case TypeFloat:
				curMap.channel <- curMap.KeysF[idx]
			case TypeString:
				curMap.channel <- curMap.KeysS[idx]
			}
		}
	}()
	return curMap.channel
}
