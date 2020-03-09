package featuremap

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
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
	OutputValues    []string
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

			curStruct.prepareOutputs()

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

// GetLenKeys returns the length of a key
func (curMap *Obj) prepareOutputs() {
	curMap.OutputValues = make([]string, 0)
	lenKeys := len(curMap.Values)
	for idx := 0; idx < lenKeys; idx++ {
		curVector := make([]bool, lenKeys)
		curVector[idx] = true
		curMap.OutputValues = append(curMap.OutputValues, bool2string(curVector))
	}
}

// bool2string returns the string of 0s and 1s of a given bool slice
func bool2string(state []bool) string {
	var resIdx string
	for idx := 0; idx < len(state); idx++ {
		if state[idx] {
			resIdx += "1"
		} else {
			resIdx += "0"
		}
	}
	return resIdx
}

// GetValue returns the boolean vector representing the value
func (curMap *Obj) GetValue(value interface{}) string {
	var result string
	if curMap.Buckets == false {
		if curMap.UnknownValues {
			switch curMap.Type {
			case TypeBool:
				pos, inMap := curMap.Values[fmt.Sprintf("%t", value.(bool))]
				if inMap {
					result = curMap.OutputValues[pos]
				} else {
					result = curMap.OutputValues[curMap.Values["unknown"]]
				}
			case TypeInt:
				pos, inMap := curMap.Values[string(value.(int64))]
				if inMap {
					result = curMap.OutputValues[pos]
				} else {
					result = curMap.OutputValues[curMap.Values["unknown"]]
				}
			case TypeFloat:
				pos, inMap := curMap.Values[fmt.Sprintf("%0.2f", value.(float64))]
				if inMap {
					result = curMap.OutputValues[pos]
				} else {
					result = curMap.OutputValues[curMap.Values["unknown"]]
				}
			case TypeString:
				pos, inMap := curMap.Values[value.(string)]
				if inMap {
					result = curMap.OutputValues[pos]
				} else {
					result = curMap.OutputValues[curMap.Values["unknown"]]
				}
			}
		} else {
			switch curMap.Type {
			case TypeBool:
				result = curMap.OutputValues[curMap.Values[fmt.Sprintf("%t", value.(bool))]]
			case TypeInt:
				result = curMap.OutputValues[curMap.Values[string(value.(int64))]]
			case TypeFloat:
				result = curMap.OutputValues[curMap.Values[fmt.Sprintf("%0.2f", value.(float64))]]
			case TypeString:
				result = curMap.OutputValues[curMap.Values[value.(string)]]
			}
		}
	} else {
		var (
			inputValueI int64
			inputValueF float64
			inputValueS string
			resPrepared = false
		)
		switch curMap.Type {
		case TypeInt:
			inputValueI = int64(value.(float64))
			for _, curKey := range curMap.KeysI {
				if inputValueI <= curKey {
					result = curMap.OutputValues[curMap.Values[fmt.Sprintf("%d", curKey)]]
					resPrepared = true
					break
				}
			}
		case TypeFloat:
			inputValueF = value.(float64)
			for _, curKey := range curMap.KeysF {
				if inputValueF <= curKey {
					result = curMap.OutputValues[curMap.Values[fmt.Sprintf("%0.2f", curKey)]]
					resPrepared = true
					break
				}
			}
		case TypeString:
			inputValueS = value.(string)
			for _, curKey := range curMap.KeysS {
				if inputValueS <= curKey {
					result = curMap.OutputValues[curMap.Values[fmt.Sprintf("%s", curKey)]]
					resPrepared = true
					break
				}
			}
		}
		if !resPrepared {
			if curMap.BucketOpenRight == true {
				result = curMap.OutputValues[curMap.Values["max"]]
			} else {
				panic(fmt.Sprintf("Cannot convert a value '%v'", value))
			}
		}
	}
	return result
}
