package featuremap

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

type mapType int

const (
	// TypeInt is the type int for the feature map
	TypeInt mapType = iota
	// TypeFloat is the type float for the feature map
	TypeFloat
	// TypeString is the type string for the feature map
	TypeString
	// TypeBool is the type bool for the feature map
	TypeBool
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
}

// Key is a key of the map
type Key struct {
	ValueI int64
	ValueF float64
	ValueS string
}

// Entry is a parsed entry of the feature map
type Entry struct {
	Key   string
	Value Obj
}

// Parse returns the entries of a feature map
func Parse(featureMapFilePath string) chan Entry {
	var tmpMap interface{}
	fileExtension := filepath.Ext(featureMapFilePath)

	featureMapFile, errOpenFile := os.Open(featureMapFilePath)
	if errOpenFile != nil {
		log.Fatalf("Cannot open file '%s'\n", errOpenFile)
	}

	if fileExtension == ".gzip" || fileExtension == ".gz" {
		featureMapFileGz, errOpenZipFile := gzip.NewReader(featureMapFile)
		if errOpenZipFile != nil {
			log.Fatalf("Cannot open zip stream from file '%s'\nError: %s\n", featureMapFilePath, errOpenZipFile)
		}

		errJSONUnmarshal := json.NewDecoder(featureMapFileGz).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			log.Fatalf("Cannot unmarshal gzipped json from file '%s'\nError: %s\n", featureMapFilePath, errJSONUnmarshal)
		}
	} else if fileExtension == ".json" {
		errJSONUnmarshal := json.NewDecoder(featureMapFile).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			log.Fatalf("Cannot unmarshal plain json from file '%s'\nError: %s\n", featureMapFilePath, errJSONUnmarshal)
		}
	} else {
		log.Fatalf("Cannot unmarshal file '%s' with extension '%s'", featureMapFilePath, fileExtension)
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
	}
	return lenght
}

// GetKeys returns all the keys
func (curMap Obj) GetKeys() chan Key {
	channel := make(chan Key)
	go func() {
		defer close(channel)
		numKeys := curMap.GetLenKeys()
		for idx := 0; idx < numKeys; idx++ {
			curKey := Key{}
			switch curMap.Type {
			case TypeInt:
				curKey.ValueI = curMap.KeysI[idx]
			case TypeFloat:
				curKey.ValueF = curMap.KeysF[idx]
			case TypeString:
				curKey.ValueS = curMap.KeysS[idx]
			}
			channel <- curKey
		}
	}()
	return channel
}
