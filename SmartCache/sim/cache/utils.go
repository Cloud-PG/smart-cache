package cache

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
)

const (
	hashSize int = 8
)

var (
	logger = zap.L()
)

// HashHexDigest convert to a string into an hash string
func HashHexDigest(input string) string {
	curHash, _ := blake2b.New(hashSize, nil)
	_, writeErr := curHash.Write([]byte(input))
	if writeErr != nil {
		panic(writeErr)
	}
	return fmt.Sprintf("%x", curHash.Sum(nil))
}

// HashInt converto a string into an integer value from hash
func HashInt(input string) uint32 {
	uinteger, _ := strconv.ParseUint(HashHexDigest(input), 16, 64)
	return uint32(uinteger % 10000000000)
}

// GetSimulationRunNum returns the last number of run for the simulation
func GetSimulationRunNum(folder string) int {
	files, err := ioutil.ReadDir(folder)
	if err != nil {
		log.Fatal(err)
	}

	run := 0
	for _, f := range files {
		if strings.Contains(f.Name(), "_run-") {
			run++
		}
	}

	return run
}
