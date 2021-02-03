package cache

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	"golang.org/x/crypto/blake2b"
)

const (
	hashSize int = 8
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
		log.Err(err).Msg("Utils")
	}

	run := 0
	for _, f := range files {
		if strings.Contains(f.Name(), "_run-") {
			run++
		}
	}

	return run
}

// OutputCSV is an utility to output CSV
type OutputCSV struct {
	filename         string
	file             *os.File
	compressedWriter *gzip.Writer
	csvWriter        *csv.Writer
}

// Create an output file in CSV format
func (output *OutputCSV) Create(filename string, compressed bool) {
	if compressed {
		output.filename = filename + ".gz"
	} else {
		output.filename = filename
	}

	outputFile, errCreateFile := os.Create(output.filename)
	if errCreateFile != nil {
		panic(errCreateFile)
	}
	output.file = outputFile

	if compressed {
		output.compressedWriter = gzip.NewWriter(output.file)
		output.csvWriter = csv.NewWriter(output.compressedWriter)
	} else {
		output.csvWriter = csv.NewWriter(output.file)
	}

}

// Close the output file after flush the buffer
func (output OutputCSV) Write(record []string) {
	if errWriter := output.csvWriter.Write(record); errWriter != nil {
		panic(errWriter)
	}
	output.csvWriter.Flush()
}

// Close the output file after flush the buffer
func (output OutputCSV) Close() {
	output.csvWriter.Flush()
	if output.compressedWriter != nil {
		output.compressedWriter.Close()
	}
	output.file.Close()
}
