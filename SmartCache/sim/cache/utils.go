package cache

import (
	"fmt"
	"strconv"

	"golang.org/x/crypto/blake2b"
)

const (
	hashSize int = 8
)

// HashHexDigest convert to a string into an hash string
func HashHexDigest(input string) string {
	curHash, _ := blake2b.New(hashSize, nil)
	curHash.Write([]byte(input))
	return fmt.Sprintf("%x", curHash.Sum(nil))
}

// HashInt converto a string into an integer value from hash
func HashInt(input string) uint32 {
	uinteger, _ := strconv.ParseUint(HashHexDigest(input), 16, 64)
	return uint32(uinteger % 10000000000)
}
