package cache

import (
	"testing"
)

func TestBaseHashing2Int(t *testing.T) {
	res := HashInt("/test/file/0")
	if res != 2707255150 {
		t.Fatalf("Hashing error -> Expected %d but got %d", 2707255150, res)
	}
}
