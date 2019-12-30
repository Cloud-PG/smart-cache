package qlearn

import (
	_ "fmt"
	"math/rand"
	"testing"
)

func TestOneHotVector(t *testing.T) {
	rand.Seed(42)
	for idx := 16; idx < 1024; idx++ {
		len := rand.Intn(idx) + 2
		pos := rand.Intn(len - 1)
		res := createOneHot(len, pos)
		if res[pos] != 1.0 {
			t.Fatalf("Error: creating one hot vector of len %d with one at pos %d\nVector->%v", len, pos, res)
		}
	}
}

func TestGetArgMax(t *testing.T) {
	rand.Seed(42)
	for idx := 16; idx < 1024; idx++ {
		len := rand.Intn(idx) + 2
		pos := rand.Intn(len - 1)
		res := createOneHot(len, pos)
		resIdx := getArgMax(res)
		if resIdx != pos {
			t.Fatalf("Error: max value have to be in position %d and not in %d\nVector->%v", pos, resIdx, res)
		}
	}
}
