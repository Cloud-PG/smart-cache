package qlearn

import (
	"fmt"
	_ "fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestOneHotVector(t *testing.T) {
	rand.Seed(42)
	for idx := 16; idx < 1024; idx++ {
		size := rand.Intn(idx) + 2
		pos := rand.Intn(size - 1)
		res := createOneHot(size, pos)
		if res[pos] != 1.0 {
			t.Fatalf("Error: creating one hot vector of len %d with one at pos %d\nVector->%v", size, pos, res)
		}
	}
}

func TestGetStateStr(t *testing.T) {
	table := QTable{}
	rand.Seed(42)
	for idx := 16; idx < 1024; idx++ {
		size := rand.Intn(idx) + 2
		pos := rand.Intn(size - 1)
		res := createOneHot(size, pos)
		tmpStr := fmt.Sprintf("%v", res)
		expectedRes := strings.Join(
			strings.Split(tmpStr[1:len(tmpStr)-1], " "),
			"",
		)
		resStr := table.GetStateStr(res)
		if resStr != expectedRes {
			t.Fatalf("Error: expected %v but got %v", expectedRes, resStr)
		}
	}
}

func TestGetArgMax(t *testing.T) {
	rand.Seed(42)
	for idx := 16; idx < 1024; idx++ {
		size := rand.Intn(idx) + 2
		pos := rand.Intn(size - 1)
		res := createOneHot(size, pos)
		resIdx := getArgMax(res)
		if resIdx != pos {
			t.Fatalf("Error: max value have to be in position %d and not in %d\nVector->%v", pos, resIdx, res)
		}
	}
}
