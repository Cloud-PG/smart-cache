package qlearn

import (
	"fmt"
	_ "fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestQTable(t *testing.T) {
	sizes := []int{}
	numStates := 1
	for idx := 0; idx < 5; idx++ {
		curNum := rand.Intn(10) + 1
		numStates *= curNum
		sizes = append(sizes, curNum)
	}

	qTable := QTable{}
	qTable.Init(sizes, AdditionTable, 1.0, 0.000001)
	coverP := qTable.GetActionCoverage()

	if len(qTable.States) != numStates {
		t.Fatalf("Error: created %d states instead of %d", len(qTable.States), numStates)
	} else if coverP != 0. {
		t.Fatalf("Error: cover %% have to be 0.0 and not %f at init time", coverP)
	}
}
func TestOneHotVector(t *testing.T) {
	rand.Seed(42)
	for idx := 16; idx < 1024; idx++ {
		size := rand.Intn(idx) + 2
		pos := rand.Intn(size - 1)
		res := createOneHot(size, pos)
		if res[pos] != true {
			t.Fatalf("Error: creating one hot vector of len %d with one at pos %d\nVector->%v", size, pos, res)
		}
	}
}

func TestState2String(t *testing.T) {
	rand.Seed(42)
	for idx := 16; idx < 1024; idx++ {
		size := rand.Intn(idx) + 2
		pos := rand.Intn(size - 1)
		res := createOneHot(size, pos)
		tmpStr := fmt.Sprintf("%v", res)
		expectedRes := strings.Join(
			strings.Split(
				strings.ReplaceAll(
					strings.ReplaceAll(
						tmpStr[1:len(tmpStr)-1],
						"false",
						"0",
					),
					"true",
					"1",
				),
				" "),
			"",
		)
		resStr := State2String(res)
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
		res := make([]float64, size)
		res[pos] = 1.0
		resIdx := getArgMax(res)
		if resIdx != pos {
			t.Fatalf("Error: max value have to be in position %d and not in %d\nVector->%v", pos, resIdx, res)
		}
	}
}
