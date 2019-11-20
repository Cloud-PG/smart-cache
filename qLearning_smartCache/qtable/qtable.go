package qtable

import (
	"fmt"
	"math"
	"strconv"
)

// ActionType are cache possible actions
type ActionType int

const (
	// ActionStore indicates to store an element in cache
	ActionStore ActionType = iota
	// ActionNotStore indicates to not store an element in cache
	ActionNotStore
)

// QTable implements the Q-learning
type QTable struct {
	states       map[uint64][]float64
	learningRate float64
	DecayRate    float64
	features     [][]float64
	featureNames []string
	actions      []ActionType
	Epsilon      float64
	MaxEpsilon   float64
	MinEpsilon   float64
}

// Init initilizes the QTable struct
func (table *QTable) Init(features [][]float64, featureNames []string, actions []ActionType) {
	table.actions = make([]ActionType, len(actions))
	copy(table.actions, actions)

	table.featureNames = make([]string, len(featureNames))
	copy(table.featureNames, featureNames)

	table.features = make([][]float64, len(features))

	for idx, feature := range features {
		table.features[idx] = make([]float64, len(feature))
		copy(table.features[idx], feature)
	}

	table.states = make(map[uint64][]float64, 0)
	for state := range table.genStates() {
		stateIdx := table.GetStateIdx(state)
		fmt.Println(state, stateIdx)
		_, inMap := table.states[stateIdx]
		if !inMap {
			table.states[stateIdx] = make([]float64, len(actions))
		} else {
			fmt.Printf("State %v with idx %d already present...\n", state, stateIdx)
			panic("Insert state error!!!")
		}

	}

	table.PrintTable()

	table.learningRate = 0.9
	table.DecayRate = 0.005
	table.Epsilon = 1.0
	table.MaxEpsilon = 1.0
	table.MinEpsilon = 0.01
}

func createOneHot(lenght int, targetIdx int) []float64 {
	res := make([]float64, lenght)
	if targetIdx >= lenght {
		res[lenght-1] = 1.0
	} else {
		res[targetIdx] = 1.0
	}
	return res
}

func (table QTable) genStates() chan []float64 {
	genChan := make(chan []float64)
	go func() {
		defer close(genChan)
		partials := make([][]float64, 0)

		for _, feature := range table.features {
			var newEntries [][]float64
			for idx := range feature {
				oneHot := createOneHot(len(feature), idx)
				newEntries = append(newEntries, oneHot)
			}
			if len(partials) == 0 {
				for idx := 0; idx < len(newEntries); idx++ {
					partials = append(partials, make([]float64, len(newEntries[idx])))
					copy(partials[idx], newEntries[idx])
				}
			} else {
				curPartials := make([][]float64, len(partials))
				copy(curPartials, partials)
				for idx0 := 0; idx0 < len(newEntries)-1; idx0++ {
					for idx1 := 0; idx1 < len(curPartials); idx1++ {
						partials = append(partials, make([]float64, len(curPartials[idx1])))
						copy(partials[len(partials)-1], curPartials[idx1])
					}
				}
				for idx0 := 0; idx0 < len(newEntries); idx0++ {
					startIdx := len(curPartials) * idx0
					for idx1 := startIdx; idx1 < startIdx+len(curPartials); idx1++ {
						partials[idx1] = append(partials[idx1], newEntries[idx0]...)
					}
					if len(partials) > 12 {
					}

				}
			}
		}
		for _, partial := range partials {
			genChan <- partial
		}
	}()
	return genChan
}

// GetStateIdx returns the index of a given state
func (table QTable) GetStateIdx(state []float64) uint64 {
	var resIdx uint64

	for idx := 0; idx < len(state); idx++ {
		if state[idx] != 0.0 {
			resIdx += uint64(math.Pow(2.0, float64(len(state)-idx-1)))
		}
	}

	return resIdx
}

// PrintTable outputs the state values
func (table QTable) PrintTable() {
	for state, actions := range table.states {
		fmt.Printf("[%s]\t[", strconv.FormatInt(int64(state), 2))
		for idx, action := range actions {
			fmt.Printf("%09.2f", action)
			if idx != len(actions)-1 {
				fmt.Print(" ")
			}
		}
		fmt.Println("]")
	}
}

func getArgMax(array []float64) int {
	maxIdx := 0
	maxElm := array[maxIdx]
	for idx := 0; idx < len(array); idx++ {
		if array[idx] > maxElm {
			maxElm = array[idx]
			maxIdx = idx
		}
	}
	return maxIdx
}

// GetAction returns the possible environment action from a state
func (table QTable) GetAction(stateIdx uint64, action ActionType) float64 {
	values := table.states[stateIdx]
	outIdx := 0
	for idx := 0; idx < len(table.actions); idx++ {
		if table.actions[idx] == action {
			outIdx = idx
			break
		}
	}
	return values[outIdx]
}

// GetBestAction returns the action of the best action for the given state
func (table QTable) GetBestAction(state []float64) ActionType {
	stateIdx := table.GetStateIdx(state)
	values := table.states[stateIdx]
	return table.actions[getArgMax(values)]
}

func (table QTable) convertFeature(featureIdx int, value float64) int {
	for idx := 0; idx < len(table.features[featureIdx]); idx++ {
		if value <= table.features[featureIdx][idx] {
			return idx
		}
	}
	return len(table.features[featureIdx]) - 1
}

// GenCurState return the current environment state
func (table QTable) GenCurState(args []float64) []float64 {
	state := []float64{}
	for idx := 0; idx < len(table.featureNames); idx++ {
		curFeature := args[idx]
		resIdx := table.convertFeature(idx, curFeature)
		resArr := createOneHot(len(table.features[idx]), resIdx)
		state = append(state, resArr...)
	}
	return state
}

// Update change the Q-table values of the given action
func (table *QTable) Update(state []float64, action ActionType, reward float64) {
	stateIdx := table.GetStateIdx(state)
	actionValue := table.GetAction(stateIdx, action)
	maxValue := getArgMax(table.states[stateIdx])
	table.states[stateIdx][action] = actionValue + table.learningRate*(reward+table.DecayRate*table.states[stateIdx][maxValue]-actionValue)
}
