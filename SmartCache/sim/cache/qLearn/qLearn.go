package qlearn

import (
	"fmt"
	"math"
	"math/rand"
)

// ActionType are cache possible actions
type ActionType int

const (
	// ActionStore indicates to store an element in cache
	ActionStore ActionType = iota
	// ActionNotStore indicates to not store an element in cache
	ActionNotStore
)

// QTable used in Qlearning
type QTable struct {
	States           map[string][]float64 `json:"states"`
	numStates        int64                `json:"num_states"`
	numVars          int64                `json:"num_vars"`
	LearningRate     float64              `json:"learning_rate"`
	DecayRate        float64              `json:"decay_rate"`
	DecayRateEpsilon float64              `json:"decay_rate_epsilon"`
	Epsilon          float64              `json:"epsilon"`
	MaxEpsilon       float64              `json:"max_epsilon"`
	MinEpsilon       float64              `json:"min_epsilon"`
	EpisodeCounter   float64              `json:"episode_counter"`
	Actions          []ActionType         `json:"actions"`
	RGenerator       *rand.Rand           `json:"r_generator"`
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

func createOneHot(lenght int, targetIdx int) []float64 {
	res := make([]float64, lenght)
	res[targetIdx] = 1.0
	return res
}

func (table QTable) genAllStates(featureLenghts []int) chan []float64 {
	genChan := make(chan []float64)
	go func() {
		defer close(genChan)
		partials := make([][]float64, 0)

		for _, featureLenght := range featureLenghts {
			var newEntries [][]float64
			for idx := 0; idx < featureLenght; idx++ {
				oneHot := createOneHot(featureLenght, idx)
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

// Init initilizes the QTable struct
func (table *QTable) Init(featureLenghts []int) {
	table.LearningRate = 0.9
	table.DecayRate = 0.0 // No discount
	table.DecayRateEpsilon = 0.000005
	table.Epsilon = 1.0
	table.MaxEpsilon = 1.0
	table.MinEpsilon = 0.1
	table.Actions = []ActionType{
		ActionNotStore,
		ActionStore,
	}
	table.RGenerator = rand.New(rand.NewSource(42))

	var numStates int64 = 1
	for _, featureLen := range featureLenghts {
		numStates *= int64(featureLen)
	}
	table.numStates = numStates

	fmt.Printf("[Generate %d states][...]\n", numStates)
	table.States = make(map[string][]float64, numStates)

	for state := range table.genAllStates(featureLenghts) {
		stateIdx := table.GetStateIdx(state)
		_, inMap := table.States[stateIdx]
		if !inMap {
			table.States[stateIdx] = make([]float64, len(table.Actions))
		} else {
			fmt.Printf("State %v with idx %s already present...\n", state, stateIdx)
			panic("Insert state error!!!")
		}

	}
	table.numVars = table.numStates * int64(len(table.Actions))
	fmt.Printf("[Tot. Vars: %d]\n", table.numVars)
}

// GetRandomTradeOff generates a random number
func (table QTable) GetRandomTradeOff() float64 {
	return table.RGenerator.Float64()
}

// PrintTable outputs the state values
func (table QTable) PrintTable() {
	for state, actions := range table.States {
		fmt.Printf("[%s]\t[", state)
		for idx, action := range actions {
			fmt.Printf("%09.2f", action)
			if idx != len(actions)-1 {
				fmt.Print(" ")
			}
		}
		fmt.Println("]")
	}
}

// GetCoveragePercentage returns the exploration result of the QTable
func (table QTable) GetCoveragePercentage() float64 {
	numSetVariables := 0
	for _, actions := range table.States {
		for _, action := range actions {
			if action != 0.0 {
				numSetVariables++
			}
		}
	}
	return (float64(numSetVariables) / float64(table.numVars)) * 100.
}

// GetStateIdx returns the index of a given state
func (table QTable) GetStateIdx(state []float64) string {
	var resIdx string
	for idx := 0; idx < len(state); idx++ {
		if state[idx] != 0.0 {
			resIdx += "1"
		} else {
			resIdx += "0"
		}
	}
	return resIdx
}

// GetAction returns the possible environment action from a state
func (table QTable) GetAction(stateIdx string, action ActionType) float64 {
	values := table.States[stateIdx]
	outIdx := 0
	for idx := 0; idx < len(table.Actions); idx++ {
		if table.Actions[idx] == action {
			outIdx = idx
			break
		}
	}
	return values[outIdx]
}

// GetBestAction returns the action of the best action for the given state
func (table QTable) GetBestAction(state []float64) ActionType {
	stateIdx := table.GetStateIdx(state)
	values := table.States[stateIdx]
	return table.Actions[getArgMax(values)]
}

// Update change the Q-table values of the given action
func (table *QTable) Update(state []float64, action ActionType, reward float64) {
	stateIdx := table.GetStateIdx(state)
	actionValue := table.GetAction(stateIdx, action)
	maxValue := getArgMax(table.States[stateIdx])
	table.States[stateIdx][action] = actionValue + table.LearningRate*(reward+table.DecayRate*table.States[stateIdx][maxValue]-actionValue)
	table.EpisodeCounter += 1.0
}

// UpdateEpsilon upgrades the epsilon variable
func (table *QTable) UpdateEpsilon() {
	table.Epsilon = table.MinEpsilon + (table.MaxEpsilon-table.MinEpsilon)*math.Exp(-table.DecayRateEpsilon*float64(table.EpisodeCounter))
}
