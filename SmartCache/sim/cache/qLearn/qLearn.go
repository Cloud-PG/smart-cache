package qlearn

import (
	"fmt"
	"math"
	"math/rand"
)

// ActionType are cache possible actions
type ActionType int

// RLUpdateType are the possible update functions
type RLUpdateType int

const (
	// ActionStore indicates to store an element in cache
	ActionStore ActionType = iota
	// ActionNotStore indicates to not store an element in cache
	ActionNotStore

	// RLSARSA indicates the standard RL update algorithm SARSA
	RLSARSA RLUpdateType = iota
	// RLQLearning indicates the Bellman equation
	RLQLearning
)

// QTable used in Qlearning
type QTable struct {
	States           map[string][]float64 `json:"states"`
	NumStates        int64                `json:"num_states"`
	NumVars          int64                `json:"num_vars"`
	LearningRate     float64              `json:"learning_rate"`
	DiscountFactor   float64              `json:"discount_factor"`
	DecayRateEpsilon float64              `json:"decay_rate_epsilon"`
	Epsilon          float64              `json:"epsilon"`
	MaxEpsilon       float64              `json:"max_epsilon"`
	MinEpsilon       float64              `json:"min_epsilon"`
	EpisodeCounter   float64              `json:"episode_counter"`
	Actions          []ActionType         `json:"actions"`
	RGenerator       *rand.Rand           `json:"r_generator"`
	UpdateFunction   RLUpdateType         `json:"update_function"`
}

func getArgMax(array []float64) int {
	maxIdx := 0
	maxElm := array[maxIdx]
	for idx := 1; idx < len(array); idx++ {
		if array[idx] > maxElm {
			maxElm = array[idx]
			maxIdx = idx
		}
	}
	return maxIdx
}

func createOneHot(lenght int, targetIdx int) []bool {
	res := make([]bool, lenght)
	res[targetIdx] = true
	return res
}

func (table QTable) genAllStates(featureLenghts []int) chan []bool {
	genChan := make(chan []bool)
	go func() {
		defer close(genChan)
		partials := make([][]bool, 0)

		for _, featureLenght := range featureLenghts {
			var newEntries [][]bool
			for idx := 0; idx < featureLenght; idx++ {
				oneHot := createOneHot(featureLenght, idx)
				newEntries = append(newEntries, oneHot)
			}
			if len(partials) == 0 {
				for idx := 0; idx < len(newEntries); idx++ {
					partials = append(partials, make([]bool, len(newEntries[idx])))
					copy(partials[idx], newEntries[idx])
				}
			} else {
				curPartials := make([][]bool, len(partials))
				copy(curPartials, partials)
				for idx0 := 0; idx0 < len(newEntries)-1; idx0++ {
					for idx1 := 0; idx1 < len(curPartials); idx1++ {
						partials = append(partials, make([]bool, len(curPartials[idx1])))
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
	table.LearningRate = 0.9 // also named Alpha
	table.DiscountFactor = 0.5
	table.DecayRateEpsilon = 0.000005
	table.Epsilon = 1.0
	table.MaxEpsilon = 1.0
	table.MinEpsilon = 0.1
	// With getArgMax the first action is the default choice
	table.Actions = []ActionType{
		ActionStore,
		ActionNotStore,
	}
	table.UpdateFunction = RLQLearning
	table.RGenerator = rand.New(rand.NewSource(42))

	var numStates int64 = 1
	for _, featureLen := range featureLenghts {
		numStates *= int64(featureLen)
	}
	table.NumStates = numStates

	fmt.Printf("[Generate %d states][...]\n", numStates)
	table.States = make(map[string][]float64, numStates)

	for state := range table.genAllStates(featureLenghts) {
		stateString := State2String(state)
		_, inMap := table.States[stateString]
		if !inMap {
			table.States[stateString] = make([]float64, len(table.Actions))
		} else {
			fmt.Printf("State %v with idx %s already present...\n", state, stateString)
			panic("Insert state error!!!")
		}

	}
	table.NumVars = table.NumStates * int64(len(table.Actions))
	fmt.Printf("[Tot. Vars: %d]\n", table.NumVars)
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
	return (float64(numSetVariables) / float64(table.NumVars)) * 100.
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
func (table QTable) GetBestAction(state string) ActionType {

	values := table.States[state]
	return table.Actions[getArgMax(values)]
}

// Update change the Q-table values of the given action
func (table *QTable) Update(state string, action ActionType, reward float64) {
	curStateValue := table.GetAction(state, action)
	switch table.UpdateFunction {
	case RLSARSA:
		// TODO: fix next state with a proper one, not the maximum of the same state
		nextStateIdx := getArgMax(table.States[state]) // The next state is the same
		table.States[state][action] = (1.0-table.LearningRate)*curStateValue + table.LearningRate*(reward+table.DiscountFactor*table.States[state][nextStateIdx])
	case RLQLearning:
		nextStateIdx := getArgMax(table.States[state]) // The next state is the max value
		table.States[state][action] = curStateValue + table.LearningRate*(reward+table.DiscountFactor*table.States[state][nextStateIdx]-curStateValue)
	}
	table.EpisodeCounter += 1.0
}

// UpdateEpsilon upgrades the epsilon variable
func (table *QTable) UpdateEpsilon() {
	table.Epsilon = table.MinEpsilon + (table.MaxEpsilon-table.MinEpsilon)*math.Exp(-table.DecayRateEpsilon*float64(table.EpisodeCounter))
}

// TODO: sistemare gli stati per avere current state e next state
// TODO: sistemare SARSA e QLearning

// State2String returns the string of a given state
func State2String(state []bool) string {
	var resIdx string
	for idx := 0; idx < len(state); idx++ {
		if state[idx] {
			resIdx += "1"
		} else {
			resIdx += "0"
		}
	}
	return resIdx
}
