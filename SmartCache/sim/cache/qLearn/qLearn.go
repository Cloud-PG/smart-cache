package qlearn

import (
	"fmt"
	"math"
	"math/rand"
	"simulator/v2/cache/ai/featuremap"
	"strings"

	"go.uber.org/zap"
)

// ActionType are cache possible actions
type ActionType int

// RLUpdateType are the possible update functions
type RLUpdateType int

// QTableRole are the possible table roles
type QTableRole int

const (
	// ActionNotStore indicates to store an element in cache
	ActionNotStore ActionType = iota - 7
	// ActionStore indicates to not store an element in cache
	ActionStore
	// ActionRemoveWithLRU indicates to remove a file with LRU policy
	ActionRemoveWithLRU
	// ActionRemoveWithLFU indicates to remove a file with LFU policy
	ActionRemoveWithLFU
	// ActionRemoveWithSizeSmall indicates to remove a file with Size Small policy
	ActionRemoveWithSizeSmall
	// ActionRemoveWithSizeBig indicates to remove a file with Size Big policy
	ActionRemoveWithSizeBig
	// ActionRemoveWithWeight indicates to remove a file with Weight policy
	ActionRemoveWithWeight

	// RLSARSA indicates the standard RL update algorithm SARSA
	RLSARSA RLUpdateType = iota - 2
	// RLQLearning indicates the Bellman equation
	RLQLearning

	// EvictionTable indicates the table to choose which files to delete
	EvictionTable QTableRole = iota - 3
	// EvictionTableExtended indicates the table to choose which files to delete version extended
	EvictionTableExtended
	// AdditionTable indicates the table to accept file requests
	AdditionTable
)

var (
	logger = zap.L()
)

// QTable used in Qlearning
type QTable struct {
	States           map[string][]float64 `json:"states"`
	NumStates        int                  `json:"num_states"`
	NumVars          int                  `json:"num_vars"`
	LearningRate     float64              `json:"learning_rate"`
	DiscountFactor   float64              `json:"discount_factor"`
	DecayRateEpsilon float64              `json:"decay_rate_epsilon"`
	Epsilon          float64              `json:"epsilon"`
	MaxEpsilon       float64              `json:"max_epsilon"`
	MinEpsilon       float64              `json:"min_epsilon"`
	StepNum          int32                `json:"episode_counter"`
	Actions          []ActionType         `json:"actions"`
	ActionStrings    []string             `json:"actionStrings"`
	RGenerator       *rand.Rand           `json:"r_generator"`
	UpdateFunction   RLUpdateType         `json:"update_function"`
}

// Init initilizes the QTable struct
func (table *QTable) Init(featureLenghts []int, role QTableRole, initEpsilon float64) {
	logger = zap.L()

	table.LearningRate = 0.9 // also named Alpha
	table.DiscountFactor = 0.5
	table.DecayRateEpsilon = 0.0000042
	table.Epsilon = initEpsilon
	table.MaxEpsilon = 1.0
	table.MinEpsilon = 0.1
	switch role {
	case AdditionTable:
		// With getArgMax the first action is the default choice
		table.Actions = []ActionType{
			ActionNotStore,
			ActionStore,
		}
		table.ActionStrings = []string{
			"ActionNotStore",
			"ActionStore",
		}
	case EvictionTable:
		// With getArgMax the first action is the default choice
		table.Actions = []ActionType{
			ActionRemoveWithLRU,
			ActionRemoveWithLFU,
			ActionRemoveWithSizeBig,
			ActionRemoveWithSizeSmall,
		}
		table.ActionStrings = []string{
			"ActionRemoveWithLRU",
			"ActionRemoveWithLFU",
			"ActionRemoveWithSizeBig",
			"ActionRemoveWithSizeSmall",
		}
	case EvictionTableExtended:
		// With getArgMax the first action is the default choice
		table.Actions = []ActionType{
			ActionRemoveWithLRU,
			ActionRemoveWithLFU,
			ActionRemoveWithSizeBig,
			ActionRemoveWithSizeSmall,
			ActionRemoveWithWeight,
		}
		table.ActionStrings = []string{
			"ActionRemoveWithLRU",
			"ActionRemoveWithLFU",
			"ActionRemoveWithSizeBig",
			"ActionRemoveWithSizeSmall",
			"ActionRemoveWithWeight",
		}
	}

	table.UpdateFunction = RLQLearning
	table.RGenerator = rand.New(rand.NewSource(42))

	numStates := 1
	for _, featureLen := range featureLenghts {
		numStates *= int(featureLen)
	}
	table.NumStates = numStates

	logger.Info("Num generated states", zap.Int("numStates", numStates))
	table.States = make(map[string][]float64, numStates)

	for state := range table.genAllStates(featureLenghts) {
		stateString := State2String(state)
		_, inMap := table.States[stateString]
		if !inMap {
			table.States[stateString] = make([]float64, len(table.Actions))
		} else {
			logger.Sugar().Errorf("State %v with idx %s already present...\n", state, stateString)
			panic("Insert state error!!!")
		}

	}
	table.NumVars = table.NumStates * len(table.Actions)
	logger.Info("Num action values", zap.Int("numActionValues", table.NumVars))
}

// ResetParams resets the learning parameters
func (table *QTable) ResetParams(initEpsilon float64) {
	logger = zap.L()

	table.LearningRate = 0.9 // also named Alpha
	table.DiscountFactor = 0.5
	table.DecayRateEpsilon = 0.000042
	table.Epsilon = initEpsilon
	table.MaxEpsilon = 1.0
	table.MinEpsilon = 0.1
	table.StepNum = 0

	logger.Info("Parameters restored as default...")
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

// GetRandomFloat generates a random number
func (table QTable) GetRandomFloat() float64 {
	return table.RGenerator.Float64()
}

// ToString outputs the state values in a csv format string
func (table QTable) ToString(featureMap *map[string]featuremap.Obj, featureMapOrder *[]string) string {
	csvOutput := ""
	if featureMap != nil && featureMapOrder != nil {
		csvOutput += strings.Join(
			[]string{
				strings.Join(table.ActionStrings, ","),
				strings.Join(*featureMapOrder, ","),
			},
			",",
		)
		csvOutput += "\n"
	}
	for state, actions := range table.States {
		for idx, action := range actions {
			csvOutput += fmt.Sprintf("%09.2f", action)
			if idx != len(actions)-1 {
				csvOutput += fmt.Sprint(",")
			}
		}
		if featureMap == nil && featureMapOrder == nil {
			fmt.Printf(",%s", state)
		} else {
			stateRepr := String2StateRepr(state, *featureMap, *featureMapOrder)
			csvOutput += fmt.Sprintf(",%s", stateRepr)
		}
		csvOutput += "\n"
	}
	return csvOutput
}

// GetActionCoverage returns the exploration result of the QTable Actions
func (table QTable) GetActionCoverage() float64 {
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

// GetStateCoverage returns the exploration result of the QTable States
func (table QTable) GetStateCoverage() float64 {
	numSetVariables := 0
	for _, actions := range table.States {
		for _, action := range actions {
			if action != 0.0 {
				numSetVariables++
				break
			}
		}
	}
	return (float64(numSetVariables) / float64(len(table.States))) * 100.
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
	maxValueIdx := getArgMax(values)
	logger.Debug("Get best action", zap.Float64s("values", values), zap.Int("idx max value", maxValueIdx))
	return table.Actions[maxValueIdx]
}

// GetActionIndex returns the index of a given action
func (table QTable) GetActionIndex(action ActionType) int {
	for idx, value := range table.Actions {
		if value == action {
			return idx
		}
	}
	return -1
}

// Update change the Q-table values of the given action
func (table *QTable) Update(state string, action ActionType, reward float64) {
	curStateValue := table.GetAction(state, action)
	actionIdx := table.GetActionIndex(action)
	switch table.UpdateFunction {
	case RLSARSA:
		// TODO: fix next state with a proper one, not the maximum of the same state
		nextStateIdx := getArgMax(table.States[state]) // The next state is the same
		table.States[state][actionIdx] = (1.0-table.LearningRate)*curStateValue + table.LearningRate*(reward+table.DiscountFactor*table.States[state][nextStateIdx])
	case RLQLearning:
		nextStateIdx := getArgMax(table.States[state]) // The next state is the max value
		table.States[state][actionIdx] = curStateValue + table.LearningRate*(reward+table.DiscountFactor*table.States[state][nextStateIdx]-curStateValue)
	}
}

// UpdateEpsilon upgrades the epsilon variable
func (table *QTable) UpdateEpsilon() {
	table.StepNum++
	table.Epsilon = table.MinEpsilon + (table.MaxEpsilon-table.MinEpsilon)*math.Exp(-table.DecayRateEpsilon*float64(table.StepNum))
}

// TODO: sistemare gli stati per avere current state e next state
// TODO: sistemare SARSA e QLearning

//##############################################################################
// Support functions
//##############################################################################

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

// String2StateRepr create a human representation of the state starting from the state string
func String2StateRepr(state string, featureMap map[string]featuremap.Obj, featureMapOrder []string) string {
	var (
		result []string
		curPos = 0
	)

	for _, featureName := range featureMapOrder {
		curCategory := featureMap[featureName]
		lenCategory := 0
		if curCategory.UnknownValues == true || curCategory.BucketOpenRight == true {
			lenCategory = curCategory.GetLenKeys() + 1
		} else {
			lenCategory = curCategory.GetLenKeys()
		}
		partialState := state[curPos : curPos+lenCategory]
		keyIdx := int(strings.IndexRune(partialState, '1'))
		for key, value := range curCategory.Values {
			if value == keyIdx {
				result = append(
					result,
					key,
				)
				break
			}
		}
		curPos += lenCategory
	}

	return strings.Join(result, ",")
}
