package qlearn

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"

	"simulator/v2/cache/ai/featuremap"

	"go.uber.org/zap"
)

// ActionType are cache possible actions
type ActionType int

// RLUpdateType are the possible update functions
type RLUpdateType int

// AgentRole are the possible table roles
type AgentRole int

const (
	// ActionNotStore indicates to store an element in cache
	ActionNotStore ActionType = iota - 4
	// ActionStore indicates to not store an element in cache
	ActionStore
	// ActionNotDelete indicates to not remove a category of files
	ActionNotDelete
	// ActionDelete indicates to remove a category of files
	ActionDelete

	// RLSARSA indicates the standard RL update algorithm SARSA
	RLSARSA RLUpdateType = iota - 2
	// RLQLearning indicates the Bellman equation
	RLQLearning

	// EvictionAgent indicates the table to choose which files to delete
	EvictionAgent AgentRole = iota - 2
	// AdditionAgent indicates the table to accept file requests
	AdditionAgent
)

var (
	logger = zap.L()
)

// QTable struct used by agents
type QTable struct {
	States         [][]int                   `json:"states"`
	Actions        [][]float64               `json:"actions"`
	FeatureManager featuremap.FeatureManager `json:"featureManager"`
	ActionTypes    []ActionType              `json:"actionTypes"`
}

// Init prepare the QTable States and Actions
func (table *QTable) Init(featureManager featuremap.FeatureManager, actions []ActionType) {
	logger := zap.L()

	table.States = make([][]int, 0)
	table.Actions = make([][]float64, 0)
	table.FeatureManager = featureManager
	table.ActionTypes = make([]ActionType, len(actions))
	copy(table.ActionTypes, actions)

	counters := make([]int, len(table.FeatureManager.Features))
	lenghts := make([]int, len(table.FeatureManager.Features))

	for idx, feature := range table.FeatureManager.Features {
		lenghts[idx] = feature.Size()
	}

	for {
		curState := make([]int, len(table.FeatureManager.Features))
		curActions := make([]float64, len(actions))

		copy(curState, counters)

		table.States = append(table.States, curState)
		table.Actions = append(table.Actions, curActions)

		allEqual := true
		for idx := 0; idx < len(counters); idx++ {
			if counters[idx]+1 != lenghts[idx] {
				allEqual = false
				break
			}
		}
		if allEqual {
			break
		}

		counters[len(counters)-1]++
		for idx := len(counters) - 1; idx > -1; idx-- {
			if counters[idx] == lenghts[idx] {
				counters[idx] = 0
				if idx-1 > -1 {
					counters[idx-1]++
				}
			}
		}
	}

	// Output test
	// for idx := 0; idx < len(table.States); idx++ {
	// 	fmt.Println(idx, table.States[idx], table.FeatureIdxs2StateIdx(table.States[idx]...))
	// }
	// fmt.Println(table.getPrevIndexesLenProd(0), len(table.States))

	// testIdxs := table.Features2Idxs([]interface{}{float64(5000.0), int64(1), int64(500000)}...)
	// fmt.Println(testIdxs, "->", table.FeatureIdxs2StateIdx(testIdxs...))

	if table.getPrevIndexesLenProd(0) != len(table.States) {
		logger.Error("State generation",
			zap.String("error", "wrong number of states generated"),
			zap.Int("numStates", len(table.States)),
			zap.Int("expectedNumStates", table.getPrevIndexesLenProd(0)),
		)
		os.Exit(-1)
	}
}

// getPrevIndexesLenProd returns the product of the feature lenghts starting from
// a specific index
func (table QTable) getPrevIndexesLenProd(start int) int {
	prod := 1
	for idx := start; idx < len(table.FeatureManager.Features); idx++ {
		prod *= table.FeatureManager.Features[idx].Size()
	}
	return prod
}

// FeatureIdxs2StateIdx returns the State index of the corresponding feature indexes
func (table QTable) FeatureIdxs2StateIdx(featureIdxs ...int) int {
	index := 0
	for idx, curIdx := range featureIdxs {
		if idx != len(featureIdxs)-1 {
			index += curIdx * table.getPrevIndexesLenProd(idx+1)
		} else {
			index += curIdx
		}
	}
	return index
}

// Features2Idxs transform a list of features in their indexes
func (table QTable) Features2Idxs(features ...interface{}) []int {
	featureIdxs := make([]int, len(features))
	for idx, val := range features {
		fmt.Println(idx, val, table.FeatureManager.Features[idx].Type)
		switch table.FeatureManager.Features[idx].ReflectType {
		case reflect.Int64:
			featureIdxs[idx] = table.FeatureManager.Features[idx].Index(val.(int64))
		case reflect.Float64:
			featureIdxs[idx] = table.FeatureManager.Features[idx].Index(val.(float64))
		}
	}
	return featureIdxs
}

// Agent used in Qlearning
type Agent struct {
	Table            QTable       `json:"qtable"`
	NumStates        int          `json:"num_states"`
	NumVars          int          `json:"num_vars"`
	LearningRate     float64      `json:"learning_rate"`
	DiscountFactor   float64      `json:"discount_factor"`
	DecayRateEpsilon float64      `json:"decay_rate_epsilon"`
	Epsilon          float64      `json:"epsilon"`
	MaxEpsilon       float64      `json:"max_epsilon"`
	MinEpsilon       float64      `json:"min_epsilon"`
	StepNum          int32        `json:"episode_counter"`
	RGenerator       *rand.Rand   `json:"r_generator"`
	UpdateFunction   RLUpdateType `json:"update_function"`
	TrainingEnabled  bool         `json:"training_enabled"`
	ValueFunction    float64      `json:"value_function"`
}

// Init initilizes the Agent struct
func (agent *Agent) Init(featureManager featuremap.FeatureManager, role AgentRole, trainingEnabled bool, initEpsilon float64, decayRateEpsilon float64) {
	logger = zap.L()

	agent.TrainingEnabled = trainingEnabled
	agent.LearningRate = 0.5 // also named Alpha
	agent.DiscountFactor = 0.5
	agent.DecayRateEpsilon = decayRateEpsilon
	agent.Epsilon = initEpsilon
	agent.MaxEpsilon = 1.0
	agent.MinEpsilon = 0.1
	switch role {
	case AdditionAgent:
		// With getArgMax the first action is the default choice
		agent.Table.Init(
			featureManager,
			[]ActionType{
				ActionNotStore,
				ActionStore,
			},
		)
		// case EvictionAgent:
		// 	// With getArgMax the first action is the default choice
		// 	agent.Actions = []ActionType{
		// 		ActionRemoveWithLRU,
		// 		ActionRemoveWithLFU,
		// 		ActionRemoveWithSizeBig,
		// 		ActionRemoveWithSizeSmall,
		// 	}
		// 	agent.ActionStrings = []string{
		// 		"ActionRemoveWithLRU",
		// 		"ActionRemoveWithLFU",
		// 		"ActionRemoveWithSizeBig",
		// 		"ActionRemoveWithSizeSmall",
		// 	}
		// case EvictionAgentExtended:
		// 	// With getArgMax the first action is the default choice
		// 	agent.Actions = []ActionType{
		// 		ActionRemoveWithLRU,
		// 		ActionRemoveWithLFU,
		// 		ActionRemoveWithSizeBig,
		// 		ActionRemoveWithSizeSmall,
		// 		ActionRemoveWithWeight,
		// 	}
		// 	agent.ActionStrings = []string{
		// 		"ActionRemoveWithLRU",
		// 		"ActionRemoveWithLFU",
		// 		"ActionRemoveWithSizeBig",
		// 		"ActionRemoveWithSizeSmall",
		// 		"ActionRemoveWithWeight",
		// 	}
	}

	agent.UpdateFunction = RLQLearning
	agent.RGenerator = rand.New(rand.NewSource(42))

	// numStates := 1
	// for _, featureLen := range featureLenghts {
	// 	numStates *= int(featureLen)
	// }
	// agent.NumStates = numStates

	// logger.Info("Num generated states", zap.Int("numStates", numStates))
	// agent.Table = QTable{
	// 	States:  []int{},
	// 	Actions: []float64{},
	// }

	// for state := range agent.genAllStates(featureLenghts) {
	// 	stateString := State2String(state)
	// 	_, inMap := agent.Table[stateString]
	// 	if !inMap {
	// 		agent.Table[stateString] = make([]float64, len(agent.Actions))
	// 	} else {
	// 		logger.Sugar().Errorf("State %v with idx %s already present...\n", state, stateString)
	// 		panic("Insert state error!!!")
	// 	}
	// }
	// agent.NumVars = agent.NumStates * len(agent.Actions)
	logger.Info("Num action values", zap.Int("numActionValues", agent.NumVars))
}

// ResetParams resets the learning parameters
func (agent *Agent) ResetParams(trainingEnabled bool, initEpsilon float64, decayRateEpsilon float64) {
	logger = zap.L()

	agent.TrainingEnabled = trainingEnabled
	agent.LearningRate = 0.9 // also named Alpha
	agent.DiscountFactor = 0.5
	agent.DecayRateEpsilon = decayRateEpsilon
	agent.Epsilon = initEpsilon
	agent.MaxEpsilon = 1.0
	agent.MinEpsilon = 0.1
	agent.StepNum = 0
	agent.ValueFunction = 0.

	logger.Info("Parameters restored as default...")
}

// ResetValueFunction clean the value function
func (agent *Agent) ResetValueFunction() {
	agent.ValueFunction = 0.
}

func (agent Agent) genAllStates(featureLenghts []int) chan []bool {
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
func (agent Agent) GetRandomFloat() float64 {
	return agent.RGenerator.Float64()
}

// ToString outputs the state values in a csv format string
// func (agent Agent) ToString(featureMap map[string]featuremap.Obj, featureMapOrder []string) string {
// 	var csvOutput []string
// 	if featureMap != nil && featureMapOrder != nil {
// 		csvOutput = append(csvOutput, strings.Join(
// 			[]string{
// 				strings.Join(agent.ActionStrings, ","),
// 				strings.Join(featureMapOrder, ","),
// 			},
// 			",",
// 		))
// 		csvOutput = append(csvOutput, "\n")
// 	}
// 	// counter := 0
// 	for state, actions := range agent.Table {
// 		for idx, action := range actions {
// 			csvOutput = append(csvOutput, fmt.Sprintf("%09.2f", action))
// 			if idx != len(actions)-1 {
// 				csvOutput = append(csvOutput, fmt.Sprint(","))
// 			}
// 		}
// 		if featureMap == nil && featureMapOrder == nil {
// 			fmt.Printf(",%s", state)
// 		} else {
// 			stateRepr := String2StateRepr(state, featureMap, featureMapOrder)
// 			csvOutput = append(csvOutput, fmt.Sprintf(",%s", stateRepr))
// 		}
// 		// counter++
// 		// fmt.Println(counter, len(agent.Table))
// 		csvOutput = append(csvOutput, "\n")
// 	}
// 	return strings.Join(csvOutput, "")
// }

// GetActionCoverage returns the exploration result of the Agent Actions
// func (agent Agent) GetActionCoverage() float64 {
// 	numSetVariables := 0
// 	for _, actions := range agent.Table {
// 		for _, action := range actions {
// 			if action != 0.0 {
// 				numSetVariables++
// 			}
// 		}
// 	}
// 	return (float64(numSetVariables) / float64(agent.NumVars)) * 100.
// }

// // GetStateCoverage returns the exploration result of the Agent States
// func (agent Agent) GetStateCoverage() float64 {
// 	numSetVariables := 0
// 	for _, actions := range agent.Table {
// 		for _, action := range actions {
// 			if action != 0.0 {
// 				numSetVariables++
// 				break
// 			}
// 		}
// 	}
// 	return (float64(numSetVariables) / float64(len(agent.Table))) * 100.
// }

// GetAction returns the possible environment action from a state
// func (agent Agent) GetAction(stateIdx string, action ActionType) float64 {
// 	values := agent.Table[stateIdx]
// 	outIdx := 0
// 	for idx := 0; idx < len(agent.Actions); idx++ {
// 		if agent.Actions[idx] == action {
// 			outIdx = idx
// 			break
// 		}
// 	}
// 	return values[outIdx]
// }

// // GetBestAction returns the action of the best action for the given state
// func (agent Agent) GetBestAction(state string) ActionType {
// 	values := agent.Table[state]
// 	maxValueIdx := getArgMax(values)
// 	logger.Debug("Get best action", zap.Float64s("values", values), zap.Int("idx max value", maxValueIdx))
// 	return agent.Actions[maxValueIdx]
// }

// GetActionIndex returns the index of a given action
// func (agent Agent) GetActionIndex(action ActionType) int {
// 	for idx, value := range agent.Actions {
// 		if value == action {
// 			return idx
// 		}
// 	}
// 	return -1
// }

// Update change the Q-table values of the given action
// func (agent *Agent) Update(state string, action ActionType, reward float64, newState string) {
// 	agent.ValueFunction += reward
// 	curStateValue := agent.GetAction(state, action)
// 	actionIdx := agent.GetActionIndex(action)
// 	if newState == "" {
// 		newState = state
// 	}
// 	switch agent.UpdateFunction {
// 	case RLSARSA:
// 		// TODO: fix next state with a proper one, not the maximum of the same state
// 		nextStateIdx := getArgMax(agent.Table[newState]) // The next state is the same
// 		agent.Table[state][actionIdx] = (1.0-agent.LearningRate)*curStateValue + agent.LearningRate*(reward+agent.DiscountFactor*agent.Table[state][nextStateIdx])
// 	case RLQLearning:
// 		nextStateIdx := getArgMax(agent.Table[newState]) // The next state is the max value
// 		agent.Table[state][actionIdx] = curStateValue + agent.LearningRate*(reward+agent.DiscountFactor*agent.Table[state][nextStateIdx]-curStateValue)
// 		// fmt.Printf(
// 		// 	"OLD VALUE %0.2f | NEW VALUE %0.2f | CUR REW %0.2f\n",
// 		// 	curStateValue, agent.Table[state][actionIdx], reward,
// 		// )
// 	}
// }

// UpdateEpsilon upgrades the epsilon variable
func (agent *Agent) UpdateEpsilon() {
	if agent.Epsilon > agent.MinEpsilon {
		agent.StepNum++
		agent.Epsilon = agent.MinEpsilon + (agent.MaxEpsilon-agent.MinEpsilon)*math.Exp(-agent.DecayRateEpsilon*float64(agent.StepNum))
	}
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
// func String2StateRepr(state string, featureMap map[string]featuremap.Obj, featureMapOrder []string) string {
// 	var (
// 		result []string
// 		curPos = 0
// 	)

// 	for _, featureName := range featureMapOrder {
// 		curCategory := featureMap[featureName]
// 		lenCategory := 0
// 		if curCategory.UnknownValues == true || curCategory.BucketOpenRight == true {
// 			lenCategory = curCategory.GetLenKeys() + 1
// 		} else {
// 			lenCategory = curCategory.GetLenKeys()
// 		}
// 		partialState := state[curPos : curPos+lenCategory]
// 		keyIdx := int(strings.IndexRune(partialState, '1'))
// 		for key, value := range curCategory.Values {
// 			if value == keyIdx {
// 				result = append(
// 					result,
// 					key,
// 				)
// 				break
// 			}
// 		}
// 		curPos += lenCategory
// 	}

// 	return strings.Join(result, ",")
// }
