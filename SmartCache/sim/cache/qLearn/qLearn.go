package qlearn

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strings"

	"simulator/v2/cache/ai/featuremap"

	"go.uber.org/zap"
)

// ActionType are cache possible actions
type ActionType int

// RLUpdateType are the possible update functions
type RLUpdateAlg int

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
	RLSARSA RLUpdateAlg = iota - 2
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
	ActionTypeIdxs map[ActionType]int        `json:"actionTypeIdxs"`
	ActionTypes    []ActionType              `json:"actionTypes"`
}

// Init prepare the QTable States and Actions
func (table *QTable) Init(featureManager featuremap.FeatureManager, actions []ActionType) {
	logger := zap.L()

	table.States = make([][]int, 0)
	table.Actions = make([][]float64, 0)
	table.FeatureManager = featureManager
	table.ActionTypeIdxs = make(map[ActionType]int, len(actions))
	table.ActionTypes = make([]ActionType, len(actions))

	for idx, action := range actions {
		table.ActionTypeIdxs[action] = idx
	}
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

	// ----- Output test -----
	// for idx := 0; idx < len(table.States); idx++ {
	// 	fmt.Println(idx, table.States[idx], table.FeatureIdxs2StateIdx(table.States[idx]...))
	// }
	// fmt.Println(table.getPrevIndexesSizeProd(0), len(table.States))

	// testIdxs := table.Features2Idxs([]interface{}{float64(5000.0), int64(1), int64(500000)}...)
	// fmt.Println(testIdxs, "->", table.FeatureIdxs2StateIdx(testIdxs...))

	if table.getPrevIndexesSizeProd(0) != len(table.States) {
		logger.Error("State generation",
			zap.String("error", "wrong number of states generated"),
			zap.Int("numStates", len(table.States)),
			zap.Int("expectedNumStates", table.getPrevIndexesSizeProd(0)),
		)
		os.Exit(-1)
	}
}

// getPrevIndexesSizeProd returns the product of the feature lenghts starting from
// a specific index
func (table QTable) getPrevIndexesSizeProd(start int) int {
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
			index += curIdx * table.getPrevIndexesSizeProd(idx+1)
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
	Table            QTable      `json:"qtable"`
	NumStates        int         `json:"numStates"`
	NumVars          int         `json:"numVars"`
	LearningRate     float64     `json:"learningRate"`
	DiscountFactor   float64     `json:"discountFactor"`
	DecayRateEpsilon float64     `json:"decayRateEpsilon"`
	Epsilon          float64     `json:"epsilon"`
	MaxEpsilon       float64     `json:"maxEpsilon"`
	MinEpsilon       float64     `json:"minEpsilon"`
	StepNum          int32       `json:"episodeCounter"`
	RGenerator       *rand.Rand  `json:"rGenerator"`
	UpdateAlgorithm  RLUpdateAlg `json:"updateAlgorithm"`
	ValueFunction    float64     `json:"valueFunction"`
}

// Init initilizes the Agent struct
func (agent *Agent) Init(featureManager featuremap.FeatureManager, role AgentRole, initEpsilon float64, decayRateEpsilon float64) {
	logger = zap.L()

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
	case EvictionAgent:
		// With getArgMax the first action is the default choice
		agent.Table.Init(
			featureManager,
			[]ActionType{
				ActionNotDelete,
				ActionDelete,
			},
		)
	}

	agent.UpdateAlgorithm = RLQLearning
	agent.RGenerator = rand.New(rand.NewSource(42))

	agent.NumStates = len(agent.Table.States)
	agent.NumVars = agent.NumStates * len(agent.Table.Actions[0])

	logger.Info("Agent",
		zap.Int("numStates", agent.NumStates),
		zap.Int("numVars", agent.NumVars),
	)
}

// ResetParams resets the learning parameters
func (agent *Agent) ResetParams(initEpsilon float64, decayRateEpsilon float64) {
	logger = zap.L()

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

// GetRandomFloat generates a random number
func (agent Agent) GetRandomFloat() float64 {
	return agent.RGenerator.Float64()
}

// QTableToString outputs the state values in a csv format string
func (agent Agent) QTableToString() string {
	var csvOutput []string

	var tmp []string

	for _, action := range agent.Table.ActionTypes {
		switch action {
		case ActionDelete:
			tmp = append(tmp, "ActionDelete")
		case ActionNotDelete:
			tmp = append(tmp, "ActionNotDelete")
		case ActionStore:
			tmp = append(tmp, "ActionStore")
		case ActionNotStore:
			tmp = append(tmp, "ActionNotStore")
		}
	}
	for _, feature := range agent.Table.FeatureManager.Features {
		tmp = append(tmp, feature.Name)
	}

	csvOutput = append(csvOutput, strings.Join(tmp, ","))

	// counter := 0
	for idx, state := range agent.Table.States {
		tmp = tmp[:0]

		for _, value := range agent.Table.Actions[idx] {
			tmp = append(tmp, fmt.Sprintf("%09.2f", value))
		}

		for featureIdx, featureValIdx := range state {
			curFeature := agent.Table.FeatureManager.Features[featureIdx]
			switch curFeature.ReflectType {
			case reflect.Int64:
				curFeatureVal := curFeature.Int64Values[featureValIdx]
				tmp = append(tmp, fmt.Sprintf("%d", curFeatureVal))
			case reflect.Float64:
				curFeatureVal := curFeature.Float64Values[featureValIdx]
				tmp = append(tmp, fmt.Sprintf("%09.2f", curFeatureVal))
			}

		}

		csvOutput = append(csvOutput, strings.Join(tmp, ","))
	}
	return strings.Join(csvOutput, "\n")
}

// GetCoverage returns action and state coverage percentages
func (agent Agent) GetCoverage() (float64, float64) {
	actionsCov := 0
	stateCov := 0
	for _, actions := range agent.Table.Actions {
		curStateCov := false
		for _, action := range actions {
			if action != 0.0 {
				actionsCov++
				curStateCov = true
			}
		}
		if curStateCov {
			stateCov++
		}
	}
	actionCovPerc := (float64(actionsCov) / float64(agent.NumVars)) * 100.
	stateCovPerc := (float64(stateCov) / float64(agent.NumStates)) * 100.
	return actionCovPerc, stateCovPerc
}

// GetActionValue returns the value of a state action
func (agent Agent) GetActionValue(stateIdx int, action ActionType) float64 {
	return agent.Table.Actions[stateIdx][agent.Table.ActionTypeIdxs[action]]
}

// GetBestActionValue returns the best action for the given state
func (agent Agent) GetBestActionValue(stateIdx int) float64 {
	values := agent.Table.Actions[stateIdx]
	maxValueIdx, maxValue := getArgMax(values)
	logger.Debug("Get best action",
		zap.Float64s("values", values),
		zap.Int("idx max value", maxValueIdx),
	)
	return maxValue
}

// GetBestAction returns the best action for the given state
func (agent Agent) GetBestAction(stateIdx int) ActionType {
	values := agent.Table.Actions[stateIdx]
	maxValueIdx, _ := getArgMax(values)
	bestAction := agent.Table.ActionTypes[maxValueIdx]
	logger.Debug("Get best action",
		zap.Float64s("values", values),
		zap.Int("idx max value", maxValueIdx),
		zap.Int("type best action", int(bestAction)),
	)
	return bestAction
}

// Update change the Q-table values of the given action
func (agent *Agent) Update(stateIdx int, action ActionType, reward float64, newStateIdx int) {
	agent.ValueFunction += reward

	curStateValue := agent.GetActionValue(stateIdx, action)
	nextStateBestValue := agent.GetBestActionValue(newStateIdx)

	actionIdx := agent.Table.ActionTypeIdxs[action]

	switch agent.UpdateAlgorithm {
	// case RLSARSA:
	// 	nextStateIdx := getArgMax(agent.Table[newState]) // The next state is the same
	// 	agent.Table.Actions[stateIdx][actionIdx] = (1.0-agent.LearningRate)*curStateValue + agent.LearningRate*(reward+agent.DiscountFactor*agent.Table.Actions[state][nextStateIdx])
	case RLQLearning:
		newQ := curStateValue + agent.LearningRate*(reward+agent.DiscountFactor*nextStateBestValue-curStateValue)
		agent.Table.Actions[stateIdx][actionIdx] = newQ
	default:
		panic(fmt.Sprintf("Update %d is not implemented", agent.UpdateAlgorithm))
	}
}

// UpdateEpsilon upgrades the epsilon variable
func (agent *Agent) UpdateEpsilon() {
	if agent.Epsilon > agent.MinEpsilon {
		agent.StepNum++
		agent.Epsilon = agent.MinEpsilon + (agent.MaxEpsilon-agent.MinEpsilon)*math.Exp(-agent.DecayRateEpsilon*float64(agent.StepNum))
	}
}

//##############################################################################
//#                            Support functions                               #
//##############################################################################

func getArgMax(array []float64) (int, float64) {
	maxIdx := 0
	maxElm := array[maxIdx]
	for idx := 1; idx < len(array); idx++ {
		if array[idx] > maxElm {
			maxElm = array[idx]
			maxIdx = idx
		}
	}
	return maxIdx, maxElm
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
