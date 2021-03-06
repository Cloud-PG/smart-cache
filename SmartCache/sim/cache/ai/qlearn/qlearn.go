package qlearn

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"simulator/v2/cache/ai/featuremap"
	"strings"

	"go.uber.org/zap"
)

// ActionType are cache possible actions.
type ActionType int

// RLUpdateAlg are the possible update functions.
type RLUpdateAlg int

// AgentRole are the possible table roles.
type AgentRole int

const (
	// ActionNotStore indicates to store an element in cache.
	ActionNotStore ActionType = iota - 7
	// ActionStore indicates to not store an element in cache.
	ActionStore
	// ActionNotDelete indicates to not remove a category of files.
	ActionNotDelete
	// ActionDeleteAll indicates to remove a all files from a category.
	ActionDeleteAll
	// ActionDeleteHalf indicates to remove an half of the category files.
	ActionDeleteHalf
	// ActionDeleteQuarter indicates to remove a quarter of the category files.
	ActionDeleteQuarter
	// ActionDeleteOne indicates to remove a file of the category.
	ActionDeleteOne
)

const (
	// RLSARSA indicates the standard RL update algorithm SARSA.
	RLSARSA RLUpdateAlg = iota - 2
	// RLQLearning indicates the Bellman equation.
	RLQLearning
)

const (
	// EvictionAgent indicates the table to choose which files to delete.
	EvictionAgent AgentRole = iota - 2
	// AdditionAgent indicates the table to accept file requests.
	AdditionAgent
)

const (
	maxStoredPastChoices = 32
)

// QTable struct used by agents.
type QTable struct {
	States          [][]int                    `json:"states"`
	Actions         [][]float64                `json:"actions"`
	FeatureManager  *featuremap.FeatureManager `json:"featureManager"`
	ActionTypeIdxs  map[ActionType]int         `json:"actionTypeIdxs"`
	ActionTypes     []ActionType               `json:"actionTypes"`
	IndexWeights    []int                      `json:"indexWeights"`
	logger          *zap.Logger
	RandomGenerator *rand.Rand
}

// Init prepare the QTable States and Actions
func (table *QTable) Init(featureManager *featuremap.FeatureManager, actions []ActionType, randSeed int64) { //nolint:ignore,funlen
	table.logger = zap.L()

	table.States = make([][]int, 0)
	table.Actions = make([][]float64, 0)
	table.FeatureManager = featureManager
	table.ActionTypeIdxs = make(map[ActionType]int, len(actions))
	table.ActionTypes = make([]ActionType, len(actions))
	table.RandomGenerator = rand.New(rand.NewSource(randSeed))

	for idx, action := range actions {
		table.ActionTypeIdxs[action] = idx
	}

	copy(table.ActionTypes, actions)

	counters := make([]int, len(table.FeatureManager.Features))
	lengths := make([]int, len(table.FeatureManager.Features))

	for idx, feature := range table.FeatureManager.Features {
		lengths[idx] = feature.Size()
	}

	for {
		curState := make([]int, len(table.FeatureManager.Features))
		curActions := make([]float64, len(actions))

		copy(curState, counters)

		table.States = append(table.States, curState)
		table.Actions = append(table.Actions, curActions)

		allEqual := true

		for idx := 0; idx < len(counters); idx++ {
			if counters[idx]+1 != lengths[idx] {
				allEqual = false

				break
			}
		}

		if allEqual {
			break
		}

		counters[len(counters)-1]++
		for idx := len(counters) - 1; idx > -1; idx-- {
			if counters[idx] == lengths[idx] {
				counters[idx] = 0
				if idx-1 > -1 {
					counters[idx-1]++
				}
			}
		}
	}

	table.IndexWeights = table.FeatureManager.FeatureIdxWeights

	// ----- Output test -----
	// fmt.Println("Index weights", table.IndexWeights)

	// for idx := 0; idx < len(table.States); idx++ {
	// 	fmt.Println(idx, table.States[idx], table.FeatureIdxs2StateIdx(table.States[idx]...))
	// }

	// testIdxs := table.Features2Idxs([]interface{}{float64(5000.0), int64(1), int64(500000)}...)
	// fmt.Println(testIdxs, "->", table.FeatureIdxs2StateIdx(testIdxs...))
}

// FeatureIdxs2StateIdx returns the State index of the corresponding feature indexes
func (table QTable) FeatureIdxs2StateIdx(featureIdxs ...int) int {
	index := 0

	for idx, curIdx := range featureIdxs {
		if idx != len(featureIdxs)-1 {
			index += curIdx * table.IndexWeights[idx]
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
		// fmt.Println(idx, val, table.FeatureManager.Features[idx].Type)
		switch table.FeatureManager.Features[idx].ReflectType {
		case reflect.Int64:
			featureIdxs[idx] = table.FeatureManager.Features[idx].Index(val.(int64))
		case reflect.Float64:
			featureIdxs[idx] = table.FeatureManager.Features[idx].Index(val.(float64))
		}
	}

	return featureIdxs
}

// ResetActions put to 0 all action values
func (table *QTable) ResetActions() {
	for idx := 0; idx < len(table.Actions); idx++ {
		table.Actions[idx] = make([]float64, len(table.Actions[idx]))
	}
}

// Choice of an agent
type Choice struct {
	State     int        `json:"state"`
	Action    ActionType `json:"action"`
	Tick      int64      `json:"tick"`
	DeltaT    int64      `json:"deltaT"`
	Capacity  float64    `json:"capacity"`
	Hit       bool       `json:"hit"`
	Size      float64    `json:"Size"`
	Frequency int64      `json:"frequency"`
}

// Agent used in Qlearning
type Agent struct {
	Memory              map[interface{}][]Choice `json:"memory"`
	QTable              QTable                   `json:"qtable"`
	NumStates           int                      `json:"numStates"`
	NumVars             int                      `json:"numVars"`
	LearningRate        float64                  `json:"learningRate"`
	DiscountFactor      float64                  `json:"discountFactor"`
	DecayRateEpsilon    float64                  `json:"decayRateEpsilon"`
	Epsilon             float64                  `json:"epsilon"`
	MaxEpsilon          float64                  `json:"maxEpsilon"`
	MinEpsilon          float64                  `json:"minEpsilon"`
	StepNum             int64                    `json:"episodeCounter"`
	RGenerator          *rand.Rand               `json:"rGenerator"`
	UpdateAlgorithm     RLUpdateAlg              `json:"updateAlgorithm"`
	QValue              float64                  `json:"qValue"`
	allowEpsilonUnleash bool                     `json:"allowEpsilonUnleash"`
	logger              *zap.Logger
}

// Init initilizes the Agent struct
func (agent *Agent) Init(featureManager *featuremap.FeatureManager, role AgentRole, initEpsilon float64, decayRateEpsilon float64, allowEpsilonUnleash bool, randSeed int64) {
	agent.logger = zap.L()

	agent.LearningRate = 0.9 // also named Alpha
	agent.DiscountFactor = 0.5
	agent.DecayRateEpsilon = decayRateEpsilon
	agent.Epsilon = initEpsilon
	agent.MaxEpsilon = 1.0
	agent.MinEpsilon = 0.1
	agent.Memory = make(map[interface{}][]Choice)
	agent.allowEpsilonUnleash = allowEpsilonUnleash

	switch role {
	case AdditionAgent:
		// With getArgMax the first action is the default choice
		agent.QTable.Init(
			featureManager,
			[]ActionType{
				ActionNotStore,
				ActionStore,
			},
			randSeed,
		)
	case EvictionAgent:
		// With getArgMax the first action is the default choice
		agent.QTable.Init(
			featureManager,
			[]ActionType{
				ActionNotDelete,
				ActionDeleteOne,
				ActionDeleteQuarter,
				ActionDeleteHalf,
				ActionDeleteAll,
			},
			randSeed,
		)
	}

	agent.UpdateAlgorithm = RLQLearning
	agent.RGenerator = agent.QTable.RandomGenerator

	agent.NumStates = len(agent.QTable.States)
	agent.NumVars = agent.NumStates * len(agent.QTable.Actions[0])

	agent.logger.Info("Agent",
		zap.Int("numStates", agent.NumStates),
		zap.Int("numVars", agent.NumVars),
	)
}

// ResetParams resets the learning parameters
func (agent *Agent) ResetParams(initEpsilon float64, decayRateEpsilon float64) {
	agent.logger = zap.L()

	agent.LearningRate = 0.9 // also named Alpha
	agent.DiscountFactor = 0.5
	agent.DecayRateEpsilon = decayRateEpsilon
	agent.Epsilon = initEpsilon
	agent.MaxEpsilon = 1.0
	agent.MinEpsilon = 0.1
	agent.StepNum = 0
	agent.QValue = 0.

	agent.logger.Info("Parameters restored as default...")
}

// ResetQValue clean the value function
func (agent *Agent) ResetQValue() {
	agent.QValue = 0.
}

// ResetTableAction clean QTable actions
func (agent *Agent) ResetTableAction() {
	agent.QTable.ResetActions()
}

// UnleashEpsilon set Epsilon to 1.0
func (agent *Agent) UnleashEpsilon(newEpsilon interface{}) {
	if agent.allowEpsilonUnleash {
		agent.Epsilon = 1.0
		agent.StepNum = 0

		if newEpsilon != nil {
			targetEpsilon := newEpsilon.(float64)
			for agent.Epsilon > targetEpsilon {
				agent.UpdateEpsilon()
			}
		}
	}
}

// GetRandomFloat generates a random number
func (agent Agent) GetRandomFloat() float64 {
	return agent.RGenerator.Float64()
}

// QTableToString outputs the state values in a csv format string
func (agent Agent) QTableToString() string {
	var (
		csvOutput = make([]string, 0)
		tmp       = make([]string, 0)
	)

	for _, action := range agent.QTable.ActionTypes {
		switch action {
		case ActionDeleteAll:
			tmp = append(tmp, "ActionDeleteAll")
		case ActionDeleteHalf:
			tmp = append(tmp, "ActionDeleteHalf")
		case ActionDeleteQuarter:
			tmp = append(tmp, "ActionDeleteQuarter")
		case ActionDeleteOne:
			tmp = append(tmp, "ActionDeleteOne")
		case ActionNotDelete:
			tmp = append(tmp, "ActionNotDelete")
		case ActionStore:
			tmp = append(tmp, "ActionStore")
		case ActionNotStore:
			tmp = append(tmp, "ActionNotStore")
		}
	}

	for _, feature := range agent.QTable.FeatureManager.Features {
		tmp = append(tmp, feature.Name)
	}

	csvOutput = append(csvOutput, strings.Join(tmp, ","))

	// counter := 0
	for idx, state := range agent.QTable.States {
		tmp = tmp[:0]

		for _, value := range agent.QTable.Actions[idx] {
			tmp = append(tmp, fmt.Sprintf("%09.2f", value))
		}

		for featureIdx, featureValIdx := range state {
			curFeature := agent.QTable.FeatureManager.Features[featureIdx]
			tmp = append(tmp, curFeature.ToString(featureValIdx))
		}

		csvOutput = append(csvOutput, strings.Join(tmp, ","))
	}

	return strings.Join(csvOutput, "\n")
}

// GetCoverage returns action and state coverage percentages
func (agent Agent) GetCoverage() (float64, float64) {
	actionsCov := 0
	stateCov := 0

	for _, actions := range agent.QTable.Actions {
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
	return agent.QTable.Actions[stateIdx][agent.QTable.ActionTypeIdxs[action]]
}

// GetBestActionValue returns the best action for the given state
func (agent Agent) GetBestActionValue(stateIdx int) float64 {
	values := agent.QTable.Actions[stateIdx]
	maxValueIdx, maxValue := agent.getArgMax(values)
	agent.logger.Debug("Get best action",
		zap.Float64s("values", values),
		zap.Int("idx max value", maxValueIdx),
	)

	return maxValue
}

// GetBestAction returns the best action for the given state
func (agent Agent) GetBestAction(stateIdx int) ActionType {
	values := agent.QTable.Actions[stateIdx]
	maxValueIdx, _ := agent.getArgMax(values)
	bestAction := agent.QTable.ActionTypes[maxValueIdx]
	agent.logger.Debug("Get best action",
		zap.Float64s("values", values),
		zap.Int("idx max value", maxValueIdx),
		zap.Int("type best action", int(bestAction)),
	)

	return bestAction
}

// UpdateTable change the table values of the given action
func (agent *Agent) UpdateTable(stateIdx int, newStateIdx int, action ActionType, reward float64) {
	agent.QValue += reward

	curStateValue := agent.GetActionValue(stateIdx, action)
	nextStateBestValue := agent.GetBestActionValue(newStateIdx)

	actionIdx, inActionTable := agent.QTable.ActionTypeIdxs[action]
	if !inActionTable {
		panic(fmt.Sprintf("ERROR: wrong action passed... action -> %d\n", action))
	}

	switch agent.UpdateAlgorithm {
	// case RLSARSA:
	// 	nextStateIdx := getArgMax(agent.QTable[newState]) // The next state is the same
	// 	agent.QTable.Actions[stateIdx][actionIdx] = (1.0-agent.LearningRate)*curStateValue + agent.LearningRate*(reward+agent.DiscountFactor*agent.QTable.Actions[state][nextStateIdx])
	case RLQLearning:
		newQ := curStateValue + agent.LearningRate*(reward+agent.DiscountFactor*nextStateBestValue-curStateValue)
		agent.QTable.Actions[stateIdx][actionIdx] = newQ
	case RLSARSA:
		panic("ERROR: not implemented...")
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

// GetMemories returns made actions in memory
func (agent *Agent) GetMemories(key interface{}) []Choice {
	pastChoices, inMemory := agent.Memory[key]
	if !inMemory {
		panic("Error: Memory not present...")
	}

	return pastChoices
}

// DeleteMemory remove past choices in memory
func (agent *Agent) DeleteMemory(key interface{}) {
	delete(agent.Memory, key)
}

// ResetMemories clean all the agent memories
func (agent *Agent) ResetMemories() {
	for key := range agent.Memory {
		delete(agent.Memory, key)
	}

	agent.Memory = make(map[interface{}][]Choice)
}

// SaveMemoryWithNoLimits insert made actions in memory with no limits on memory size
func (agent *Agent) SaveMemoryWithNoLimits(key interface{}, choice Choice) {
	pastChoices, inMemory := agent.Memory[key]

	if inMemory {
		pastChoices = append(pastChoices, choice)
		agent.Memory[key] = pastChoices
	} else {
		newChoices := make([]Choice, 0)
		newChoices = append(newChoices, choice)
		agent.Memory[key] = newChoices
	}
}

// SaveMemory insert made actions in memory
func (agent *Agent) SaveMemory(key interface{}, choice Choice) {
	pastChoices, inMemory := agent.Memory[key]

	if inMemory {
		if len(pastChoices) < maxStoredPastChoices {
			pastChoices = append(pastChoices, choice)
		} else {
			pastChoices = pastChoices[1:]
			pastChoices = append(pastChoices, choice)
		}

		agent.Memory[key] = pastChoices
	} else {
		newChoices := make([]Choice, 0)
		newChoices = append(newChoices, choice)
		agent.Memory[key] = newChoices
	}
}

// Remember returns some memories and then delete them
func (agent *Agent) Remember(key interface{}) ([]Choice, bool) {
	pastChoices, inMemory := agent.Memory[key]

	memories := make([]Choice, 0)

	if inMemory {
		memories = make([]Choice, len(pastChoices))
		copy(memories, pastChoices)
		delete(agent.Memory, key)
	}

	return memories, inMemory
}

// returns the max value and its index. If multiple values are the maximum it
// chooces randomly between them.
func (agent *Agent) getArgMax(array []float64) (int, float64) {
	maxIdx := 0
	maxElm := array[maxIdx]
	equalElms := make([]int, 0, len(array))

	for idx := 1; idx < len(array); idx++ {
		if array[idx] > maxElm {
			maxElm = array[idx]
			maxIdx = idx
			equalElms = equalElms[:0]
			equalElms = append(equalElms, idx)
		} else if array[idx] == maxElm {
			equalElms = append(equalElms, idx)
		}
	}

	if len(equalElms) > 1 {
		maxIdx = equalElms[agent.RGenerator.Intn(len(equalElms))]
		maxElm = array[maxIdx]
	}

	return maxIdx, maxElm
}

//##############################################################################
//#                            Support functions                               #
//##############################################################################

func createOneHot(length int, targetIdx int) []bool {
	res := make([]bool, length)
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
