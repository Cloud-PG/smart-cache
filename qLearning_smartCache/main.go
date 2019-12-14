package main

import (
	"fmt"
	"math"
	"math/rand"

	"./cache"
	"./qtable"
)

const (
	cacheSize   = 1000000.0
	numFiles    = 1000
	maxFileSize = 1000
)

var (
	allSizes              = []float64{50, 100, 200, 500, 750, maxFileSize}
	allNumReq             = []float64{1, 2, 3, 4, 5, 6, -1}
	allCacheCap           = []float64{10, 25, 50, 75, 90, 100}
	allGreatThanMinStatus = []float64{0, 1}
	allActions            = []qtable.ActionType{qtable.ActionNotStore, qtable.ActionStore}

	files = []string{}
	sizes = []float64{}

	// Random generator
	r = rand.New(rand.NewSource(42))
)

func main() {
	for idx := 0; idx < numFiles; idx++ {
		files = append(files, fmt.Sprintf("file%d", idx))
		sizes = append(sizes, float64(r.Int()%maxFileSize))
	}

	table := qtable.QTable{}
	table.Init(
		[][]float64{
			allSizes,
			allNumReq,
			allCacheCap,
			allGreatThanMinStatus,
		},
		[]string{
			"size",
			"numReq",
			"cacheCap",
			"gtms",
		},
		allActions,
	)

	// Set hyperparameters
	totalEpisodes := 1000
	maxSteps := 100000

	for episode := 0; episode < totalEpisodes; episode++ {
		fmt.Printf("----- Start Episode [%d] -----\n", episode)

		simCache := cache.SimCache{
			MaxSize: cacheSize,
		}
		simCache.Init()
		lastPoints := simCache.GetPoints()

		sumRewards := 0.0

		for step := 0; step < maxSteps; step++ {
			idx := r.Int() % len(files)
			curFile := files[idx]
			curSize := sizes[idx]

			// Get env data
			_, curNumReq, curValue := simCache.UpdateStats(curFile, curSize)
			_, minValue := simCache.GetMinValue()
			gtms := 0.0
			if curValue > minValue {
				gtms = 1.0
			}

			// Get current state
			curState := table.GenCurState(
				[]float64{
					curSize,
					curNumReq,
					simCache.GetCapacity(),
					gtms,
				},
			)

			var curAction qtable.ActionType

			/* ----- Q-learning ----- */

			// Check random action
			expTradeoff := r.Float64()
			if expTradeoff > table.Epsilon {
				// action
				curAction = table.GetBestAction(curState)
			} else {
				// random choice
				if expTradeoff > 0.5 {
					curAction = qtable.ActionStore
				} else {
					curAction = qtable.ActionNotStore
				}
			}

			// Take the action

			switch curAction {
			case qtable.ActionStore:
				simCache.Insert(curFile, curSize)
			}

			reward := simCache.GetPoints() - lastPoints

			// Update table
			table.Update(curState, curAction, reward)

			sumRewards += reward
			fmt.Printf("Reward step %d -> %0.2f\r", step, reward)
		}
		fmt.Printf("Total reward -> %0.2f\n", sumRewards)

		// Update epsilon
		table.Epsilon = table.MinEpsilon + (table.MaxEpsilon-table.MinEpsilon)*math.Exp(-table.DecayRate*float64(episode))
	}

	table.PrintTable()

}
