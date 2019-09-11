package cache

import (
	"math"
	"time"
)

// FunctionType is used to select the weight function
type FunctionType int

const (
	// FuncFileWeight indicates the simple function for weighted cache
	FuncFileWeight FunctionType = iota
	// FuncFileWeightAndTime indicates the function that uses time
	FuncFileWeightAndTime
	// FuncFileWeightOnlyTime indicates the function that uses time
	FuncFileWeightOnlyTime
	// FuncWeightedRequests has a small memory for request time
	FuncWeightedRequests
)

func fileWeight(size float32, totRequests uint32, exp float32) float32 {
	return float32(math.Pow(float64(size)/float64(totRequests), float64(exp)))
}

func fileWeightAndTime(size float32, totRequests uint32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (size / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightOnlyTime(totRequests uint32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (1. / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightedRequest(size float32, totRequests uint32, meanTicks float32, lastTimeRequested time.Time, exp float32) float32 {
	deltaLastTimeRequested := float32(time.Now().Sub(lastTimeRequested).Seconds())
	return meanTicks * (size / float32(math.Pow(float64(totRequests), float64(exp)))) * deltaLastTimeRequested
}
