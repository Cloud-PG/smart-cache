package functions

import (
	"math"
)

// FunctionType is used to select the weight function
type FunctionType int

const (
	// FuncAdditive indicates the simple function for weight function cache with parameters
	FuncAdditive FunctionType = iota - 4
	// FuncAdditiveExp indicates the simple function for weight function cache with parameters but exponential
	FuncAdditiveExp
	// FuncMultiplicative  indicates the simple function for weight function cache with parameter as exponentials
	FuncMultiplicative
	// FuncWeightedRequests has a small memory for request time
	FuncWeightedRequests
)

func FileAdditiveWeight(totRequests int64, size float64, meanTicks float64, alpha float64, beta float64, gamma float64) float64 {
	return alpha*float64(totRequests) + beta*size + gamma*meanTicks
}

func FileAdditiveExpWeight(totRequests int64, size float64, meanTicks float64, alpha float64, beta float64, gamma float64) float64 {
	return float64(math.Pow(float64(totRequests), alpha) + math.Pow(float64(size), beta) + math.Pow(float64(meanTicks), gamma))
}

func FileMultiplicativeWeight(totRequests int64, size float64, meanTicks float64, alpha float64, beta float64, gamma float64) float64 {
	return float64(math.Pow(float64(totRequests), alpha) * math.Pow(float64(size), beta) * math.Pow(float64(meanTicks), gamma))
}

func FileWeightedRequest(totRequests int64, size float64, meanTicks float64) float64 {
	return meanTicks + (size / math.Exp(float64(totRequests)))
}
