package functions

import (
	"math"
)

// WeightFun is the type of the function exported
type WeightFun func(int64, float64, float64, float64, float64, float64) float64

// FunctionType is used to select the weight function
type Type int

const (
	// Additive indicates the simple function for weight function cache with parameters
	Additive Type = iota - 4
	// AdditiveExp indicates the simple function for weight function cache with parameters but exponential
	AdditiveExp
	// Multiplicative  indicates the simple function for weight function cache with parameter as exponentials
	Multiplicative
	// WeightedRequests has a small memory for request time
	WeightedRequests
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
