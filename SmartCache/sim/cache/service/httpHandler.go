package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"simulator/v2/cache"
	"time"

	"github.com/rs/zerolog/log"
)

func Version(buildstamp string, githash string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received %s request for host %s from IP address %s and X-FORWARDED-FOR %s",
			r.Method, r.Host, r.RemoteAddr, r.Header.Get("X-FORWARDED-FOR"))

		resp := fmt.Sprintf("Build time:\t%s\nGit hash:\t%s\n", buildstamp, githash)
		_, errWrite := w.Write([]byte(resp))

		if errWrite != nil {
			log.Err(errWrite).Str("resp", resp).Msg("Cannot write a response")
		} else {
			log.Printf("Sent response %s", resp)
		}
	}
}

type statsJSON struct {
	Date                    string  `json:"date"`
	NumReq                  int     `json:"num_req"`
	NumHit                  int     `json:"num_hit"`
	NumAdded                int     `json:"num_added"`
	NumDeleted              int     `json:"num_deleted"`
	NumRedirected           int     `json:"num_redirected"`
	NumMissAfterDelete      int     `json:"num_miss_after_delete"`
	SizeRedirected          float64 `json:"size_redirected"`
	CacheSize               float64 `json:"cache_size"`
	Size                    float64 `json:"size"`
	Capacity                float64 `json:"capacity"`
	Bandwidth               float64 `json:"bandwidth"`
	BandwidthUsage          float64 `json:"bandwidth_usage"`
	HitRate                 float64 `json:"hit_rate"`
	WeightedHitRate         float64 `json:"weighted_hit_rate"`
	WrittenData             float64 `json:"written_data"`
	ReadData                float64 `json:"read_data"`
	ReadOnHitData           float64 `json:"read_on_hit_data"`
	ReadOnMissData          float64 `json:"read_on_miss_data"`
	DeletedData             float64 `json:"deleted_data"`
	AvgFreeSpace            float64 `json:"avg_free_space"`
	StdDevFreeSpace         float64 `json:"std_dev_free_space"`
	CpuEfficiency           float64 `json:"cpu_efficiency"`
	CpuHitEfficiency        float64 `json:"cpu_hit_efficiency"`
	CpuMissEfficiency       float64 `json:"cpu_miss_efficiency"`
	CpuEfficiencyUpperBound float64 `json:"cpu_efficiency_upper_bound"`
	CpuEfficiencyLowerBound float64 `json:"cpu_efficiency_lower_bound"`
}

func Stats(cacheInstance cache.Cache) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received %s request for host %s from IP address %s and X-FORWARDED-FOR %s",
			r.Method, r.Host, r.RemoteAddr, r.Header.Get("X-FORWARDED-FOR"))

		curStats := statsJSON{
			Date: time.Now().String(),
		}

		// cache.NumRequests(cacheInstance)
		// cache.NumHits(cacheInstance)
		// cache.NumAdded(cacheInstance)
		// cache.NumDeleted(cacheInstance)
		// cache.NumRedirected(cacheInstance)
		// cache.GetTotDeletedFileMiss(cacheInstance)
		// cache.RedirectedSize(cacheInstance)
		// cache.GetMaxSize(cacheInstance)
		// cache.Size(cacheInstance)
		// cache.Capacity(cacheInstance)
		// cache.Bandwidth(cacheInstance)
		// cache.BandwidthUsage(cacheInstance)
		// cache.HitRate(cacheInstance)
		// cache.WeightedHitRate(cacheInstance)
		// cache.DataWritten(cacheInstance)
		// cache.DataRead(cacheInstance)
		// cache.DataReadOnHit(cacheInstance)
		// cache.DataReadOnMiss(cacheInstance)
		// cache.DataDeleted(cacheInstance)
		// cache.AvgFreeSpace(cacheInstance)
		// cache.StdDevFreeSpace(cacheInstance)
		// cache.CPUEff(cacheInstance)
		// cache.CPUHitEff(cacheInstance)
		// cache.CPUMissEff(cacheInstance)
		// cache.CPUEffUpperBound(cacheInstance)
		// cache.CPUEffLowerBound(cacheInstance)

		jsonOutput, errMarshal := json.Marshal(curStats)
		if errMarshal != nil {
			log.Err(errMarshal).Str("resp", string(jsonOutput)).Msg("Cannot marchal stats json")
		}

		_, errWrite := w.Write(jsonOutput)

		if errWrite != nil {
			log.Err(errWrite).Str("resp", string(jsonOutput)).Msg("Cannot write stats json")
		} else {
			log.Printf("Sent response %s", string(jsonOutput))
		}
	}
}
