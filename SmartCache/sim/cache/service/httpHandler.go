package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"simulator/v2/cache"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
)

func Version(buildstamp string, githash string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debug().Str("received",
			r.Method).Str("host",
			r.Host).Str("ip",
			r.RemoteAddr).Str("X-FORWARDED-FOR",
			r.Header.Get("X-FORWARDED-FOR")).Msg("Version")

		resp := fmt.Sprintf("Build time:\t%s\nGit hash:\t%s\n", buildstamp, githash)
		_, errWrite := w.Write([]byte(resp))

		if errWrite != nil {
			log.Err(errWrite).Str("resp", resp).Msg("Cannot write a response")
		} else {
			log.Printf("Sent response %s", resp)
		}
	}
}

func Get(cacheInstance cache.Cache) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debug().Str("received",
			r.Method).Str("host",
			r.Host).Str("ip",
			r.RemoteAddr).Str("X-FORWARDED-FOR",
			r.Header.Get("X-FORWARDED-FOR")).Msg("Get")

		// var rbody bytes.Buffer
		// [0] -> filename int64
		// [1] -> size     float64
		// [2] -> protocol int64
		// [3] -> cpuEff   float64
		// [4] -> day      int64
		// [5] -> siteName int64
		// [6] -> userID   int64
		// [7] -> fileType int64

		var errWrite error

		log.Debug().Str("URL params", r.URL.String()).Msg("Get")

		filename, err := strconv.ParseInt(r.URL.Query().Get("filename"), 10, 64)
		if err != nil {
			log.Err(err).Str("params", r.URL.String()).Msg("Cannot parse URL parameters")

			w.WriteHeader(http.StatusBadRequest)

			_, errWrite = w.Write([]byte("Cannot parse URL parameter filename"))
			if errWrite != nil {
				log.Err(errWrite).Msg("Cannot write a response")
			}

			return
		}

		size, err := strconv.ParseFloat(r.URL.Query().Get("size"), 64)
		if err != nil {
			log.Err(err).Str("params", r.URL.String()).Msg("Cannot parse URL parameters")

			w.WriteHeader(http.StatusBadRequest)

			_, errWrite = w.Write([]byte("Cannot parse URL parameter size"))
			if errWrite != nil {
				log.Err(errWrite).Msg("Cannot write a response")
			}

			return
		}

		log.Debug().Int64("filename",
			filename).Float64("size",
			size).Msg("Get - request params")

		toInsert, _ := cache.GetFile(cacheInstance, filename, size)

		_, errWrite = w.Write([]byte(fmt.Sprintf("{ \"to_insert\": %t}", toInsert)))

		if errWrite != nil {
			log.Err(errWrite).Msg("Cannot write a response")
		} else {
			log.Printf("Sent response %s", "")
		}
	}
}

type statsJSON struct {
	Date                    string  `json:"date"`
	NumReq                  int64   `json:"num_req"`
	NumHit                  int64   `json:"num_hit"`
	NumAdded                int64   `json:"num_added"`
	NumDeleted              int64   `json:"num_deleted"`
	NumRedirected           int64   `json:"num_redirected"`
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
	CPUEfficiency           float64 `json:"cpu_efficiency"`
	CPUHitEfficiency        float64 `json:"cpu_hit_efficiency"`
	CPUMissEfficiency       float64 `json:"cpu_miss_efficiency"`
	CPUEfficiencyUpperBound float64 `json:"cpu_efficiency_upper_bound"`
	CPUEfficiencyLowerBound float64 `json:"cpu_efficiency_lower_bound"`
}

func Stats(cacheInstance cache.Cache) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debug().Str("received",
			r.Method).Str("host",
			r.Host).Str("ip",
			r.RemoteAddr).Str("X-FORWARDED-FOR",
			r.Header.Get("X-FORWARDED-FOR")).Msg("Stats")

		curStats := statsJSON{
			Date:                    time.Now().String(),
			NumReq:                  cache.NumRequests(cacheInstance),
			NumHit:                  cache.NumHits(cacheInstance),
			NumAdded:                cache.NumAdded(cacheInstance),
			NumDeleted:              cache.NumDeleted(cacheInstance),
			NumRedirected:           cache.NumRedirected(cacheInstance),
			NumMissAfterDelete:      cache.GetTotDeletedFileMiss(cacheInstance),
			SizeRedirected:          cache.RedirectedSize(cacheInstance),
			CacheSize:               cache.GetMaxSize(cacheInstance),
			Size:                    cache.Size(cacheInstance),
			Capacity:                cache.Capacity(cacheInstance),
			Bandwidth:               cache.Bandwidth(cacheInstance),
			BandwidthUsage:          cache.BandwidthUsage(cacheInstance),
			HitRate:                 cache.HitRate(cacheInstance),
			WeightedHitRate:         cache.WeightedHitRate(cacheInstance),
			WrittenData:             cache.DataWritten(cacheInstance),
			ReadData:                cache.DataRead(cacheInstance),
			ReadOnHitData:           cache.DataReadOnHit(cacheInstance),
			ReadOnMissData:          cache.DataReadOnMiss(cacheInstance),
			DeletedData:             cache.DataDeleted(cacheInstance),
			AvgFreeSpace:            cache.AvgFreeSpace(cacheInstance),
			StdDevFreeSpace:         cache.StdDevFreeSpace(cacheInstance),
			CPUEfficiency:           cache.CPUEff(cacheInstance),
			CPUHitEfficiency:        cache.CPUHitEff(cacheInstance),
			CPUMissEfficiency:       cache.CPUMissEff(cacheInstance),
			CPUEfficiencyUpperBound: cache.CPUEffUpperBound(cacheInstance),
			CPUEfficiencyLowerBound: cache.CPUEffLowerBound(cacheInstance),
		}

		// fmt.Printf("%+v\n", curStats)
		var errWrite error

		jsonOutput, errMarshal := json.Marshal(curStats)
		if errMarshal != nil {
			log.Err(errMarshal).Str("curStats", fmt.Sprintf("%+v", curStats)).Msg("Cannot marshal stats json")

			w.WriteHeader(http.StatusBadRequest)
			_, errWrite = w.Write([]byte(fmt.Sprintf("Cannot marshal stats json:\n---\n%+v\n---", curStats)))
		} else {
			_, errWrite = w.Write(jsonOutput)
		}

		if errWrite != nil {
			log.Err(errWrite).Str("resp", string(jsonOutput)).Msg("Cannot write response")
		} else {
			log.Printf("Sent response %s", string(jsonOutput))
		}
	}
}
