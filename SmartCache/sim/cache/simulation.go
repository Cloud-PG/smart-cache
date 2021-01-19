package cache

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

const (
	csvHeader        = "Filename,SiteName,UserID,TaskID,TaskMonitorID,JobID,Protocol,JobExecExitCode,JobStart,JobEnd,NumCPU,WrapWC,WrapCPU,Size,DataType,FileType,JobLengthH,JobLengthM,JobSuccess,CPUTime,IOTime,reqDay,Region,Campaign,Process"
	readerBufferSize = 1024
)

var (
	/*
		CSV HEADER:
			- [0] Filename (int64)
			- [1] SiteName (int64)
			- [2] UserID (int64)
			- [3] TaskID (int64)
			- [4] TaskMonitorID (int64)
			- [5] JobID (int64)
			- [6] Protocol (int64)
			- [7] JobExecExitCode (int64)
			- [8] JobStart (int64)
			- [9] JobEnd (int64)
			- [10] NumCPU (int64)
			- [11] WrapWC (float64)
			- [12] WrapCPU (float64)
			- [13] Size (float64)
			- [14] DataType (int64)
			- [15] FileType (int64)
			- [16] JobLengthH (float64)
			- [17] JobLengthM (float64)
			- [18] JobSuccess (bool)
			- [19] CPUTime (float64)
			- [20] IOTime (float64)
			- [21] reqDay (int64)
			- [22] Region (int64)
			- [23] Campaign (int64)
			- [24] Process (int64)
	*/
	csvHeaderIndexes = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
)

// SimulationStats is used to output the simulation statistics
type SimulationStats struct {
	TimeElapsed           string  `json:"timeElapsed"`
	Extra                 string  `json:"extra"`
	TotNumRecords         int64   `json:"totNumRecords"`
	TotJumpedRecords      int64   `json:"totJumpedRecords"`
	TotInvalidRecords     int64   `json:"totInvalidRecords"`
	TotFilteredRecords    int64   `json:"totFilteredRecords"`
	TotRedirectedRecords  int64   `json:"totRedirectedRecords"`
	SizeRedirectedRecords float64 `json:"sizeRedirectedRecords"`
	AvgSpeed              string  `json:"avgSpeed"`
}

// CSVRecord is the base record composition readed from the logs
type CSVRecord struct {
	Day             int64   `json:"day"`
	Region          int64   `json:"region"`
	Filename        int64   `json:"filename"`
	FileType        int64   `json:"fileType"`
	DataType        int64   `json:"dataType"`
	Campaign        int64   `json:"campaign"`
	Process         int64   `json:"process"`
	Protocol        int64   `json:"protocol"`
	TaskMonitorID   int64   `json:"taskMonitorID"`
	TaskID          int64   `json:"taskID"`
	JobID           int64   `json:"jobID"`
	SiteName        int64   `json:"siteName"`
	JobExecExitCode int64   `json:"jobExecExitCode"`
	JobStart        int64   `json:"jobStart"`
	JobEnd          int64   `json:"jobEnd"`
	JobSuccess      bool    `json:"jobSuccess"`
	JobLengthH      float64 `json:"jobLengthH"`
	JobLengthM      float64 `json:"jobLengthM"`
	UserID          int64   `json:"user"`
	NumCPU          int64   `json:"numCPU"`
	WrapWC          float64 `json:"WrapWC"`
	WrapCPU         float64 `json:"WrapCPU"`
	CPUTime         float64 `json:"CPUTime"`
	IOTime          float64 `json:"IOTime"`
	Size            float64 `json:"size"`
	SizeM           float64 `json:"sizeM"`
}

func recordGenerator(csvReader *csv.Reader, curFile *os.File, headerMap []int) chan CSVRecord { //nolint:ignore,funlen
	channel := make(chan CSVRecord, readerBufferSize)

	go func() {
		defer close(channel)
		defer curFile.Close()

		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Err(err).Msg("recordGenerator")
			}

			// fmt.Println(record)

			filename, _ := strconv.ParseInt(record[headerMap[0]], 10, 64)
			siteName, _ := strconv.ParseInt(record[headerMap[1]], 10, 64)
			userID, _ := strconv.ParseInt(record[headerMap[2]], 10, 64)
			taskID, _ := strconv.ParseInt(record[headerMap[3]], 10, 64)
			taskMonitorID, _ := strconv.ParseInt(record[headerMap[4]], 10, 64)
			jobID, _ := strconv.ParseInt(record[headerMap[5]], 10, 64)
			protocol, _ := strconv.ParseInt(record[headerMap[6]], 10, 64)
			size, _ := strconv.ParseFloat(record[headerMap[13]], 64)
			dataType, _ := strconv.ParseInt(record[headerMap[14]], 10, 64)
			fileType, _ := strconv.ParseInt(record[headerMap[15]], 10, 64)
			joblengthh, _ := strconv.ParseFloat(record[headerMap[16]], 64)
			joblengthm, _ := strconv.ParseFloat(record[headerMap[17]], 64)
			jobSuccess, _ := strconv.ParseBool(record[headerMap[18]])
			cputime, _ := strconv.ParseFloat(record[headerMap[19]], 64)
			iotime, _ := strconv.ParseFloat(record[headerMap[20]], 64)
			day, _ := strconv.ParseFloat(record[headerMap[21]], 64)
			region, _ := strconv.ParseInt(record[headerMap[22]], 10, 64)
			campaign, _ := strconv.ParseInt(record[headerMap[23]], 10, 64)
			process, _ := strconv.ParseInt(record[headerMap[24]], 10, 64)

			sizeInMegabytes := size / (1024. * 1024.)

			curRecord := CSVRecord{
				Day:           int64(day),
				Region:        region,
				Filename:      filename,
				FileType:      fileType,
				DataType:      dataType,
				Protocol:      protocol,
				TaskMonitorID: taskMonitorID,
				TaskID:        taskID,
				JobID:         jobID,
				SiteName:      siteName,
				JobSuccess:    jobSuccess,
				JobLengthH:    joblengthh,
				JobLengthM:    joblengthm,
				UserID:        userID,
				CPUTime:       cputime,
				IOTime:        iotime,
				Size:          size,
				SizeM:         sizeInMegabytes,
				Campaign:      campaign,
				Process:       process,
			}

			// fmt.Println(curRecord)

			channel <- curRecord
		}
	}()
	return channel
}

// OpenSimFile opens a simulation file
func OpenSimFile(filePath string) chan CSVRecord {

	fileExt := path.Ext(filePath)
	var iterator chan CSVRecord

	log.Debug().Str("filename", filePath).Msg("Open Data File")
	curFile, errOpenFile := os.Open(filePath)
	if errOpenFile != nil {
		panic(errOpenFile)
	}

	headerMap := csvHeaderIndexes

	switch fileExt {
	case ".gz", ".gzip":
		// Create new reader to decompress gzip.
		curCsv, errReadGz := gzip.NewReader(curFile)
		if errReadGz != nil {
			panic(errReadGz)
		}
		csvReader := csv.NewReader(curCsv)
		// Discar header
		header, errCSVRead := csvReader.Read()
		headerStr := strings.Join(header, ",")
		log.Debug().Str("CSV header", headerStr).Str("file", filePath).Msg("File header")
		if headerStr != csvHeader {
			log.Info().Str("CSV header",
				headerStr).Str("file",
				filePath).Msg("File header is different from standard")
			headerMap = getHeaderIndexes(header)
		}
		if errCSVRead != nil {
			panic(errCSVRead)
		}
		iterator = recordGenerator(csvReader, curFile, headerMap)
	default:
		csvReader := csv.NewReader(curFile)
		// Discar header
		header, errCSVRead := csvReader.Read()
		headerStr := strings.Join(header, ",")
		log.Debug().Str("CSV header", headerStr).Str("file", filePath).Msg("FIle header")
		if headerStr != csvHeader {
			headerMap = getHeaderIndexes(header)
		}
		if errCSVRead != nil {
			panic(errCSVRead)
		}
		iterator = recordGenerator(csvReader, curFile, headerMap)
	}

	return iterator
}

func getHeaderIndexes(header []string) []int {
	var indexes []int
	for _, name := range strings.Split(csvHeader, ",") {
		for idx, curHeaderName := range header {
			if name == curHeaderName {
				indexes = append(indexes, idx)
			}
		}
	}
	return indexes
}

// OpenSimFolder opens a simulation folder
func OpenSimFolder(dirPath *os.File) chan CSVRecord {
	var fileList = make([]string, 0)

	channel := make(chan CSVRecord)
	fileStats, _ := dirPath.Readdir(0)

	for _, file := range fileStats {
		fileList = append(fileList, path.Join(dirPath.Name(), file.Name()))
	}
	sort.Slice(fileList, func(i, j int) bool { return fileList[i] < fileList[j] })

	go func() {
		defer close(channel)
		for _, name := range fileList {
			fileExt := path.Ext(name)
			switch fileExt {
			case ".csv":
			case ".gz":
				for record := range OpenSimFile(name) {
					channel <- record
				}
			}

		}
	}()

	return channel
}

// OutputCSV is an utility to output CSV
type OutputCSV struct {
	filename         string
	file             *os.File
	compressedWriter *gzip.Writer
	csvWriter        *csv.Writer
}

// Create an output file in CSV format
func (output *OutputCSV) Create(filename string, compressed bool) {
	if compressed {
		output.filename = filename + ".gz"
	} else {
		output.filename = filename
	}

	outputFile, errCreateFile := os.Create(output.filename)
	if errCreateFile != nil {
		panic(errCreateFile)
	}
	output.file = outputFile

	if compressed {
		output.compressedWriter = gzip.NewWriter(output.file)
		output.csvWriter = csv.NewWriter(output.compressedWriter)
	} else {
		output.csvWriter = csv.NewWriter(output.file)
	}

}

// Close the output file after flush the buffer
func (output OutputCSV) Write(record []string) {
	if errWriter := output.csvWriter.Write(record); errWriter != nil {
		panic(errWriter)
	}
	output.csvWriter.Flush()
}

// Close the output file after flush the buffer
func (output OutputCSV) Close() {
	output.csvWriter.Flush()
	if output.compressedWriter != nil {
		output.compressedWriter.Close()
	}
	output.file.Close()
}

// Filter interface
type Filter interface {
	Check(CSVRecord) bool
}

// UsDataMcTypes for USA
type UsDataMcTypes struct {
}

// Check if the record have to be sent to the cache
func (filter UsDataMcTypes) Check(record CSVRecord) bool {
	// Check if data type == data, mc
	if record.DataType == 0 || record.DataType == 3 {
		return true
	}
	return false
}

// ItDataMcTypes for IT
type ItDataMcTypes struct {
}

// Check if the record have to be sent to the cache
func (filter ItDataMcTypes) Check(record CSVRecord) bool {
	// Check if data type == data, mc
	if record.DataType == 0 || record.DataType == 1 {
		return true
	}
	return false
}

// SuccessJob filter
type SuccessJob struct {
}

// Check if the record is with a success or not
func (filter SuccessJob) Check(record CSVRecord) bool {
	if record.JobSuccess && record.JobExecExitCode == 0 {
		return true
	}
	return false
}

// UsMINIAODNOT1andT3 for USA MINIAOD records without fnalpc and T1
type UsMINIAODNOT1andT3 struct {
}

// Check if the record have to be sent to the cache
func (filter UsMINIAODNOT1andT3) Check(record CSVRecord) bool {
	// Check if file type == MINIAOD, MINIAODSIM
	// Check if site != T1_US_FNAL, T3_US_FNALLPC
	if (record.FileType == 2 || record.FileType == 9) && record.SiteName < 9 {
		return true
	}
	return false
}

// UsMINIAODNOFNALLPCNOT1FNALLFilter for USA MINIAOD records without fnalpc and T1
type UsMINIAODNOFNALLPCNOT1FNALLFilter struct {
}

// Check if the record have to be sent to the cache
func (filter UsMINIAODNOFNALLPCNOT1FNALLFilter) Check(record CSVRecord) bool {
	// Check if file type == MINIAOD, MINIAODSIM
	// Check if site != T1_US_FNAL, T3_US_FNALLPC
	if (record.FileType == 2 || record.FileType == 9) && record.SiteName != 9 && record.SiteName != 16 {
		return true
	}
	return false
}

// UsMINIAODandNOFNALLPCFilter for USA MINIAOD records without fnalpc
type UsMINIAODandNOFNALLPCFilter struct {
}

// Check if the record have to be sent to the cache
func (filter UsMINIAODandNOFNALLPCFilter) Check(record CSVRecord) bool {
	// Check if file type == MINIAOD, MINIAODSIM
	// Check if site != T1_US_FNAL, T3_US_FNALLPC
	if (record.FileType == 2 || record.FileType == 9) && record.SiteName != 16 {
		return true
	}
	return false
}

// GetCacheSize returns the cache size in megabytes
func GetCacheSize(cacheSize float64, cacheSizeUnit string) float64 {
	res := -1.
	switch cacheSizeUnit {
	case "M", "m":
		res = cacheSize
	case "G", "g":
		res = cacheSize * 1024.
	case "T", "t":
		res = cacheSize * 1024. * 1024.
	case "P", "p":
		res = cacheSize * 1024. * 1024. * 1024.
	default:
		panic("ERROR: unit size not recognized...")
	}
	return res
}

type SimulationParams struct {
	LoadDump           bool
	Dump               bool
	DumpFilesAndStats  bool
	ColdStart          bool
	ColdStartNoStats   bool
	DataPath           string
	OutFile            string
	BaseName           string
	ResultRunStatsName string
	DumpFileName       string
	LoadDumpFileName   string
	AIRLEpsilonStart   float64
	AIRLEpsilonDecay   float64
	CPUprofile         string
	MEMprofile         string
	WindowSize         int
	WindowStart        int
	WindowStop         int
	OutputUpdateDelay  float64
	RecordFilter       Filter
	DataTypeFilter     Filter
}

func Simulate(cacheType string, cacheInstance Cache, param SimulationParams) { //nolint:ignore,funlen
	// Simulation variables
	var (
		numDailyRecords    int64
		numInvalidRecords  int64
		numJumpedRecords   int64
		numFilteredRecords int64
		totNumRecords      int64
		totIterations      uint64
		numIterations      uint64
		windowStepCounter  int
		windowCounter      int
		succesJobFilter    = SuccessJob{}
		redirectedData     float64
		numRedirected      int64
	)

	if param.LoadDump { //nolint:ignore,nestif
		log.Info().Str("filename", param.LoadDumpFileName).Msg("Loading cache dump")

		latestCacheRun := GetSimulationRunNum(filepath.Dir(param.LoadDumpFileName))

		renameErr := os.Rename(
			param.OutFile,
			fmt.Sprintf("%s_run-%02d.csv",
				strings.Split(param.OutFile, ".")[0],
				latestCacheRun,
			),
		)
		if renameErr != nil {
			panic(renameErr)
		}

		loadedDump := Load(cacheInstance, param.LoadDumpFileName)

		if cacheType == "aiRL" {
			Loads(cacheInstance, loadedDump, param.AIRLEpsilonStart, param.AIRLEpsilonDecay)
		} else {
			Loads(cacheInstance, loadedDump)
		}

		log.Info().Msg("Cache dump loaded!")
		if param.ColdStart {
			if param.ColdStartNoStats {
				Clear(cacheInstance)
				log.Info().Msg("Cache Files deleted... COLD START with NO STATISTICS")
			} else {
				ClearFiles(cacheInstance)
				log.Info().Msg("Cache Files deleted... COLD START")
			}
		} else {
			log.Info().Msg("Cache Files stored... HOT START")
		}
	}

	// Open simulation files
	fileStats, statErr := os.Stat(param.DataPath)
	if statErr != nil {
		fmt.Printf("ERR: Cannot open source %s.\n", param.DataPath)
		panic(statErr)
	}

	var iterator chan CSVRecord

	switch mode := fileStats.Mode(); {
	case mode.IsRegular():
		iterator = OpenSimFile(param.DataPath)
	case mode.IsDir():
		curFolder, _ := os.Open(param.DataPath)

		defer func() {
			closeErr := curFolder.Close()
			if closeErr != nil {
				panic(closeErr)
			}
		}()

		iterator = OpenSimFolder(curFolder)
	}

	csvSimOutput := OutputCSV{}

	csvSimOutput.Create(param.OutFile, false)
	defer csvSimOutput.Close()

	csvHeaderColumns := []string{"date",
		"num req",
		"num hit",
		"num added",
		"num deleted",
		"num redirected",
		"num miss after delete",
		"size redirected",
		"cache size",
		"size",
		"capacity",
		"bandwidth",
		"bandwidth usage",
		"hit rate",
		"weighted hit rate",
		"written data",
		"read data",
		"read on hit data",
		"read on miss data",
		"deleted data",
		"avg free space",
		"std dev free space",
		"CPU efficiency",
		"CPU hit efficiency",
		"CPU miss efficiency",
		"CPU efficiency upper bound",
		"CPU efficiency lower bound",
	}
	if cacheType == "aiRL" {
		csvHeaderColumns = append(csvHeaderColumns, "Addition epsilon")
		csvHeaderColumns = append(csvHeaderColumns, "Eviction epsilon")
		csvHeaderColumns = append(csvHeaderColumns, "Addition qvalue function")
		csvHeaderColumns = append(csvHeaderColumns, "Eviction qvalue function")
		csvHeaderColumns = append(csvHeaderColumns, "Eviction calls")
		csvHeaderColumns = append(csvHeaderColumns, "Eviction forced calls")
		csvHeaderColumns = append(csvHeaderColumns, "Eviction mean num categories")
		csvHeaderColumns = append(csvHeaderColumns, "Eviction std dev num categories")
		csvHeaderColumns = append(csvHeaderColumns, "Action store")
		csvHeaderColumns = append(csvHeaderColumns, "Action not store")
		csvHeaderColumns = append(csvHeaderColumns, "Action delete all")
		csvHeaderColumns = append(csvHeaderColumns, "Action delete half")
		csvHeaderColumns = append(csvHeaderColumns, "Action delete quarter")
		csvHeaderColumns = append(csvHeaderColumns, "Action delete one")
		csvHeaderColumns = append(csvHeaderColumns, "Action not delete")
	}
	csvSimOutput.Write(csvHeaderColumns)

	simBeginTime := time.Now()
	start := time.Now()
	var latestTime time.Time

	if param.CPUprofile != "" {
		profileOut, err := os.Create(param.CPUprofile)
		if err != nil {
			fmt.Printf("ERR: Can not create CPU profile file %s.\n", err)
			panic("ERROR: on create cpu profile")
		}

		log.Info().Str("filename", param.CPUprofile).Msg("Enable CPU profiliing")
		startProfileErr := pprof.StartCPUProfile(profileOut)

		if startProfileErr != nil {
			panic(startProfileErr)
		}

		defer pprof.StopCPUProfile()
	}

	log.Info().Msg("Simulation START")

	for record := range iterator {
		numIterations++

		// --------------------- Make daily output ---------------------
		if latestTime.IsZero() {
			latestTime = time.Unix(record.Day, 0.)
		}

		curTime := time.Unix(record.Day, 0.)

		if curTime.Sub(latestTime).Hours() >= 23. {
			if windowCounter >= param.WindowStart {
				csvRow := []string{
					latestTime.String(),
					fmt.Sprintf("%d", NumRequests(cacheInstance)),
					fmt.Sprintf("%d", NumHits(cacheInstance)),
					fmt.Sprintf("%d", NumAdded(cacheInstance)),
					fmt.Sprintf("%d", NumDeleted(cacheInstance)),
					fmt.Sprintf("%d", NumRedirected(cacheInstance)),
					fmt.Sprintf("%d", GetTotDeletedFileMiss(cacheInstance)),
					fmt.Sprintf("%f", RedirectedSize(cacheInstance)),
					fmt.Sprintf("%f", GetMaxSize(cacheInstance)),
					fmt.Sprintf("%f", Size(cacheInstance)),
					fmt.Sprintf("%f", Capacity(cacheInstance)),
					fmt.Sprintf("%f", Bandwidth(cacheInstance)),
					fmt.Sprintf("%f", BandwidthUsage(cacheInstance)),
					fmt.Sprintf("%0.2f", HitRate(cacheInstance)),
					fmt.Sprintf("%0.2f", WeightedHitRate(cacheInstance)),
					fmt.Sprintf("%f", DataWritten(cacheInstance)),
					fmt.Sprintf("%f", DataRead(cacheInstance)),
					fmt.Sprintf("%f", DataReadOnHit(cacheInstance)),
					fmt.Sprintf("%f", DataReadOnMiss(cacheInstance)),
					fmt.Sprintf("%f", DataDeleted(cacheInstance)),
					fmt.Sprintf("%f", AvgFreeSpace(cacheInstance)),
					fmt.Sprintf("%f", StdDevFreeSpace(cacheInstance)),
					fmt.Sprintf("%f", CPUEff(cacheInstance)),
					fmt.Sprintf("%f", CPUHitEff(cacheInstance)),
					fmt.Sprintf("%f", CPUMissEff(cacheInstance)),
					fmt.Sprintf("%f", CPUEffUpperBound(cacheInstance)),
					fmt.Sprintf("%f", CPUEffLowerBound(cacheInstance)),
				}
				if cacheType == "aiRL" {
					csvRow = append(csvRow, strings.Split(ExtraOutput(cacheInstance, "epsilonStats"), ",")...)
					csvRow = append(csvRow, strings.Split(ExtraOutput(cacheInstance, "valueFunctions"), ",")...)
					csvRow = append(csvRow, strings.Split(ExtraOutput(cacheInstance, "evictionStats"), ",")...)
					csvRow = append(csvRow, strings.Split(ExtraOutput(cacheInstance, "evictionCategoryStats"), ",")...)
					csvRow = append(csvRow, strings.Split(ExtraOutput(cacheInstance, "actionStats"), ",")...)
				}

				csvSimOutput.Write(csvRow)
			}
			// log.Debug().Float64("CPU eff.",
			// 	CPUEff(cacheInstance),
			// ).Float64("CPU eff Upper Bound",
			// 	CPUEffUpperBound(cacheInstance),
			// ).Float64("CPU eff Lower Bound",
			// 	CPUEffLowerBound(cacheInstance)).Msg("STATS")
			// ClearStats(cacheInstance)
			// log.Debug().Float64("CPU eff.",
			// 	CPUEff(cacheInstance),
			// ).Float64("CPU eff Upper Bound",
			// 	CPUEffUpperBound(cacheInstance),
			// ).Float64("CPU eff Lower Bound",
			// 	CPUEffLowerBound(cacheInstance)).Msg("STATS")
			// Update time window
			latestTime = curTime
			windowStepCounter++
		}

		if windowStepCounter == param.WindowSize {
			windowCounter++
			windowStepCounter = 0
			numDailyRecords = 0
		}
		if windowCounter == param.WindowStop {
			break
		}

		totNumRecords++

		if windowCounter >= param.WindowStart { //nolint:ignore,nestif
			// Filter records with job failure
			if !succesJobFilter.Check(record) {
				numFilteredRecords++

				continue
			}
			// Filter records with specific data type
			if param.DataTypeFilter != nil {
				if !param.DataTypeFilter.Check(record) {
					numFilteredRecords++

					continue
				}
			}
			// Filter records with specific rules
			if param.RecordFilter != nil {
				if !param.RecordFilter.Check(record) {
					numFilteredRecords++

					continue
				}
			}

			sizeInMbytes := record.SizeM // Size in Megabytes

			cpuEff := (record.CPUTime / (record.CPUTime + record.IOTime)) * 100.
			// Filter records with invalid CPU efficiency
			switch {
			case cpuEff < 0.:
				numInvalidRecords++

				continue
			case math.IsInf(cpuEff, 0):
				numInvalidRecords++

				continue
			case math.IsNaN(cpuEff):
				numInvalidRecords++

				continue
			case cpuEff > 100.:
				numInvalidRecords++

				continue
			}

			_, redirected := GetFile(
				cacheInstance,
				record.Filename,
				sizeInMbytes,
				record.Protocol,
				cpuEff,
				record.Day,
				record.SiteName,
				record.UserID,
				record.FileType,
			)

			if redirected {
				redirectedData += sizeInMbytes
				numRedirected++

				continue
			}

			numDailyRecords++

			if time.Since(start).Seconds() >= param.OutputUpdateDelay {
				elapsedTime := time.Since(simBeginTime)
				log.Info().Str("cache",
					param.BaseName).Str("elapsedTime",
					fmt.Sprintf("%02d:%02d:%02d",
						int(elapsedTime.Hours()),
						int(elapsedTime.Minutes())%60,
						int(elapsedTime.Seconds())%60)).Int("window",
					windowCounter).Int("step",
					windowStepCounter).Int("windowSize",
					param.WindowSize).Int64("numDailyRecords",
					numDailyRecords).Float64("hitRate",
					HitRate(cacheInstance)).Float64("capacity",
					Capacity(cacheInstance)).Float64("redirectedData",
					redirectedData).Int64("numRedirected",
					numRedirected).Str("extra",
					ExtraStats(cacheInstance)).Float64("it/s",
					float64(numIterations)/time.Since(start).Seconds()).Msg("Simulation")
				totIterations += numIterations
				numIterations = 0
				start = time.Now()
			}
		} else {
			numJumpedRecords++
			if time.Since(start).Seconds() >= param.OutputUpdateDelay {
				log.Info().Int64("numDailyRecords",
					numDailyRecords).Int64("numJumpedRecords",
					numJumpedRecords).Int64("numFilteredRecords",
					numFilteredRecords).Int64("numInvalidRecords",
					numInvalidRecords).Int("window",
					windowCounter).Msg("Jump records")
				start = time.Now()
			}
		}
	}

	if param.MEMprofile != "" {
		profileOut, err := os.Create(param.MEMprofile)
		if err != nil {
			log.Err(err).Str("filename", param.MEMprofile)
			panic("ERROR: on create memory profile")
		}
		log.Info().Str("filename", param.MEMprofile).Msg("Write memprofile")
		profileWriteErr := pprof.WriteHeapProfile(profileOut)
		if profileWriteErr != nil {
			panic(profileWriteErr)
		}
		profileCloseErr := profileOut.Close()
		if profileCloseErr != nil {
			panic(profileCloseErr)
		}
		return
	}

	elapsedTime := time.Since(simBeginTime)
	elTH := int(elapsedTime.Hours())
	elTM := int(elapsedTime.Minutes()) % 60
	elTS := int(elapsedTime.Seconds()) % 60
	avgSpeed := float64(totIterations) / elapsedTime.Seconds()
	log.Info().Str("elapsedTime",
		fmt.Sprintf("%02d:%02d:%02d", elTH, elTM, elTS)).Float64("avg it/s",
		avgSpeed).Int64("totRecords", totNumRecords).Int64("numJumpedRecords",
		numJumpedRecords).Int64("numFilteredRecords",
		numFilteredRecords).Int64("numInvalidRecords",
		numInvalidRecords).Msg("Simulation end...")
	// Save run statistics
	statFile, errCreateStat := os.Create(param.ResultRunStatsName)

	defer func() {
		closeErr := statFile.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	if errCreateStat != nil {
		panic(errCreateStat)
	}

	jsonBytes, errMarshal := json.Marshal(SimulationStats{
		TimeElapsed:           fmt.Sprintf("%02d:%02d:%02d", elTH, elTM, elTS),
		Extra:                 ExtraStats(cacheInstance),
		TotNumRecords:         totNumRecords,
		TotFilteredRecords:    numFilteredRecords,
		TotJumpedRecords:      numJumpedRecords,
		TotInvalidRecords:     numInvalidRecords,
		AvgSpeed:              fmt.Sprintf("Num.Records/s = %0.2f", avgSpeed),
		TotRedirectedRecords:  numRedirected,
		SizeRedirectedRecords: redirectedData,
	})
	if errMarshal != nil {
		panic(errMarshal)
	}
	_, statFileWriteErr := statFile.Write(jsonBytes)
	if statFileWriteErr != nil {
		panic(statFileWriteErr)
	}

	if param.Dump {
		Dump(cacheInstance, param.DumpFileName, param.DumpFilesAndStats)
	}

	if cacheType == "aiRL" {
		// Save tables
		log.Info().Msg("Save addition table...")
		ExtraOutput(cacheInstance, "additionQTable")
		log.Info().Msg("Save eviction table...")
		ExtraOutput(cacheInstance, "evictionQTable")
	}

	_ = Terminate(cacheInstance)

	errViperWrite := viper.WriteConfigAs("config.yaml")
	if errViperWrite != nil {
		panic(errViperWrite)
	}

	log.Info().Msg("Simulation DONE!")
}
