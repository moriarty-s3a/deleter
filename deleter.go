package main

import (
	"encoding/json"
	"io/ioutil"
	"flag"
	log "github.com/Sirupsen/logrus"
	"time"
	"path/filepath"
	"os"
	"strings"
	"strconv"
	"sync"
)

func main() {
	var baseDir, logLevel string
	flag.StringVar(&baseDir, "baseDir", "/tmp/foo", "service name")
	flag.StringVar(&logLevel, "level", "debug", "Logging level")
	flag.Parse()
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Fatal("Invalid Logging Level")
		return
	}
	log.SetLevel(level)

	config := readConfig()
	log.Debugln("Config= ", config)
	configMap := convertConfigToMap(config)
	currTime := time.Now().UTC()
	companyDirs, err := ioutil.ReadDir(baseDir)
	if err != nil {
		// Not much we can do if we can't read the base directory. Something went very wrong.
		log.Fatal("Could not open base directory.", err)
	}
	var wg sync.WaitGroup
	for _, entry := range companyDirs {
		if entry.IsDir() {
			companyConfig, exists := configMap[entry.Name()]
			if !exists {
				companyConfig = configMap["default"]
			}
			log.Debugln("Config = ", companyConfig)
			wg.Add(1)
			go pruneSingleCompanyDir(filepath.Join(baseDir, entry.Name()), companyConfig, currTime, &wg)
		}
	}
	wg.Wait()
}

func pruneSingleCompanyDir(fileName string, config CompanyConfig, currTime time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	retentionDays, retentionErr := strconv.ParseInt(config.Retention, 10, 0)
	if retentionErr != nil {
		log.Errorf("Error, retention time [%s] for company %s [%s] is not a number.", config.Retention, config.Name, config.Id)
		return
	}
	deleteTime := currTime.AddDate(0, 0, -1 * int(retentionDays))
	baseLen := len(strings.Split(fileName, string(os.PathSeparator)))
	err := filepath.Walk(fileName, func(path string, f os.FileInfo, err error) error {
		log.Println("Walk found: " + path)
		if err != nil {
			// Ignore errors so that we do as much work as possible.
			log.Errorf("Error in path %s  : %+v", path, err)
			return nil
		}
		// I assume that any stray files in non-leaf directories should be left alone?
		if !f.IsDir() {
			return nil
		}
		compareDate := getCompareDate(path, baseLen)
		log.Debugf("DirTime = %s   DeleteTime = %s\n", compareDate.String(), deleteTime.String())
		if compareDate.Before(deleteTime) {
			log.Debugln("Removing " + path)
			removeErr := os.RemoveAll(path)
			if removeErr != nil {
				log.Debugf("Error removing path %s  : %+v\n", path, removeErr)
			}
		}
		return nil
	})
	if err != nil {
		log.Errorln("Error walking path" + fileName, err)
	}
}

func getCompareDate(path string, baseLen int) time.Time {
	pathArray := strings.Split(path, string(os.PathSeparator))
	pathLen := len(pathArray)

	// If the directory structure is incomplete, build as much as we can and choose the last second of that interval.
	// This will avoid having to individually delete multiple directories that would have all expired.
	if pathLen < baseLen + 2 {
		return time.Now()
	}
	year := getDatePiece(pathArray, baseLen, 1)
	if pathLen < baseLen + 3 {
		return time.Date(year + 1, 0, 0, 0, 0, 0, 0, time.Local).Add(-1 * time.Second)
	}
	month := getDatePiece(pathArray, baseLen, 2)
	if pathLen < baseLen + 4 {
		return time.Date(year, time.Month(month + 1), 0, 0, 0, 0, 0, time.Local).Add(-1 * time.Second)
	}
	day := getDatePiece(pathArray, baseLen, 3)
	if pathLen < baseLen + 5 {
		return time.Date(year, time.Month(month), day + 1, 0, 0, 0, 0, time.Local).Add(-1 * time.Second)
	}
	hour := getDatePiece(pathArray, baseLen, 4)
	if pathLen < baseLen + 6 {
		return time.Date(year, time.Month(month), day, hour + 1, 0, 0, 0, time.Local).Add(-1 * time.Second)
	}
	min := getDatePiece(pathArray, baseLen, 5)
	return time.Date(year, time.Month(month), day, hour, min + 1, 0, 0, time.Local).Add(-1 * time.Second)
}

func getDatePiece(pathArray [] string, baseLen int, idx int) int {
	dirAge, err := strconv.ParseInt(pathArray[baseLen + idx], 10, 0)
	if err == nil {
		return int(dirAge)
	}
	return 0
}

func readConfig() Config {
	configFile, err := ioutil.ReadFile("resources/config.json")
	if err != nil {
		// Not much we can do if we can't read the configuration.
		// In a production environment, this should periodically reread its configuration from a database
		// or at least listen for SIGHUP and reread the config file.
		log.Fatal("Could not open config.", err)
	}
	var config Config
	json.Unmarshal(configFile, &config)
	return config
}

func convertConfigToMap(config Config) map[string]CompanyConfig {
	configMap := make(map[string]CompanyConfig)
	configMap["default"] = config.DefaultConfig
	for _, entry := range config.CompanyConfigs {
		configMap[entry.Id] = entry
	}
	return configMap
}

type Config struct {
	DefaultConfig CompanyConfig `json:"default"`
	CompanyConfigs []CompanyConfig `json:"companies"`
}

type CompanyConfig struct {
	Id string `json:"companyId"`
	Name string `json:"companyName"`
	Retention string `json:"retentionDays"`
}