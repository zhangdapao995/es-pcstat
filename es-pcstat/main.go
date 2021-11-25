package main

/*
 * Copyright 2014-2017 A. Tobey <tobert@gmail.com> @AlTobey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * es-pcstat.go - page cache stat
 *
 * uses the mincore(2) syscall to find out which pages (almost always 4k)
 * of a file are currently cached in memory
 *
 */

import (
	"bufio"
	"es-pcstat/es-collect"
	"flag"
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	collectIntervalFlag int
	outputTypeFlag      string
	sortFlag            bool
)

func init() {
	// TODO: error on useless/broken combinations
	flag.IntVar(&collectIntervalFlag, "collectIntervalFlag", 60, "the interval between collect")
	flag.StringVar(&outputTypeFlag, "outputTypeFlag", "console", "output ,choose in [es, log, console]")
	flag.BoolVar(&sortFlag, "sortFlag", false, "sort by cache desc")

}

const (
	ES_IP_FIELD           = "es.ip"
	ES_PORT_FIELD         = "es.port"
	ES_INDICES_PATH_FIELD = "es.indicesPath"
	ES_NODE_NAME_FIELD    = "es.nodeName"
	ES_CLUSTER_NAME       = "es.clusterName"

	ES_COLLECTION_INDICES_PREFIX_FIELD = "es.collection.indicesPrefix"

	OUTPUT_LOG_KEEP_LOG_NUM_FIELD = "output.log.keepLogNum"
	OUTPUT_LOG_LOG_PATH_FIELD     = "output.log.logPath"

	OUTPUT_ES_KEEP_INDEX_NUM_FIELD = "output.es.keepIndexNum"
	OUTPUT_ES_PC_INDEX_NAME        = "output.es.pcIndexName"
)

// init log
func initLog(path string, keepLogNum int) {
	/* 日志轮转相关函数
	`WithLinkName` 为最新的日志建立软连接
	`WithRotationTime` 设置日志分割的时间，隔多久分割一次
	WithMaxAge 和 WithRotationCount二者只能设置一个
	 `WithMaxAge` 设置文件清理前的最长保存时间
	 `WithRotationCount` 设置文件清理前最多保存的个数
	*/
	if outputTypeFlag == LOG {
		writer, _ := rotatelogs.New(
			path+".%Y%m%d",
			rotatelogs.WithLinkName(path),
			rotatelogs.WithMaxAge(time.Duration(24*keepLogNum)*time.Hour),
			rotatelogs.WithRotationTime(time.Duration(24)*time.Hour),
		)
		log.SetOutput(writer)
		log.SetFormatter(&log.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05"})
	}
}

func main() {
	flag.Parse()
	files := flag.Args()
	config := initConfig(files[0])
	client := initEsClient(config[ES_IP_FIELD], config[ES_PORT_FIELD], outputTypeFlag == ES)
	nodeName := config[ES_NODE_NAME_FIELD]
	path := config[ES_INDICES_PATH_FIELD]
	clusterName := config[ES_CLUSTER_NAME]
	indicesPrefix := strings.Split(config[ES_COLLECTION_INDICES_PREFIX_FIELD], ",")
	if outputTypeFlag == LOG {
		logPath := config[OUTPUT_LOG_LOG_PATH_FIELD]
		keepLogNum, err := strconv.Atoi(config[OUTPUT_LOG_KEEP_LOG_NUM_FIELD])
		if err != nil {
			fmt.Printf("keepLogNum can't conv to integer,%s", err)
			keepLogNum = 5
		}
		initLog(logPath, keepLogNum)
	}
	if outputTypeFlag == ES {
		keepIndexNum, err := strconv.Atoi(config[OUTPUT_ES_KEEP_INDEX_NUM_FIELD])
		if err != nil {
			fmt.Printf("keepIndexNum can't conv to integer,%s", err)
			keepIndexNum = 5
		}
		es_collect.KEEP_INDEX_NUM = keepIndexNum
		es_collect.PCSTAT_INDEX_NAME = config[OUTPUT_ES_PC_INDEX_NAME]
	}

	for {
		collectStart := time.Now()
		fmt.Printf("start collect time, %s\n", collectStart)
		indexMap := es_collect.GetIndiceMap(client, indicesPrefix)
		shardMap := es_collect.GetShardMap(client)
		shardMap = es_collect.FillShardMapFilterNode(shardMap, indexMap, nodeName)
		indexStats := shardMap.Stats(path)

		if outputTypeFlag == ES {
			indexStats.WriteToEs(client, clusterName, nodeName, collectStart)
		} else if outputTypeFlag == LOG {
			indexStats.FormatForSLS(clusterName, nodeName, collectStart)
		} else if outputTypeFlag == CONSOLE {
			indexStats.FormatForConsole(sortFlag)
		}

		waitToNextCollect(collectStart, collectIntervalFlag)
	}
}

func waitToNextCollect(collectStart time.Time, collectIntervalFlag int) {
	nextTime := collectStart.Add(time.Duration(collectIntervalFlag) * time.Second)
	for {
		if nextTime.After(time.Now()) {
			duration := nextTime.Sub(time.Now())
			fmt.Printf("currnet time to sleep, %s\n", time.Now())
			fmt.Printf("wait for next collect, sleep %s seconds\n", duration)
			fmt.Printf("next time collect time, %s\n", nextTime)
			time.Sleep(duration)
			break
		} else {
			nextTime = nextTime.Add(time.Duration(collectIntervalFlag) * time.Second)
		}
	}
}

func initEsClient(ip string, port string, initEsClient bool) es_collect.Client {
	client := es_collect.NewClient(ip, port, initEsClient)
	return *client
}

func initConfig(path string) map[string]string {
	config := make(map[string]string)

	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		s := strings.TrimSpace(string(b))
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}
		key := strings.TrimSpace(s[:index])
		if len(key) == 0 {
			continue
		}
		value := strings.TrimSpace(s[index+1:])
		if len(value) == 0 {
			continue
		}
		config[key] = value
	}
	return config
}

func getPidMaps(pid int) []string {
	fname := fmt.Sprintf("/proc/%d/maps", pid)

	f, err := os.Open(fname)
	if err != nil {
		log.Fatalf("could not open '%s' for read: %v", fname, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	// use a map to help avoid duplicates
	maps := make(map[string]bool)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 6 && strings.HasPrefix(parts[5], "/") {
			// found something that looks like a file
			maps[parts[5]] = true
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("reading '%s' failed: %s", fname, err)
	}

	// convert back to a list
	out := make([]string, 0, len(maps))
	for key := range maps {
		out = append(out, key)
	}

	return out
}
