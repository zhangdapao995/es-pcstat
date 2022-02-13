package es_collect

import (
	"es-pcstat"
	"fmt"
	"io/ioutil"
	"path"
	"sort"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v6"

	log "github.com/sirupsen/logrus"
)

var FOUR_KB_TO_MB = 256

type Shard struct {
	indexName string
	shardId   string
	nodeName  string
	uuid      string
	primary   bool

	//MB
	pageCache      int
	fileSuffixStat FileSuffixStat
}

func (shard Shard) getShardKey() string {
	return shard.indexName + "|" + shard.shardId + "|" + shard.nodeName
}

func (shard *Shard) stats(rootPath string) {
	shardPath := shard.getShardPath(rootPath)
	files := getFiles(shardPath)
	files = fileSuffixFilter(files)
	fileSuffixStat := FileSuffixStat{}
	pcStatusList := es_pcstat.GetPcStatusFiles(files)
	cached := 0
	for _, pcStatus := range pcStatusList {
		cached += pcStatus.Cached
		fileSuffixStat.Add(getFileSuffix(pcStatus.Name), pcStatus.Cached, shard.primary)
	}
	shard.fileSuffixStat = fileSuffixStat
	shard.pageCache = cached
}

func getFiles(path string) []string {
	dirList, e := ioutil.ReadDir(path)
	files := make([]string, 0)
	if e != nil {
		fmt.Printf("read dir error, dir : %q , %v", path, e)
		return files
	}
	for _, info := range dirList {
		files = append(files, path+"/"+info.Name())
		//fmt.Println(i, "=", v.Name())
	}
	return files
}

//"" is file like SEGMENT_N
func getFileSuffix(fileName string) string {
	suffix := path.Ext(fileName)
	if strings.HasPrefix(suffix, ".") {
		suffix = string([]rune(suffix)[1:])
	}
	if suffix == "" {
		suffix = "other"
	}

	return suffix
}

func (shard Shard) getShardPath(rootPath string) string {
	return rootPath + "/" + shard.uuid + "/" + shard.shardId + "/index"
}

type Index struct {
	indexName string
	uuid      string

	priPageCache   int
	repPageCache   int
	pageCache      int //total
	fileSuffixStat FileSuffixStat
}

type ShardMap map[string]Shard

func (shardMap ShardMap) Stats(rootPath string) IndexStats {
	indexMap := IndexMap{}
	total := Index{indexName: "total", pageCache: 0, fileSuffixStat: FileSuffixStat{}, priPageCache: 0, repPageCache: 0}
	indexStats := IndexStats{indexMap: indexMap, total: total}
	for _, shard := range shardMap {
		shard.stats(rootPath)
		indexMap.addShardForStats(shard)
		// can not use total.pageCache,because total and indexStats.total are not same obj
		indexStats.total.pageCache += shard.pageCache
		if shard.primary {
			indexStats.total.priPageCache += shard.pageCache
		} else {
			indexStats.total.repPageCache += shard.pageCache
		}
		indexStats.total.fileSuffixStat.AddAll(shard.fileSuffixStat, shard.primary)
	}
	return indexStats
}

type IndexMap map[string]Index

func (indexMap IndexMap) addShardForStats(shard Shard) IndexMap {
	index, exists := indexMap[shard.indexName]
	if exists {
		index.pageCache += shard.pageCache
	} else {
		index = Index{indexName: shard.indexName, pageCache: shard.pageCache, uuid: shard.uuid, fileSuffixStat: FileSuffixStat{},
			repPageCache: 0, priPageCache: 0}
	}
	if shard.primary {
		index.priPageCache += shard.pageCache
	} else {
		index.repPageCache += shard.pageCache
	}

	index.fileSuffixStat.AddAll(shard.fileSuffixStat, shard.primary)
	indexMap[shard.indexName] = index
	return indexMap
}

type IndexStats struct {
	indexMap IndexMap
	total    Index
}

func (indexStats IndexStats) FormatForConsole(sortByCache bool) {
	indexMap := indexStats.indexMap
	total := indexStats.total
	maxName := indexMap.maxNameLen()
	fmt.Printf("max len , %d \n", maxName)
	// create horizontal grid line
	titleBlank := maxName - 9
	if titleBlank < 0 {
		titleBlank = 0
	}
	title := fmt.Sprintf("| index_name%s| cache (MB) | pri cache  | rep cache  |", strings.Repeat(" ", titleBlank))

	pad := strings.Repeat("-", maxName+2)
	top := fmt.Sprintf("+%s+------------+------------+------------+", pad)
	bot := fmt.Sprintf("+%s+------------+------------+------------+", pad)

	fmt.Println(title)
	fmt.Println(top)

	indexList := make([]Index, 0)
	for _, index := range indexMap {
		indexList = append(indexList, index)
	}

	if sortByCache {
		sort.Slice(indexList, func(i, j int) bool {
			return indexList[i].pageCache > indexList[j].pageCache
		})
	}

	// %07.3f was chosen to make it easy to scan the percentages vertically
	// I tried a few different formats only this one kept the decimals aligned
	for _, index := range indexList {
		pad = strings.Repeat(" ", maxName-len(index.indexName))

		fmt.Printf("| %s%s |  %-10d|  %-10d|  %-10d|\n",
			index.indexName, pad, index.pageCache/FOUR_KB_TO_MB, index.priPageCache/FOUR_KB_TO_MB, index.repPageCache/FOUR_KB_TO_MB)
	}

	pad = strings.Repeat(" ", maxName-len(total.indexName))
	fmt.Printf("| %s%s |  %-10d|  %-10d|  %-10d|\n",
		total.indexName, pad, total.pageCache/FOUR_KB_TO_MB, total.priPageCache/FOUR_KB_TO_MB, total.repPageCache/FOUR_KB_TO_MB)

	fmt.Println(bot)
}

func (indexStats IndexStats) FormatForSLS(clusterName string, nodeName string, createdTime time.Time) {
	docs := indexStats.getStatDocs(clusterName, nodeName, createdTime)
	formatIndexForSLS(docs)
}

func formatIndexForSLS(docs []PageCacheDoc) {
	for _, doc := range docs {
		log.WithFields(log.Fields{
			"index_name":   doc.IndexName,
			"cache":        doc.Cache,
			"primary":      doc.Primary,
			"node_name":    doc.NodeName,
			"time":         doc.Created,
			"cluster_name": doc.ClusterName,
		}).Info()
	}
}

func (indexStats *IndexStats) sort() {

}

func (indexStats IndexStats) WriteToEs(client elastic.Client, clusterName string, nodeName string, createdTime time.Time) {
	docs := indexStats.getStatDocs(clusterName, nodeName, createdTime)

	PostPcstatData(client, docs)
}

func (indexStats IndexStats) getStatDocs(clusterName string, nodeName string, createdTime time.Time) []PageCacheDoc {
	docs := make([]PageCacheDoc, 0)
	indexMap := indexStats.indexMap
	total := indexStats.total
	for _, index := range indexMap {
		doc := getPageCacheDoc(index, clusterName, nodeName, createdTime)
		docs = appendDocs(docs, doc)
	}
	doc := getPageCacheDoc(total, clusterName, nodeName, createdTime)
	docs = appendDocs(docs, doc)
	return docs
}

func appendDocs(docs []PageCacheDoc, addDocs []PageCacheDoc) []PageCacheDoc {
	for _, doc := range addDocs {
		docs = append(docs, doc)
	}
	return docs
}

func getPageCacheDoc(index Index, clusterName string, nodeName string, createdTime time.Time) []PageCacheDoc {
	docs := make([]PageCacheDoc, 0)

	docs = append(docs, getPageCacheDocWithPr(index, clusterName, nodeName, createdTime, true))
	docs = append(docs, getPageCacheDocWithPr(index, clusterName, nodeName, createdTime, false))
	return docs
}

func getPageCacheDocWithPr(index Index, clusterName string, nodeName string, createdTime time.Time, primary bool) PageCacheDoc {
	doc := PageCacheDoc{ClusterName: clusterName, NodeName: nodeName, Created: createdTime, IndexName: index.indexName,
		Primary: primary}

	cache := map[string]int{}
	indexTotalCache := 0
	for _, fileSuffixCache := range index.fileSuffixStat {
		cached := 0
		if primary {
			cached = fileSuffixCache.priPageCache
		} else {
			cached = fileSuffixCache.repPageCache
		}
		cache[fileSuffixCache.suffixName] = cached / FOUR_KB_TO_MB

		indexTotalCache += cached
	}

	cache["total"] = indexTotalCache / FOUR_KB_TO_MB
	doc.Cache = cache

	return doc
}

func (indexMap IndexMap) maxNameLen() int {
	var maxName int
	for _, index := range indexMap {
		if len(index.indexName) > maxName {
			maxName = len(index.indexName)
		}
	}

	if maxName < 5 {
		maxName = 5
	}
	return maxName
}
