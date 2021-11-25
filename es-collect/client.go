package es_collect

import (
	"context"
	"fmt"
	"gopkg.in/olivere/elastic.v6"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var PCSTAT_INDEX_NAME = "pc_stat"
var KEEP_INDEX_NUM = 5

var mapping = `
	{
		"settings":{
			"number_of_replicas":0
		},
		"mappings":{
			"_doc":{
				"properties":{
					"cluster_name" : {
						"type" : "keyword"
					  },
					  "created" : {
						"type" : "date"
					  },
					  "index_name" : {
						"type" : "keyword"
					  },
					  "node_name" : {
						"type" : "keyword"
					  }
				}
			}
		}
	}
`

//es client for get shards or indices and more
type Client struct {
	Ip   string
	Port string

	client elastic.Client
}

func NewClient(ip string, port string, initEsClient bool) *Client {
	instance := new(Client)
	instance.Ip = ip
	instance.Port = port
	if initEsClient {
		url := "http://" + ip + ":" + port + "/"
		client, err := elastic.NewClient(
			elastic.SetURL(url),
			elastic.SetSniff(false),
			elastic.SetHealthcheckInterval(10*time.Second),
			elastic.SetGzip(true),
			elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
			elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)))
		if err != nil {
			panic(err)
		}

		instance.client = *client
	}
	return instance
}

func GetShardMap(client Client) ShardMap {
	url := "http://" + client.Ip + ":" + client.Port + "/_cat/shards?h=state,index,shard,node,prirep"
	body, err := httpGetRequest(url)
	if err != nil {
		log.Printf("get indices error,%v", err)
	}
	lines := splitWithoutNull(body, "\n")

	shardMap := ShardMap{}
	for _, shardStr := range lines {
		columns := splitWithoutNull(shardStr, " ")
		state := columns[0]
		if strings.Compare(state, "STARTED") != 0 {
			continue
		}
		indexName := columns[1]
		nodeName := columns[3]
		shardId := columns[2]
		prirep := columns[4]
		primary := false
		if prirep == "p" {
			primary = true
		}
		shard := Shard{indexName: indexName, nodeName: nodeName, shardId: shardId, primary: primary}
		shardKey := shard.getShardKey()
		shardMap[shardKey] = shard
		//log.Printf("%s , %s ,%s",columns[0], columns[1] ,columns[2])
	}
	return shardMap
}

func GetIndiceMap(client Client, indicesPrefix []string) IndexMap {
	url := "http://" + client.Ip + ":" + client.Port + "/_cat/indices?h=index,uuid"
	body, err := httpGetRequest(url)
	if err != nil {
		log.Printf("get indices error,%v", err)
	}
	lines := splitWithoutNull(body, "\n")

	indexMap := IndexMap{}
	for _, indexStr := range lines {
		columns := splitWithoutNull(indexStr, " ")
		indexName := columns[0]
		uuid := columns[1]
		if checkInIndices(indexName, indicesPrefix) {
			indexMap[indexName] = Index{indexName: indexName, uuid: uuid}
		}
		//log.Printf("%s , %s",columns[0], columns[1])
	}
	return indexMap
}

func checkInIndices(index string, indicesPrefix []string) bool {
	for _, prefixStr := range indicesPrefix {
		if strings.HasPrefix(index, prefixStr) {
			return true
		}
	}
	return false
}

func FillShardMap(shardMap ShardMap, indexMap IndexMap) ShardMap {
	return FillShardMapFilterNode(shardMap, indexMap, "")
}

func FillShardMapFilterNode(shardMap ShardMap, indexMap IndexMap, nodeName string) ShardMap {
	for key, shard := range shardMap {
		index, exist := indexMap[shard.indexName]
		if (nodeName != "" && strings.Compare(shard.nodeName, nodeName) != 0) || !exist {
			if !exist {
				//log.Printf("skip fill Shard uuid,can't get index, shard:%s", shard.getShardKey())
			}
			delete(shardMap, shard.getShardKey())
			continue
		}
		shard.uuid = index.uuid
		shardMap[key] = shard
	}
	return shardMap
}

//split and skip empty String ""
func splitWithoutNull(s, sep string) []string {
	strs := strings.Split(s, sep)
	res := make([]string, 0)
	for _, str := range strs {
		if strings.Compare(str, "") != 0 {
			res = append(res, str)
		}
	}
	return res
}

func initPcstatIndex(esClient elastic.Client, indexPrefix string) (string, error) {
	//create index if not exist
	realIndex := indexPrefix + "-" + time.Now().Format("2006_01_02")
	exist, err := esClient.IndexExists(realIndex).Do(context.TODO())
	if err != nil {
		fmt.Errorf("check index exists error,index_name: %s, %s", realIndex, err)
	}
	if !exist {
		createIndex, err := esClient.CreateIndex(realIndex).Body(mapping).IncludeTypeName(true).Do(context.TODO())
		if err != nil || createIndex == nil || !createIndex.Acknowledged {
			fmt.Errorf("create index error error, index_name: %s, %s", realIndex, err)
			return realIndex, &elastic.Error{Status: 500}
		}
	}

	//detele index if exist
	dayBefore := time.Now().AddDate(0, 0, -KEEP_INDEX_NUM-1)
	toDeleteIndex := indexPrefix + "-" + dayBefore.Format("2006_01_02")
	deleteExist, _ := esClient.IndexExists(toDeleteIndex).Do(context.TODO())
	if deleteExist {
		deleteIndex, error := esClient.DeleteIndex(toDeleteIndex).Do(context.TODO())
		if error != nil || deleteIndex == nil || !deleteIndex.Acknowledged {
			fmt.Errorf("delete index error error, index_name: %s, %s", toDeleteIndex, error)
		}
	}
	return realIndex, nil
}

func PostPcstatData(client Client, docs []PageCacheDoc) {
	esClient := client.client
	indexName, error := initPcstatIndex(esClient, PCSTAT_INDEX_NAME)
	if error != nil {
		fmt.Printf("create index error, skip bulk data, %s\n", error)
		return
	}

	bulkRequest := esClient.Bulk()
	for _, doc := range docs {
		indexReq := elastic.NewBulkIndexRequest().Index(indexName).Type("_doc").Doc(doc)
		bulkRequest = bulkRequest.Add(indexReq)
	}

	bulkResponse, err := bulkRequest.Do(context.TODO())
	if bulkResponse == nil {
		fmt.Errorf("expected bulkResponse to be != nil; got nil")
		return
	}
	if err != nil {
		fmt.Errorf("bulk es data error, %s", err)
	}
	if bulkResponse.Errors {
		fmt.Errorf("bulk error")
		for _, typeItem := range bulkResponse.Items {
			for _, item := range typeItem {
				fmt.Errorf(item.Error.Reason)
			}
		}
	}
}

func httpGetRequest(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		return "", err
	}
	return string(body), nil
}

type PageCacheDoc struct {
	Cache       map[string]int `json:"cache"`
	Primary     bool           `json:"primary"`
	ClusterName string         `json:"cluster_name"`
	NodeName    string         `json:"node_name"`
	IndexName   string         `json:"index_name"`
	Created     time.Time      `json:"created,omitempty"`
}
