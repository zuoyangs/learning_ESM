/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import "bytes"

/*
定义了一些常用的 API 的 接口
*/
type ESAPI interface {

	/*获取 Elasticsearch 集群的健康状态*/
	ClusterHealth() *ClusterHealth
	/*批量插入、更新或删除文档*/
	Bulk(data *bytes.Buffer)
	/*获取索引的设置信息*/
	GetIndexSettings(indexNames string) (*Indexes, error)
	/*删除索引*/
	DeleteIndex(name string) error
	/*创建索引*/
	CreateIndex(name string, settings map[string]interface{}) error
	/*获取一个或多个索引的映射信息*/
	GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error)
	/*更新索引的设置信息*/
	UpdateIndexSettings(indexName string, settings map[string]interface{}) error
	/*更新索引的映射信息*/
	UpdateIndexMapping(indexName string, mappings map[string]interface{}) error
	/*滚动搜索，用于读取大批量数据*/
	NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, slicedId, maxSlicedCount int, fields string) (interface{}, error)
	/*获取下一批滚动搜索结果*/
	NextScroll(scrollTime string, scrollId string) (interface{}, error)
	/*刷新一个或多个索引的缓存*/
	Refresh(name string) (err error)
}
