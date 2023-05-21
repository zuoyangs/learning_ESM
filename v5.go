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

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/cihub/seelog"
)

type ESAPIV5 struct {
	ESAPIV0
}

func (s *ESAPIV5) ClusterHealth() *ClusterHealth {
	return s.ESAPIV0.ClusterHealth()
}

func (s *ESAPIV5) Bulk(data *bytes.Buffer) {
	s.ESAPIV0.Bulk(data)
}

func (s *ESAPIV5) GetIndexSettings(indexNames string) (*Indexes, error) {
	return s.ESAPIV0.GetIndexSettings(indexNames)
}

func (s *ESAPIV5) UpdateIndexSettings(indexName string, settings map[string]interface{}) error {
	return s.ESAPIV0.UpdateIndexSettings(indexName, settings)
}

func (s *ESAPIV5) GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error) {
	return s.ESAPIV0.GetIndexMappings(copyAllIndexes, indexNames)
}

func (s *ESAPIV5) UpdateIndexMapping(indexName string, settings map[string]interface{}) error {
	return s.ESAPIV0.UpdateIndexMapping(indexName, settings)
}

func (s *ESAPIV5) DeleteIndex(name string) (err error) {
	return s.ESAPIV0.DeleteIndex(name)
}

func (s *ESAPIV5) CreateIndex(name string, settings map[string]interface{}) (err error) {
	return s.ESAPIV0.CreateIndex(name, settings)
}

func (s *ESAPIV5) Refresh(name string) (err error) {
	return s.ESAPIV0.Refresh(name)
}

/*这段代码用于创建 Elasticsearch 的一个 Scroll API 请求。*/
/*
	这段代码的功能是用于创建一个 Elasticsearch 的 Scroll API 请求。返回 scroll 接口和错误类型 error。
		indexNames: string数据类型，用于表示索引名称。
		scrollTime: string数据类型，用于表示滚动时间。
		docBufferCount: int数据类型，文档缓冲区大小。
		query: string数据类型，用于表示查询。
		slicedId: int数据类型，用于表示切片ID。
		maxSlicedCount: int数据类型，用于表示最大切片数。
		fields: string数据类型，用于表示字段。

*/
func (s *ESAPIV5) NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, slicedId, maxSlicedCount int, fields string) (scroll interface{}, err error) {

	/*这段代码用来构建 Elasticsearch Scroll API 的 URL 的。*/
	url := fmt.Sprintf("%s/%s/_search?scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

	var jsonBody []byte

	/*判断是否有查询条件，是否有限制返回数据的数量，是否有需要返回的字段*/
	if len(query) > 0 || maxSlicedCount > 0 || len(fields) > 0 {
		queryBody := map[string]interface{}{}

		/*返回字段*/
		if len(fields) > 0 {
			/*如果 fields 是不包含由逗号分隔的字符串，它将被设置为 _source 查询参数的值*/
			if !strings.Contains(fields, ",") {
				queryBody["_source"] = fields
			} else {
				/*否则，字段名是一个由逗号分隔的字符串，则它将拆分成一个字符串切片，并赋值给 _source*/
				queryBody["_source"] = strings.Split(fields, ",")
			}
		}
		/*处理用户的查询参数*/
		if len(query) > 0 {
			/*如果用户设置了查询参数，则创建一个 queryBody["query"] 空 map类型变量*/
			queryBody["query"] = map[string]interface{}{}
			/*queryBody["query"] 的值设置为一个新的 query_string Map 类型变量*/
			queryBody["query"].(map[string]interface{})["query_string"] = map[string]interface{}{}
			/*并将用户提供的查询字符串设置为 queryBody["query"]["query_string"]["query"] 的值*/
			queryBody["query"].(map[string]interface{})["query_string"].(map[string]interface{})["query"] = query
		}

		/*使用 Scroll API 进行分片查询。当数据量较大的时候，es通常需要对数据进行分片处理以提高查询效率。*/
		if maxSlicedCount > 1 {

			/*
				log.Tracef 和 log.Infof 是日志记录的两个方法，它们之间的区别如下：
					log.Tracef 用于输出跟踪信息级别 (trace)，通常用于输出程序中非常详细的调试信息。对于一些需要追踪的操作，可以使用该方法输出相关的日志信息，并通过查看日志文件来分析问题所在。
					log.Infof 用于输出信息级别 (info)，通常用于输出程序的运行状态信息。当程序需要输出运行状态信息时，可以使用该方法输出相关的日志信息，并通过查看日志文件来了解程序的运行状况。
				下面这段代码，通过 log 对象输出一条日志，用来记录当前分片的ID和总分片数。
			*/
			log.Tracef("sliced scroll, %d of %d", slicedId, maxSlicedCount)
			queryBody["slice"] = map[string]interface{}{}
			queryBody["slice"].(map[string]interface{})["id"] = slicedId
			queryBody["slice"].(map[string]interface{})["max"] = maxSlicedCount
		}

		/*这段代码是将一个结构体(queryBody)转换为 json 字符串格式(jsonBody)*/
		jsonBody, err = json.Marshal(queryBody)
		if err != nil {
			log.Error(err)
		}
	}

	/*
		这段代码是使用 "DoRequest" 方法来执行 HTTP 请求并发送 JSON 格式的请求正文（即"jsonBody"）。
		如果在发送请求时出现错误，将返回 "nil" 并将错误信息记录在日志中。
	*/
	body, err := DoRequest(s.Compress, "POST", url, s.Auth, jsonBody, s.HttpProxy)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	scroll = &Scroll{}
	err = DecodeJson(body, scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, err
}

/*
这段代码的作用是获取 Elasticsearch 的 Scroll API 的下一页结果。
scrollTime: string数据类型，表示查询的有效时间。
scrollID: string数据类型，表示前一个 scroll 的返回结果中包含的 ID。
*/
func (s *ESAPIV5) NextScroll(scrollTime string, scrollId string) (interface{}, error) {

	id := bytes.NewBufferString(scrollId)
	/*根据传入的 scrollTime 和 scrollID 构造请求 URL。*/
	url := fmt.Sprintf("%s/_search/scroll?scroll=%s&scroll_id=%s", s.Host, scrollTime, id)

	/*调用 DoRequest 方法发起 HTTP GET 请求。*/
	body, err := DoRequest(s.Compress, "GET", url, s.Auth, nil, s.HttpProxy)

	// decode elasticsearch scroll response
	scroll := &Scroll{}
	err = DecodeJson(body, &scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}
