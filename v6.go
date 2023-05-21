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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/util"
)

type ESAPIV6 struct {
	ESAPIV5
}

/*
这段代码用于创建一个 Elasticsearch 的 Scroll API 请求，并获取第一页结果。
这段代码和 v5.go 部分没啥区别，区别的地方就一个， *ESAPIV6
*/
func (s *ESAPIV6) NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, slicedId, maxSlicedCount int, fields string) (scroll interface{}, err error) {

	/*这行代码，用于构建 Scroll API 的请求 URL。*/
	url := fmt.Sprintf("%s/%s/_search?scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

	var jsonBody []byte
	if len(query) > 0 || maxSlicedCount > 0 || len(fields) > 0 {
		queryBody := map[string]interface{}{}

		if len(fields) > 0 {
			if !strings.Contains(fields, ",") {
				queryBody["_source"] = fields
			} else {
				queryBody["_source"] = strings.Split(fields, ",")
			}
		}

		if len(query) > 0 {
			queryBody["query"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"].(map[string]interface{})["query"] = query
		}

		if maxSlicedCount > 1 {
			log.Tracef("sliced scroll, %d of %d", slicedId, maxSlicedCount)
			queryBody["slice"] = map[string]interface{}{}
			queryBody["slice"].(map[string]interface{})["id"] = slicedId
			queryBody["slice"].(map[string]interface{})["max"] = maxSlicedCount
		}

		jsonBody, err = json.Marshal(queryBody)
		if err != nil {
			log.Error(err)

		}
	}

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
这段代码是一个使用 Elasticsearch Scroll API 请求，用于获取下一页数据。
这段代码和 v5.go 部分没啥区别，区别的地方就一个， *ESAPIV6
*/
func (s *ESAPIV6) NextScroll(scrollTime string, scrollId string) (interface{}, error) {
	id := bytes.NewBufferString(scrollId)

	url := fmt.Sprintf("%s/_search/scroll?scroll=%s&scroll_id=%s", s.Host, scrollTime, id)
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

/*和 v5 一样，区别在于ESAPIV6。*/
func (s *ESAPIV6) GetIndexSettings(indexNames string) (*Indexes, error) {
	return s.ESAPIV0.GetIndexSettings(indexNames)
}

/*和 v5 一样，区别在于ESAPIV6。*/
func (s *ESAPIV6) UpdateIndexSettings(indexName string, settings map[string]interface{}) error {
	return s.ESAPIV0.UpdateIndexSettings(indexName, settings)
}

/*
这段和 v5.go 有很大不同。
这段代码，用于获取 Elasticsearch 的索引映射。该函数接受两个参数：

	copyAllIndexes: bool数据类型，用于是否获取所有索引。
	indexNames: string数据类型，要获取映射的索引名称。
*/
func (s *ESAPIV6) GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error) {
	/*
		构建一个HTTP GET 请求给 Elasticsearch 的 _mapping API 上，并返回索引名称、索引数量和包含索引映射的 Indexes 结构体指针。
	*/
	url := fmt.Sprintf("%s/%s/_mapping", s.Host, indexNames)
	resp, body, errs := Get(url, s.Auth, s.HttpProxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return "", 0, nil, errs[0]
	}

	if resp.StatusCode != 200 {
		return "", 0, nil, errors.New(body)
	}

	idxs := Indexes{}
	er := DecodeJson(body, &idxs)

	if er != nil {
		log.Error(body)
		return "", 0, nil, er
	}

	// if _all indexes limit the list of indexes to only these that we kept
	// after looking at mappings
	/*
		这段代码用于处理 Elasticsearch 索引名称。如果 indexNames 变量的值是 _all，
	*/
	if indexNames == "_all" {

		var newIndexes []string
		/*就把所有的索引名称都加入到一个新的字符串数组，再用逗号连接它们，从而得到所有的索引名称。*/
		for name := range idxs {
			newIndexes = append(newIndexes, name)
		}
		indexNames = strings.Join(newIndexes, ",")
		/*如果 indexNames 包含 * 或 ? 通配符，那么，*/
	} else if strings.Contains(indexNames, "*") || strings.Contains(indexNames, "?") {

		/*使用 regexp.Compile 方法编译 indexNames 进行模式匹配，生成一个新的正则表达式对象 r。*/
		r, _ := regexp.Compile(indexNames)

		//check index patterns
		var newIndexes []string
		/*遍历所有索引*/
		for name := range idxs {
			/*使用 r.MatchString 方法匹配模式，找到符合模式的索引。*/
			matched := r.MatchString(name)
			if matched {
				/*将其添加到 newIndexes 列表中*/
				newIndexes = append(newIndexes, name)
			}
		}
		/*用 strings.Join 方法将其组合成以 , 分隔的字符串*/
		indexNames = strings.Join(newIndexes, ",")

	}

	/*
		将多个 Elasticsearch 索引的名称和索引映射组合在一起，并确保每个映射都包含在“mappings”键中。
		该函数的输入是一组索引名称和它们的映射，存储在 idxs 变量中。
	*/
	i := 0
	/*for 循环迭代 idxs 变量中的每个名称和映射。*/
	for name, idx := range idxs {
		i++
		/*对每个映射，检查它是否包含 mappings，如果不包含 mappings*/
		if _, ok := idx.(map[string]interface{})["mappings"]; !ok {
			/*如果没有，则将它包装在一个具有 "mappings" 键的新映射中，并将其存储回 idxs 变量中。*/
			(idxs)[name] = map[string]interface{}{
				"mappings": idx,
			}
		}
	}
	/*返回索引名称、映射的数量和 idxs 变量的指针*/
	return indexNames, i, &idxs, nil
}

/*
这段和 v5.go 有很大不同。
更新 Elasticsearch 中指定索引的映射。它接受两个参数：

	indexName: string数据类型，indexName 是要更新映射的索引的名称。
	settings: settings map[string]interface{}数据类型，包含新映射定义的 map[string]interface{}。
*/
func (s *ESAPIV6) UpdateIndexMapping(indexName string, settings map[string]interface{}) error {

	log.Debug("start update mapping: ", indexName, settings)

	/*
		在 Elasticsearch 中，dynamic_templates 是一种机制，允许在索引中自动创建字段映射，该映射由用户提供的模板控制。
	*/
	delete(settings, "dynamic_templates")

	for name, _ := range settings {

		log.Debug("start update mapping: ", indexName, ", ", settings)

		url := fmt.Sprintf("%s/%s/%s/_mapping", s.Host, indexName, name)
		
		/*
			bytes.Buffer 是 go 语言标准库中的一种类型，它实现了一个内存缓冲器，常用于构建字符串、JSON、XML 等类型的数据结构 bytes.Buffer 具有直接操作字节切片的能力，并提供了字符串拼接、追加等高效方法。

			在这行代码中，我们创建了一个 bytes.Buffer 对象，命名为 body。接下来我们将在这个缓冲器中定义一个 JSON 类型的数据。具体而言，我们将一个可转换为 JSON 数据的 `map[string]interface{}` 类型的数据结构，序列化为 JSON 格式的字符串，并将其写入 `body` 缓冲器中。这样做的目的是为了构建一个 HTTP 请求体，用于发送给 Elasticsearch REST API。
		*/
		body := bytes.Buffer{}
		enc := json.NewEncoder(&body)
		enc.Encode(settings)
		res, err := Request("POST", url, s.Auth, &body, s.HttpProxy)
		if err != nil {
			log.Error(url)
			log.Error(util.ToJson(settings, false))
			log.Error(err, res)
			panic(err)
		}
	}
	return nil
}
