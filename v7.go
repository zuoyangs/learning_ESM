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
)

type ESAPIV7 struct {
	ESAPIV6
}

/**/
func (s *ESAPIV7) NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, slicedId, maxSlicedCount int, fields string) (scroll interface{}, err error) {
	url := fmt.Sprintf("%s/%s/_search?scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

	/*这里和 v5，v6都不同，前2者都是 var jsonBody []byte */
	jsonBody := ""
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
		/*和 v5.go v6.go相比较，5和6的这部分代码：jsonBody, err = json.Marshal(queryBody)*/
		jsonArray, err := json.Marshal(queryBody)
		if err != nil {
			log.Error(err)

		} else {
			jsonBody = string(jsonArray)
		}
	}

	/*
		v5.go v6.go:
			body, err := DoRequest(s.Compress,"POST",url, s.Auth,jsonBody,s.HttpProxy)
	*/
	resp, body, errs := Post(url, s.Auth, jsonBody, s.HttpProxy)

	/* 和 v5.go v6.go 相比较，这段是新增的，v7新增这部分的处理更好。*/
	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs[0]
	}

	/* 和 v5.go v6.go 相比较，这段是新增的，v7新增这部分的处理更好。*/
	if resp.StatusCode != 200 {
		return nil, errors.New(body)
	}

	/* 和 v5.go v6.go 相比较，这段是新增的，v7新增这部分的处理更好。*/
	log.Trace("new scroll,", body)

	if err != nil {
		log.Error(err)
		return nil, err
	}

	/*v5.go v6.go scroll = &Scroll{}, v7.go 不同*/
	scroll = &ScrollV7{}
	err = DecodeJson(body, scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, err
}

/*
这段代码用于滚动查询。它接收2个参数，这里不重复介绍了，和 v5.go v6.go 一样。
*/
func (s *ESAPIV7) NextScroll(scrollTime string, scrollId string) (interface{}, error) {
	id := bytes.NewBufferString(scrollId)

	url := fmt.Sprintf("%s/_search/scroll?scroll=%s&scroll_id=%s", s.Host, scrollTime, id)
	body, err := DoRequest(s.Compress, "GET", url, s.Auth, nil, s.HttpProxy)

	/*相比 v5.go v6.go，v7.go 在这里多一次判断。*/
	if err != nil {
		//log.Error(errs)
		return nil, err
	}
	// decode elasticsearch scroll response
	/*v5.go，v6.go scroll := &Scroll{}*/
	scroll := &ScrollV7{}
	err = DecodeJson(body, &scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}

func (s *ESAPIV7) GetIndexSettings(indexNames string) (*Indexes, error) {
	return s.ESAPIV0.GetIndexSettings(indexNames)
}

func (s *ESAPIV7) UpdateIndexSettings(indexName string, settings map[string]interface{}) error {
	return s.ESAPIV0.UpdateIndexSettings(indexName, settings)
}

/*和v6.go 没什么不同，看不出重写的意义。v5.go 是 v0.go，也看不出 v0.go 和 v7.go 区别*/
func (s *ESAPIV7) GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error) {
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
	if indexNames == "_all" {

		var newIndexes []string
		for name := range idxs {
			newIndexes = append(newIndexes, name)
		}
		indexNames = strings.Join(newIndexes, ",")

	} else if strings.Contains(indexNames, "*") || strings.Contains(indexNames, "?") {

		r, _ := regexp.Compile(indexNames)

		//check index patterns
		var newIndexes []string
		for name := range idxs {
			matched := r.MatchString(name)
			if matched {
				newIndexes = append(newIndexes, name)
			}
		}
		indexNames = strings.Join(newIndexes, ",")

	}

	i := 0
	// wrap in mappings if moving from super old es
	for name, idx := range idxs {
		i++
		fmt.Println(name)
		if _, ok := idx.(map[string]interface{})["mappings"]; !ok {
			(idxs)[name] = map[string]interface{}{
				"mappings": idx,
			}
		}
	}

	return indexNames, i, &idxs, nil
}

/*更新 Elasticsearch 中索引的映射(mapping)*/
func (s *ESAPIV7) UpdateIndexMapping(indexName string, settings map[string]interface{}) error {

	log.Debug("start update mapping: ", indexName, settings)

	/*
		v7.go 和 v6.go 在这里是相同的。v0.go 和 v5.go 是相同的，但是与 v7.go 和 v6.go不同，没有这行delete代码
	*/
	delete(settings, "dynamic_templates")

	/*
		v7.go 在这块的实现和 v0.go v5.go v6.go 均不同，后者是放在 for name, _ := range settings {} 中。
		v7.go 为啥要注释掉for循环呀？
	*/
	//for name, mapping := range settings {
	log.Debug("start update mapping: ", indexName, ", ", settings)

	/*
		用于管理和操作索引的api，可以创建、更新、获取索引映射(mapping)
		这段 v0.go 和 v5.go 是一样的，v6.go 和 v7.go 是一样的。
		v0.go：
			url := fmt.Sprintf("%s/%s/%s/_mapping", s.Host, indexName, name)
	*/
	url := fmt.Sprintf("%s/%s/_mapping", s.Host, indexName)

	body := bytes.Buffer{}
	enc := json.NewEncoder(&body)
	enc.Encode(settings)
	res, err := Request("POST", url, s.Auth, &body, s.HttpProxy)
	if err != nil {
		log.Error(url)
		log.Error(body.String())
		log.Error(err, res)
		panic(err)
	}
	//}
	return nil
}
