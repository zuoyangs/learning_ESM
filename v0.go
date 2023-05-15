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

type ESAPIV0 struct {
	Host      string //eg: http://localhost:9200
	Auth      *Auth  //eg: user:pass
	HttpProxy string //eg: http://proxyIp:proxyPort
	Compress  bool
}

/*获取 Elasticsearch 集群的健康状况。*/
func (s *ESAPIV0) ClusterHealth() *ClusterHealth {

	/*使用 fmt.Sprintf() 函数构造了请求 URL，并调用了 Get() 方法发起了 GET 请求。*/
	url := fmt.Sprintf("%s/_cluster/health", s.Host)
	/*Get() 方法返回的 r、body 和 errs 变量分别代表 HTTP 响应、响应体和请求错误。*/
	r, body, errs := Get(url, s.Auth, s.HttpProxy)

	if r != nil && r.Body != nil {
		//如果 r 的值不为空，同时 r.Body 也不为空，则说明请求成功,并通过 io.Copy()函数把 r.Body 中的数据流扔到 ioutil.Discard 中丢弃。
		//ioutil.Discard 是一个 Go 语言标准库中的变量，它是一个 io.Writer 接口的实现，可以用于写入任何数据但会直接丢弃它们。
		//这个变量通常在执行某些操作时不需要输出结果时使用，例如在日志库中，当日志级别低于某个阈值时，可以将日志输出重定向到 ioutil.Discard，这样就可以避免不必要的输出
		//io.Copy() 是一个 Go 语言标准库中的函数，用于复制一个 Reader 的内容到一个 Writer 中，它的函数签名如下： func Copy(dst Writer, src Reader) (written int64, err error)
		io.Copy(ioutil.Discard, r.Body)
		defer r.Body.Close()
	}

	/*判断 errs 变量是否为空，如果存在请求错误，则返回一个 ClusterHealth 对象，将集群状态设置为 "unreachable"。*/
	if errs != nil {
		return &ClusterHealth{Name: s.Host, Status: "unreachable"}
	}

	log.Debug(url)
	log.Debug(body)

	health := &ClusterHealth{}
	/*
	   使用 json.Unmarshal() 函数解析 HTTP 响应体，将响应体中的数据解析为一个 ClusterHealth struct对象。
	*/
	err := json.Unmarshal([]byte(body), health)

	if err != nil {
		log.Error(body)
		return &                                                                                                                                                                                                  {Name: s.Host, Status: "unreachable"}
	}
	return health
}

/*
	这段代码是一个名为 Bulk 的函数，它是 ESAPIV0 结构体的一个方法。
	该方法接受一个 *bytes.Buffer 类型的参数
*/
func (s *ESAPIV0) Bulk(data *bytes.Buffer) {
	/*如果该参数为空或长度为0，则跳过该函数*/
	if data == nil || data.Len() == 0 {
		log.Trace("data is empty, skip")
		return
	}
	/*
		否则，该函数会在数据末尾写入一个换行符，并将请求发送到 _bulk 端点。
		WriteRune 方法是 Go 语言标准库中的一个方法，其作用是将单个 Unicode 字符写入w中，返回写入的字符的字节数和错误。
		\n 是换行符的 Unicode 码点，这行代码的作用是往数据流中加入一个换行符，使得输出结果更加易于阅读。
	*/
	data.WriteRune('\n')
	url := fmt.Sprintf("%s/_bulk", s.Host)

	/*向 elasticsearch 服务器构造一个 _bulk 请求的 URL*/
	body, err := DoRequest(s.Compress, "POST", url, s.Auth, data.Bytes(), s.HttpProxy)

	if err != nil {
		log.Error(err)
		return
	}
	/*BulkResponse{} 是一个结构体，是用于解析 Elasticsearch 返回的 bulk API 的响应的*/
	response := BulkResponse{}
	err = DecodeJson(body, &response)
	/*如果请求成功，*/
	if err == nil {
		/*则会解码响应并检查是否存在错误*/
		if response.Errors {
			/*如果存在错误，则会打印响应体*/
			fmt.Println(body)
		}
	}
	/*最后，该函数会重置数据缓冲区。*/
	data.Reset()
}

/*
	这段代码定义了一个名为 GetIndexSettings 的方法, 它是结构体 ESAPIV0 类型的指针方法.
	该方法接受一个字符串参数 indexNames ，并返回一个指向 Indexes 结构体的指针和一个err类型的变量。

	该方法的主要目的是获取指定索引名称的所有设置。
*/
func (s *ESAPIV0) GetIndexSettings(indexNames string) (*Indexes, error) {

	/*
		首先 Indexes{} 是一个空的 map[string]interface{} 类型的map，类似python中的字典。
		allSettings := &Indexes{} 则创建了一个指向 Indexes 类型的指针。
		由于 Indexes 本身就是一个 map 类型，所以这里的指针其实就是指向了一个空的 map[string]interface{} 类型的 map。
		所以说它是创建了一个指向 Indexes 结构体的指针 allSettings，用于存储获取到的设置。
	*/
	allSettings := &Indexes{}

	/* 
		fmt.Sprintf 函数构造字符串的一种常见方式。
		方法使用 fmt.Sprintf 函数构造了一个 URL，该 URL 指向 Elasticsearch 集群中的 /_settings 端点。
	*/
	url := fmt.Sprintf("%s/%s/_settings", s.Host, indexNames)
	/*然后，该方法调用 Get 函数，向该 URL 发送 GET 请求，并获取响应。*/
	resp, body, errs := Get(url, s.Auth, s.HttpProxy)

	/*在获取应用后，该方法首先检查响应是否为 nil*/
	if resp != nil && resp.Body != nil {
		/*
			如果响应不是 nil，并且响应 body 也非空，如果这两条都满足，那么会使用 ioutil.Discard 将 body 内容丢弃。
			在Go中，http.Response类型包含一个响应体（Response.Body），其类型为io.ReadCloser。
			在处理HTTP响应时，为了确保正确释放资源，需要在使用完响应体后关闭它。
			但是，如果您只是调用 resp.Body.Close()，而没有读取响应体的所有内容，可能会导致HTTP连接未被完全释放。
			一般来说，释放 http 连接，也可以通过http.Response 结构体的 Close()函数来显示关闭连接，还可以通过 http.client 结构体
			的 Transport 方法来设置响应时的时间限制，可以限制一个 ResponseHeaderTimeout 。
		*/
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		return nil, errs[0]
	}

	if resp.StatusCode != 200 {
		/*
			errors.New() 函数是 go 语言中的一个函数，它用于创建一个新的 error 类型的值。
			当我们遇到某个错误时，我们通常会将其作为返回值返回给调用者。
		*/
		return nil, errors.New(body)
	}

	log.Debug(body)

	/*
	将一个 JSON 格式的字符串 body 解析成一个 Go 的结构体 allSettings。
	具体地，json.Unmarshal 函数接受一个 []byte 类型的参数作为待解析的 JSON 字符串，
	并将解析结果写入到第二个参数 allSettings 所代表的结构体中。
	函数的返回值 err 是一个 error 类型的变量，代表着解析过程中可能会产生的任何错误，
	如 JSON 格式不合法或者结构体成员与 JSON 中的字段名不匹配等。
	它的函数签名如下：
		func Unmarshal(data []byte, v interface{}) error
	
	*/
	err := json.Unmarshal([]byte(body), allSettings)
	if err != nil {
		panic(err)
		return nil, err
	}

	return allSettings, nil
}
/*
	使用 Elasticsearch HTTP API获取索引映射的函数。
*/
func (s *ESAPIV0) GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error) {
	url := fmt.Sprintf("%s/%s/_mapping", s.Host, indexNames)
	resp, body, errs := Get(url, s.Auth, s.HttpProxy)

	if resp != nil && resp.Body != nil {
		/*
		ioutil.Discard 是一个空的可写的 io.Writer。
		它的作用是忽略 (Discard) 所有写入的数据。在 Go 中，一个 HTTP 响应主体 (response body) 
		是一个 io.ReadCloser。这个类型不仅实现了 io.Reader 接口，还实现了 io.Closer 接口，
		所以可以使用 resp.Body.Close() 来显式关闭响应主体，以便释放底层的资源。
		但是，如果您在下载大文件时关闭了响应主体，那么库可能已经向操作系统发送了多个读取请求，
		这些请求可能还没有收到响应。在这种情况下，关闭响应主体可能会导致错误。

		因此，为了避免这些问题，通常使用 io.Copy 复制响应主体的内容到“ioutil.Discard”，
		以确保客户端已完全读取完响应主体，并且将其数据丢弃，然后您可以安全地关闭响应主体。
		*/
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
	er := json.Unmarshal([]byte(body), &idxs)

	if er != nil {
		log.Error(body)
		return "", 0, nil, er
	}

	// remove indexes that start with . if user asked for it
	//if copyAllIndexes == false {
	//      for name := range idxs {
	//              switch name[0] {
	//              case '.':
	//                      delete(idxs, name)
	//              case '_':
	//                      delete(idxs, name)
	//
	//
	//                      }
	//              }
	//      }

	// if _all indexes limit the list of indexes to only these that we kept
	// after looking at mappings
	/*这个if结构体主要是为了处理 Elasticsearch 查询中的索引名称*/
	/*如果索引名称是 _all，*/
	if indexNames == "_all" {

		var newIndexes []string
		for name := range idxs {
			newIndexes = append(newIndexes, name)
		}
		/*则表示需要返回所有的索引，因此需要将 idxs (一个 map 对象，保存了所有已知索引的名称) 中的索引名称拼接成一个用逗号分隔的字符串返回。*/
		indexNames = strings.Join(newIndexes, ",")
		/*如果索引名称包含通配符 (* 或 ?)，*/
	} else if strings.Contains(indexNames, "*") || strings.Contains(indexNames, "?") {
		/*首先，使用 regexp.Compile 将索引名称转换成正则表达式。*/
		r, _ := regexp.Compile(indexNames)

		//check index patterns
		var newIndexes []string
		/*然后，遍历 idxs 中保存了的所有索引名称，使用 r.MatchString 方法根据正则表达式匹配当前索引名称。*/
		for name := range idxs {
			matched := r.MatchString(name)
			if matched {
				/*如果匹配成功，则将当前索引名称加入 newIndexes 列表。*/
				newIndexes = append(newIndexes, name)
			}
		}
		/*最后，将 newIndexes 列表中的索引名称拼接成一个用逗号分隔的字符串返回。*/
		indexNames = strings.Join(newIndexes, ",")

	}
	/*首先定义了一个变量i，并将它初始化为0。*/
	i := 0
	// wrap in mappings if moving from super old es
	for name, idx := range idxs {
		/*然后，代码通过循环遍历idxs切片中的每一个元素，其中name和idx是循环变量。在循环体内，每次i都会加1。*/
		i++
		/*if语句检查idx中是否存在名为"mappings"的键。如果不存在，*/
		if _, ok := idx.(map[string]interface{})["mappings"]; !ok {
			/*
				在这段代码中，"mappings": idx 是一个map[string]interface{}类型的键值对，它将映射 "mappings" 与变量 idx 对应的值相绑定。
				这行代码的意思是将索引的名称（name）与索引的映射（idx）存储在一个新的map[string]interface{}中，并将它放入idxs切片中。
				并且因为新的map[string]interface{}没有 "mappings" 的key，并且idx是一个map[string]interface{}类型的值，
				所以将idx存储到新的map[string]interface{}中的 "mappings" key 中，以便在以后的处理中能够方便地通过这个 key 获取映射。
				差不多就是这个意思：
					idxs = map[string]interface{}{
					  name: map[string]interface{}{
					    "mappings": idx,
					  },
					}
			*/
			(idxs)[name] = map[string]interface{}{
				"mappings": idx,
			}
		}
	}
	/*
		在这个函数中，返回 &idxs 是为了提高效率，减少内存使用。
		在 Go 中，如果函数返回大的结构体，它会复制整个结构体并返回一个副本。
		而如果使用指针返回，只需返回指向结构体的指针，这样可以避免复制整个结构体，提高效率。
		因此，在此函数中，返回 &idxs 是为了避免复制整个结构体，减少内存使用和提高效率。
	*/
	return indexNames, i, &idxs, nil
}
/*这段代码的作用是创建一个空的索引配置，并将其返回。*/
func getEmptyIndexSettings() map[string]interface{} {

	/*新建了一个名为 tempIndexSettings 的 map[string]interface{}，它表示我们要创建的空的索引的配置。*/
	tempIndexSettings := map[string]interface{}{}

	/*tempIndexSettings 的 settings 字段设置为一个新的 map[string]interface{}，以表示索引的设置。*/
	tempIndexSettings["settings"] = map[string]interface{}{}

	/*将 tempIndexSettings 的 kye=settings的值value=index的value设置为空的map，相当于清空了该属性内容*/
	tempIndexSettings["settings"].(map[string]interface{})["index"] = map[string]interface{}{}
	return tempIndexSettings
}

/*
	这是一个名为 cleanSettings() 的函数。它接受一个 settings 的字典类型，这个字典类型包含了 Elasticsearch 索引的配置。
	在函数体内，使用了delete()函数来删除字典中的四个字段。这些字段分别是creation_date、uuid、version和provided_name。
	它们都是在索引创建时自动生成的字段，对于索引的实际使用并没有帮助。因此，这个函数的作用就是将这些无用的字段从索引配置信息中清理掉。
*/
func cleanSettings(settings map[string]interface{}) {
	//clean up settings
	delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "creation_date")
	delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "uuid")
	delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "version")
	delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "provided_name")
}

/*
	这是一个名为 UpdateIndexSettings 的方法。
	这个方法是 ESAPIV0 结构体的一个方法，用于更新Elasticsearch索引中的设置。
	这个方法的参数：
		name: string 类型，表示索引的名称。
		settings: map[string]interface{} 类型，表示索引设置。
*/
func (s *ESAPIV0) UpdateIndexSettings(name string, settings map[string]interface{}) error {

	/*记录要更新的索引名称及其索引设置。*/
	log.Debug("update index: ", name, settings)
	/*清理不需要的索引设置字段。通过调用 cleanSettings() 函数来执行此操作。*/
	cleanSettings(settings)
	/*使用 fmt.Sprintf() 函数来构造请求的 URL，其中 %s 与传递给它的参数替换。*/
	url := fmt.Sprintf("%s/%s/_settings", s.Host, name)

	/*检查 索引设置settings 中是否包含 index 键*/
	if _, ok := settings["settings"].(map[string]interface{})["index"]; ok {
		/*如果包含 index 键，也需要同时包含 analysis*/
		if set, ok := settings["settings"].(map[string]interface{})["index"].(map[string]interface{})["analysis"]; ok {
			/*打印一条日志，并在日志记录这个 name*/
			log.Debug("update static index settings: ", name)
			/*创建一个默认设置的空索引配置*/
			staticIndexSettings := getEmptyIndexSettings()
			/*
				1、staticIndexSettings是一个变量，类型为map[string]interface{}，它包含了要更新的索引的设置信息。
				2、从 staticIndexSettings 中获取 settings 字段，并将其断言为 map[string]interface{} 类型；再从中获取 index 字段，并将其断言为 map[string]interface{} 类型；
					最终，从 index 中获取 analysis 字段，并将其赋值为 set 变量。
			*/
			staticIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["analysis"] = set
			/*
				使用 fmt.Sprintf 方法生成一个 url，使用 post 请求，向 Elasticsearch 节点发送请求进行关闭指定索引的操作。
			*/
			Post(fmt.Sprintf("%s/%s/_close", s.Host, name), s.Auth, "", s.HttpProxy)
			
			/*
				bytes.Buffer 是 go 语言标准库中实现的一种 io.Writer 接口的数据类型，可以存储二进制数据，以便于在程序中进行读写操作。
				通常情况下，我们使用  bytes.Buffer 来存储和操作一些数据流，例如从网络连接或文件中读取的字节，或者我们要通过 HTTP 请求发送的数据。
				比如这里，我们使用 byte.Buffer 来构造 HTTP 请求的请求体（body），也就是包含在 HTTP POST 请求中的数据。
				我们可以向 bytes.Buffer 中写入字节切片或字符串，以构建请求体。通过这种方式，我们可以很方便的构建 HTTP 请求，并将数据发送给 Elasticsearch 服务。
				使用 bytes.Buffer，我们就不需要对数据类型进行格式转换，另外，使用 butyes.Buffer 将要读取的数据添加到请求体中，不需要额外的缓冲区。
				同时，与字符串相比，使用 bytes.Buffer 可以避免不必要的内存分配和拷贝操作，因为 bytes.Buffer 可以在必要时调整大小，而不需要重新分配内存。
			*/
			body := bytes.Buffer{}

			/*
				这段代码表示，创建了一个 enc 的变量，然后使用 go 语言中 json 包，通过调用其 NewEncoder 函数创建了一个 JSON 编码器 enc。
				这个编码器会把编码后的 json 数据写入到缓冲区指针 &body 所指向的内存空间。
			*/
			enc := json.NewEncoder(&body)
			/*
				1、这段代码表示，将 staticIndexSettings 的变量转换为 json 格式，然后通过 io.Writer 接口输出到外部。
				2、json.NewEncoder用于创建一个新的Encoder，而enc.Encode方法则将数据编码为JSON格式，并写入参数中 提供的io.Writer接口类型的对象中（也就是将JSON格式的数据写入到缓冲区中）。
			*/
			enc.Encode(staticIndexSettings)
			/*
				将这个body的内容 put 给 elasticsearch。
			*/
			bodyStr, err := Request("PUT", url, s.Auth, &body, s.HttpProxy)
			if err != nil {
				log.Error(bodyStr, err)
				panic(err)
				return err
			}
			/*这行代码使用delete函数从settings映射中删除一个名为"analysis"的键值对*/
			delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "analysis")
			/*这行代码使用delete函数从settings映射中删除一个名为"analysis"的键值对，*/
			Post(fmt.Sprintf("%s/%s/_open", s.Host, name), s.Auth, "", s.HttpProxy)
		}
	}

	log.Debug("update dynamic index settings: ", name)

	body := bytes.Buffer{}
	enc := json.NewEncoder(&body)
	enc.Encode(settings)
	_, err := Request("PUT", url, s.Auth, &body, s.HttpProxy)

	return err
}

/*更新 Elasticsearch 中索引的映射(mapping)*/
func (s *ESAPIV0) UpdateIndexMapping(indexName string, settings map[string]interface{}) error {

	log.Debug("start update mapping: ", indexName, settings)

	for name, mapping := range settings {

		log.Debug("start update mapping: ", indexName, name, mapping)

		/*用于管理和操作索引的api，可以创建、更新、获取索引映射(mapping)*/
		url := fmt.Sprintf("%s/%s/%s/_mapping", s.Host, indexName, name)

		body := bytes.Buffer{}
		enc := json.NewEncoder(&body)
		enc.Encode(mapping)
		res, err := Request("POST", url, s.Auth, &body, s.HttpProxy)
		if err != nil {
			log.Error(url)
			log.Error(body.String())
			log.Error(err, res)
			panic(err)
		}
	}
	return nil
}

/*这段代码是一个使用 ESAPIV0 版本来删除索引的方法*/
func (s *ESAPIV0) DeleteIndex(name string) (err error) {

	log.Debug("start delete index: ", name)

	url := fmt.Sprintf("%s/%s", s.Host, name)

	/*使用了 DELETE 方法和索引完整 URL，以及身份验证信息 s.Auth，传递了 nil 值的请求正文。然后打印了一条调试信息，表明索引已被删除。*/
	Request("DELETE", url, s.Auth, nil, s.HttpProxy)

	log.Debug("delete index: ", name)

	return nil
}


/*这段代码是一个使用 ESAPIV0 版本来创建索引的方法*/
func (s *ESAPIV0) CreateIndex(name string, settings map[string]interface{}) (err error) {
	/*将 source settings 中的一些源 settings 的字段删除掉，这个在 target Elasticsearch 创建索引时候，会重新生成*/
	cleanSettings(settings)

	body := bytes.Buffer{}
	enc := json.NewEncoder(&body)
	enc.Encode(settings)
	log.Debug("start create index: ", name, settings)

	url := fmt.Sprintf("%s/%s", s.Host, name)

	resp, err := Request("PUT", url, s.Auth, &body, s.HttpProxy)
	log.Debugf("response: %s", resp)

	return err
}

/*
	这段代码的意思，当你往 Elasticsearch 索引中新增、更新或删除文档时，这些操作由于不是实时的，它们会被暂存在内存缓冲区中。这个内存缓冲区被称为 "分片缓冲区"(shard write buffer)。
	这个缓冲区的主要作用时提高吞吐量的索引的性能。但这也意味着，搜索的数据会比索引操作的数据滞后一点。因此如果需要看到最新的修改后的数据，则需要手动刷新索引。
*/
func (s *ESAPIV0) Refresh(name string) (err error) {

	log.Debug("refresh index: ", name)

	url := fmt.Sprintf("%s/%s/_refresh", s.Host, name)

	resp, _, _ := Post(url, s.Auth, "", s.HttpProxy)
	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	return nil
}


/*
	这段代码是一个用于创建 Elasticsearch 滚动查询（scroll query）的函数，它使用了 Elasticsearch 的搜索 API，查询符合特定条件的文档并返回一个指向查询结果的“指针”，
	之后可以使用这个指针来一次次地从 Elasticsearch 中获取查询结果，直到没有结果为止。
	这个方法的参数：
		indexNames：string 数据类型，指定用于查询的 Elasticsearch 索引名称，可以指定一个或多个索引名称，多个名称之间用逗号分隔。
		scrollTime: string 数据类型，指定查询结果被缓存的时间，以便后续的滚动查询能够使用同一个缓存。可以设置为秒（s）或分钟（m），如 "10m"。
		docBufferCount：int 数据类型，指定每次获取查询结果时获取多少个文档。
		query：string 类型，指定查询的条件，可以使用 Elasticsearch 的 Query DSL 进行更复杂的查询。如果不传递该参数，会查询索引中的所有文档。
		slicedId: int 类型，表示当前搜索操作的分片 ID。通过这个参数，我们可以在分片搜索过程中将搜索操作分割成多个并发任务，从而提高搜索的效率和响应速度。
		maxSlicedCount: int 类型，表示搜索操作涉及的分片总数。通过这个参数，我们可以在分片搜索过程中将搜索操作分割成多个并发任务，从而提高搜索的效率和响应速度。
		fields：string 类型，指定要返回的字段名称，可以指定多个字段，之间用逗号分隔。如果不传递该参数，会返回所有字段。
*/
func (s *ESAPIV0) NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, slicedId, maxSlicedCount int, fields string) (scroll interface{}, err error) {

	// curl -XGET 'http://es-0.9:9200/_search?search_type=scan&scroll=10m&size=50'
	/*
		使用 Elasticsearch 的 _search API 搜索指定索引中符合条件的文档。
		由于在这里使用了滚动查询，所以在请求参数中，指定了 search_type=scan，并设置了 scroll 和 size 参数。
			scroll: 指的 scrollTime，指定查询结果被缓存的时间。
			size: 指的 docBufferCount，指定每次获取查询结果时获取多少个文档。
	*/
	url := fmt.Sprintf("%s/%s/_search?search_type=scan&scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

	/*在 go 语言中，[]byte 是表示字节切片的数据类型，通常用于处理二进制数据或者字符数据。这里指定jsonBody 数据类型为 字节切片，通过 go 内置的标准函数，避免程序员手动进行转换。*/
	var jsonBody []byte
	
	/*这段代码，将 fields 和 query 转换为对应的 Elasticsearch 查询语句。*/
	if len(query) > 0 || len(fields) > 0 {
		queryBody := map[string]interface{}{}
		if len(fields) > 0 {
			if !strings.Contains(fields, ",") {
				/*判断 fields 是否只有一个字段，如果只有一个字段，则 queryBody["_source"] = fields*/
				queryBody["_source"] = fields
			} else {
				/*如果 fields 包含了多个字段，则通过 strings.Split 方法将其拆分为字符串数组并赋值给 _source。*/
				queryBody["_source"] = strings.Split(fields, ",")
			}
		}

		if len(query) > 0 {
			queryBody["query"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"].(map[string]interface{})["query"] = query
		}

		jsonBody, err = json.Marshal(queryBody)
		if err != nil {
			log.Error(err)
			return nil, err
		}

	}
	//resp, body, errs := Post(url, s.Auth,jsonBody,s.HttpProxy)
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

/*这段代码，主要是用于获取 Elasticsearch 的 Scoll API 的下一页数据。*/
func (s *ESAPIV0) NextScroll(scrollTime string, scrollId string) (interface{}, error) {
	//  curl -XGET 'http://es-0.9:9200/_search/scroll?scroll=5m'
	id := bytes.NewBufferString(scrollId)
	/*DoRequest 发起了一个 HTTP 请求到 Elasticsearch Scroll API，包含 scroll 和 scroll_id，以获取下一页数据。*/
	url := fmt.Sprintf("%s/_search/scroll?scroll=%s&scroll_id=%s", s.Host, scrollTime, id)
	body, err := DoRequest(s.Compress, "GET", url, s.Auth, nil, s.HttpProxy)

	if err != nil {
		log.Error(err)
		return nil, err
	}

	// decode elasticsearch scroll response
	scroll := &Scroll{}
	err = DecodeJson(body, &scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}
