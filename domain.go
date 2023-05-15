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

import "sync"

/* 定义了一个 Indexes 的结构体，以空接口为值的 map 类型。 */
type Indexes map[string]interface{}

type Document struct {
	Index   string                 `json:"_index,omitempty"`  /*表示该文档所属的索引名称。*/
	Type    string                 `json:"_type,omitempty"`   /*表示文档的类型，类似于数据库表的概念。*/
	Id      string                 `json:"_id,omitempty"`     /*文档的唯一标识符。*/
	source  map[string]interface{} `json:"_source,omitempty"` /*文档的数据，以 map[string]interface{} 类型存储。*/
	Routing string                 `json:"routing,omitempty"` /*在 Elasticsearch 6.x 之后，只支持 routing 字段进行路由操作，这个字段表示文档的路由值。*/

/* 定义了 Elasticsearch 组件 Scroll API 返回结果的结构体。 */
type Scroll struct {
	Took     int    `json:"took,omitempty"`	/*表示这个 Scroll 请求耗费的时间。*/
	ScrollId string `json:"_scroll_id,omitempty"`/*表示一个用于获取下一批数据的 scroll_id。*/
	TimedOut bool   `json:"timed_out,omitempty"`/*表示请求是否超时。*/

	/*
	包含了搜索结果的具体信息，其中 MaxScore 表示所有搜索结果中的最高分数，Total 表示搜索结果的总数，Docs 存放着搜索结果具体的文档信息。
	*/
	Hits     struct {
		MaxScore float32       `json:"max_score,omitempty"` /*设置了omitempty标记，在将结构体转换成JSON字符串时，如果它们的值为零值，则会被省略掉。*/
		Total    int           `json:"total,omitempty"` 	/*设置了omitempty标记，在将结构体转换成JSON字符串时，如果它们的值为零值，则会被省略掉。*/
		Docs     []interface{} `json:"hits,omitempty"`		/*设置了omitempty标记，在将结构体转换成JSON字符串时，如果它们的值为零值，则会被省略掉。*/
	} `json:"hits"`

	/*
	表示搜索使用的分片信息，其中 Total 表示总的分片数，Successful 表示成功的分片数。
	*/
	Shards struct {
		Total      int `json:"total,omitempty"` 		/*表示请求的操作总 shard 数量*/
		Successful int `json:"successful,omitempty"`	/*表示操作成功的 shard 数量*/
		Skipped    int `json:"skipped,omitempty"`		/*表示操作被跳过的 shard 数量*/
		Failed     int `json:"failed,omitempty"`		/*表示操作失败的 shard 数量*/

		/*表示操作失败的原因列表，是一个结构体数组，包括以下字段*/
		Failures   []struct {
			Shard  int         `json:"shard,omitempty"` /*表示失败的 shard 编号*/
			Index  string      `json:"index,omitempty"`	/*表示失败的索引名称*/
			Status int         `json:"status,omitempty"`/*表示失败的 HTTP 状态码*/
			Reason interface{} `json:"reason,omitempty"`/*表示失败的原因，类型为接口类型 interface{}，可以存储任意类型的值，包括字符串、数字、结构体、切片等。*/
		} `json:"failures,omitempty"`
	} `json:"_shards,omitempty"`
}

/*定义了一个名为 ScrollV7 的结构体，它包含了一个 Scroll 类型的嵌入字段和一个名为 Hits 的内部结构体*/
type ScrollV7 struct {

	/*
		Scroll 类型可以看作这个结构体的父类，因为它被嵌入在了 ScrollV7 结构体中。
		嵌入字段可以使得这个结构体继承 Scroll 类型的方法和属性。
		通过这种方式，我们可以在不重复定义相同字段和方法的情况下扩展已存在的结构体。
	*/
	Scroll
	Hits struct {
		MaxScore float32 `json:"max_score,omitempty"` /* float32 类型的可选字段，表示搜索结果的最大匹配度得分。*/

		/*
			内部结构体，它由 Value 和 Relation 两个字段组成，用于表示搜索结果的总数和第几次搜索结果。
		*/
		Total    struct {
			Value    int    `json:"value,omitempty"`
			Relation string `json:"relation,omitempty"`
		} `json:"total,omitempty"`
		Docs []interface{} `json:"hits,omitempty"`	/*接口类型的可选字段，它表示搜索到的结果文档列表。*/
	} `json:"hits"`
}


/*结构体的定义，它包含了 Elasticsearch 集群的版本信息。*/
type ClusterVersion struct {
	Name        string `json:"name,omitempty"`					/*表示版本的名称。*/
	ClusterName string `json:"cluster_name,omitempty"`			/*表示 Elasticsearch 集群的名称。*/

	/*嵌套结构体，包含了 Elasticsearch 版本和 Lucene 版本的信息。*/
	Version     struct {
		Number        string `json:"number,omitempty"`			/*表示 Elasticsearch 版本号。*/
		LuceneVersion string `json:"lucene_version,omitempty"`	/*表示使用的 Lucene 版本号。*/
	} `json:"version,omitempty"`								/*omitempty 表示当该字段为零值时忽略输出。*/
}

type ClusterHealth struct {
	Name   string `json:"cluster_name,omitempty"`	/*cluster_name 应该用作 Name 字段的 JSON 字段名称*/
	Status string `json:"status,omitempty"`			/*status 应该用作 Status 字段的 JSON 字段名称。*/
}

// {"took":23,"errors":true,"items":[{"create":{"_index":"mybank3","_type":"my_doc2","_id":"AWz8rlgUkzP-cujdA_Fv","status":409,"error":{"type":"version_conflict_engine_exception","reason":"[AWz8rlgUkzP-cujdA_Fv]: version conflict, document already exists (current version [1])","index_uuid":"w9JZbJkfSEWBI-uluWorgw","shard":"0","index":"mybank3"}}},{"create":{"_index":"mybank3","_type":"my_doc4","_id":"AWz8rpF2kzP-cujdA_Fx","status":400,"error":{"type":"illegal_argument_exception","reason":"Rejecting mapping update to [mybank3] as the final mapping would have more than 1 type: [my_doc2, my_doc4]"}}},{"create":{"_index":"mybank3","_type":"my_doc1","_id":"AWz8rjpJkzP-cujdA_Fu","status":400,"error":{"type":"illegal_argument_exception","reason":"Rejecting mapping update to [mybank3] as the final mapping would have more than 1 type: [my_doc2, my_doc1]"}}},{"create":{"_index":"mybank3","_type":"my_doc3","_id":"AWz8rnbckzP-cujdA_Fw","status":400,"error":{"type":"illegal_argument_exception","reason":"Rejecting mapping update to [mybank3] as the final mapping would have more than 1 type: [my_doc2, my_doc3]"}}},{"create":{"_index":"mybank3","_type":"my_doc5","_id":"AWz8rrsEkzP-cujdA_Fy","status":400,"error":{"type":"illegal_argument_exception","reason":"Rejecting mapping update to [mybank3] as the final mapping would have more than 1 type: [my_doc2, my_doc5]"}}},{"create":{"_index":"mybank3","_type":"doc","_id":"3","status":400,"error":{"type":"illegal_argument_exception","reason":"Rejecting mapping update to [mybank3] as the final mapping would have more than 1 type: [my_doc2, doc]"}}}]}
/*定义 BulkResponse 结构的 Go 代码，用于解析 Elasticsearch 的 Bulk API 的响应结果。*/
type BulkResponse struct {
	Took   int                 `json:"took,omitempty"`	/*总共用时*/
	Errors bool                `json:"errors,omitempty"`/*是否存在错误*/
	Items  []map[string]Action `json:"items,omitempty"` /*Items 字段是一个由 map[string]Action 组成的切片，其中 map 的键表示操作类型（如 "create"），
														Action 是该操作的具体信息，包括索引名、文档类型、文档ID、响应状态码及错误信息等。*/
}

/*表示 Elasticsearch 中执行操作时的结果。*/
type Action struct {
	Index  string      `json:"_index,omitempty"`	/*表示操作执行的目标索引名称。*/
	Type   string      `json:"_type,omitempty"`		/*表示文档的类型，一般不建议使用，已经在 Elasticsearch 7.x 版本中去除。*/
	Id     string      `json:"_id,omitempty"`		/*表示操作执行的目标文档的 ID。*/
	Status int         `json:"status,omitempty"`	/*表示操作执行的状态码。*/
	Error  interface{} `json:"error,omitempty"`		/*表示操作执行时的错误信息，如果操作成功，则该字段为 nil。*/
}

type Migrator struct {
	FlushLock   sync.Mutex					/*FlushLock 字段是一个互斥锁，用于同步批量写入操作。*/
	DocChan     chan map[string]interface{}	/*DocChan 字段是一个channel，用于传递待迁移的文档数据。*/
	SourceESAPI ESAPI	/*SourceESAPI 和 TargetESAPI 是两个字段，类型均为 ESAPI。它们是源 Elasticsearch 和目标 Elasticsearch 的 API 接口，用于实现数据的复制。*/
	TargetESAPI ESAPI
	SourceAuth  *Auth	/*SourceAuth 和 TargetAuth 是两个字段，类型均为 Auth。表示源 Elasticsearch 和目标 Elasticsearch 的认证信息。*/
	TargetAuth  *Auth
	Config      *Config	/*Config 是一个指向 Config 结构体的指针，表示迁移任务的一些配置信息，如索引名称、文档类型、批量写入数据大小等。*/
}

type Config struct {
	
	/*SourceEs：源 Elasticsearch 实例的地址；*/
	SourceEs            string `short:"s" long:"source"  description:"source elasticsearch instance, ie: http://localhost:9200"`
	/*Query：在源 Elasticsearch 实例上的查询；*/
	Query               string `short:"q" long:"query"  description:"query against source elasticsearch instance, filter data before migrate, ie: name:medcl"`
	/*TargetEs：目标 Elasticsearch 实例的地址；*/
	TargetEs            string `short:"d" long:"dest"    description:"destination elasticsearch instance, ie: http://localhost:9201"`
	/*SourceEsAuthStr：源 Elasticsearch 实例的 HTTP Basic 认证信息；*/
	SourceEsAuthStr     string `short:"m" long:"source_auth"  description:"basic auth of source elasticsearch instance, ie: user:pass"`
	/*TargetEsAuthStr：目标 Elasticsearch 实例的 HTTP Basic 认证信息；*/
	TargetEsAuthStr     string `short:"n" long:"dest_auth"  description:"basic auth of target elasticsearch instance, ie: user:pass"`
	/*DocBufferCount：每次检索的文档数；*/
	DocBufferCount      int    `short:"c" long:"count"   description:"number of documents at a time: ie \"size\" in the scroll request" default:"10000"`
	/*BufferCount：内存中的缓存文档数量；*/
	BufferCount         int    `long:"buffer_count"   description:"number of buffered documents in memory" default:"1000000"`
	/*Workers：并发的 bulk Workers 数量；*/
	Workers             int    `short:"w" long:"workers" description:"concurrency number for bulk workers" default:"1"`
	/*BulkSizeInMB：每次 bulk 操作中的文档数量；*/
	BulkSizeInMB        int    `short:"b" long:"bulk_size" description:"bulk size in MB" default:"5"`
	/*ScrollTime：每次 scroll 的时间间隔；*/
	ScrollTime          string `short:"t" long:"time"    description:"scroll time" default:"10m"`
	/*ScrollSliceSize：sliced scroll 的大小，需要>1才能生效；*/
	ScrollSliceSize     int    `long:"sliced_scroll_size"    description:"size of sliced scroll, to make it work, the size should be > 1" default:"1"`
	/*RecreateIndex：是否在复制之前删除目标索引；*/
	RecreateIndex       bool   `short:"f" long:"force"   description:"delete destination index before copying"`
	/*CopyAllIndexes：是否包含复制起始点为.和_的索引；*/
	CopyAllIndexes      bool   `short:"a" long:"all"     description:"copy indexes starting with . and _"`
	/*CopyIndexSettings：是否复制源索引的设置；*/
	CopyIndexSettings   bool   `long:"copy_settings"          description:"copy index settings from source"`
	/*CopyIndexMappings：是否复制源索引的映射；*/
	CopyIndexMappings   bool   `long:"copy_mappings"          description:"copy index mappings from source"`
	/*ShardsCount：新创建索引的分片数量；*/
	ShardsCount         int    `long:"shards"            description:"set a number of shards on newly created indexes"`
	/*SourceIndexNames：指定要复制的索引名称，支持正则表达式和逗号分隔；*/
	SourceIndexNames    string `short:"x" long:"src_indexes" description:"indexes name to copy,support regex and comma separated list" default:"_all"`
	/*TargetIndexName：指定要保存的索引名称，如果未指定，则使用原始索引名称；*/
	TargetIndexName     string `short:"y" long:"dest_index" description:"indexes name to save, allow only one indexname, original indexname will be used if not specified" default:""`
	/*OverrideTypeName：覆盖类型名称；*/
	OverrideTypeName    string `short:"u" long:"type_override" description:"override type name" default:""`
	/*WaitForGreen：在复制之前是否等待源和目标主机的集群状态都是绿色。黄色也可以；*/
	WaitForGreen        bool   `long:"green"             description:"wait for both hosts cluster status to be green before dump. otherwise yellow is okay"`
	/*LogLevel：设置日志级别，可选值为 trace、debug、info、warn、error；*/
	LogLevel            string `short:"v" long:"log"            description:"setting log level,options:trace,debug,info,warn,error"  default:"INFO"`
	/*DumpOutFile：将源索引的文档输出到本地文件的路径；*/
	DumpOutFile         string `short:"o" long:"output_file"            description:"output documents of source index into local file" `
	/*DumpInputFile：从本地 dump 文件输入索引。*/
	DumpInputFile       string `short:"i" long:"input_file"            description:"indexing from local dump file" `

	/*
		InputFileType：数据迁移程序中输入文件的数据类型，
		包括四种选项：dump（Elasticsearch dump数据）、json_line（json格式，一行一个document）、json_array（json格式，整个文件是一个数组）、log_line（每行一个document的log格式）。
	*/
	InputFileType       string `long:"input_file_type"                 description:"the data type of input file, options: dump, json_line, json_array, log_line" default:"dump" `
	/*SourceProxy：设置源  Elasticsearch  http 连接使用的代理，例如设置为http://127.0.0.1:8080*/
	SourceProxy         string `long:"source_proxy"            description:"set proxy to source http connections, ie: http://127.0.0.1:8080"`
	/*TargetProxy：设置目标  Elasticsearch  http 连接使用的代理，例如设置为http://127.0.0.1:8080*/
	TargetProxy         string `long:"dest_proxy"            description:"set proxy to target http connections, ie: http://127.0.0.1:8080"`
	/*Refresh：迁移完成后是否刷新索引。*/
	Refresh             bool   `long:"refresh"                 description:"refresh after migration finished"`
	/*Fields：需要迁移的源 Elasticsearch 中的字段，以逗号隔开，例如：col1,col2,col3...。*/
	Fields              string `long:"fields"                 description:"filter source fields, comma separated, ie: col1,col2,col3,..." `
	/*将源 Elasticsearch 中的字段重命名，并以键值对的形式进行指定，例如：_type:type, name:myname。*/
	RenameFields        string `long:"rename"                 description:"rename source fields, comma separated, ie: _type:type, name:myname" `
	/*LogstashEndpoint：目标Logstash的TCP地址，例如：127.0.0.1:5055*/
	LogstashEndpoint    string `short:"l"  long:"logstash_endpoint"    description:"target logstash tcp endpoint, ie: 127.0.0.1:5055" `
	/*LogstashSecEndpoint：目标Logstash的TCP地址是否启用了TLS安全协议*/
	LogstashSecEndpoint bool   `long:"secured_logstash_endpoint"    description:"target logstash tcp endpoint was secured by TLS" `

	/*将源 Elasticsearch 的数据重复输出N次到目标 Elasticsearch，与参数regenerate_id配合使用可扩大数据量*/
	RepeatOutputTimes         int  `long:"repeat_times"            description:"repeat the data from source N times to dest output, use align with parameter regenerate_id to amplify the data size "`
	/*RegenerateID：为目标 Elasticsearch 中的document重新生成ID，会覆盖掉原有的document ID*/
	RegenerateID              bool `short:"r" long:"regenerate_id"   description:"regenerate id for documents, this will override the exist document id in data source"`
	/*Compress：是否使用gzip压缩传输数据*/
	Compress                  bool `long:"compress"            description:"use gzip to compress traffic"`
	/*SleepSecondsAfterEachBulk：每次请求之间的睡眠时间，单位为秒，例如：-1表示不设置睡眠时间*/
	SleepSecondsAfterEachBulk int  `short:"p" long:"sleep" description:"sleep N seconds after each bulk request" default:"-1"`
}

type Auth struct {
	User string	/*用户名*/
	Pass string	/*密码*/
}
