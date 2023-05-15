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
	"encoding/json"

	"github.com/cheggaaa/pb"
	log "github.com/cihub/seelog"
)

/*
Scroll API 的接口定义，帮助用户通过 Scroll API 获取 Elasticsearch 中的数据。
*/
type ScrollAPI interface {
	GetScrollId() string
	GetHitsTotal() int
	GetDocs() []interface{}
	ProcessScrollResult(c *Migrator, bar *pb.ProgressBar)
	Next(c *Migrator, bar *pb.ProgressBar) (done bool)
}

/*
这里的 *Scroll 是一个结构体指针，指向一个 Scroll 结构体对象。这个结构体在 domain.go 里。
GetHitsTotal() 是 Scroll 结构体对象的一个方法，返回 scroll 查询的匹配文档总数。
*/
func (scroll *Scroll) GetHitsTotal() int {
	return scroll.Hits.Total
}

/*
GetScrollId() 是 Scroll API 接口中的一个方法，其目的是获取 Scroll API 的 ID。Scroll API 是 Elasticsearch 提供的一种查询方式，
它允许用户在不受限制地向 Elasticsearch 查询大量数据，而不会因为超时或者内存问题而出现查询中断的情况。
在 Scroll API 的分页查询中，每一次查询的结果都会附带一个 _scroll_id，它作为下一次查询时的查询参数，
用于识别前后两次查询之间的关系。因此，在 Scroll API 的实现过程中，我们需要调用 Scroll API 查询的结果中获取 _scroll_id，
并将其返回给调用方，以供下一次查询使用。因此，GetScrollId() 的返回值是一个 string 类型的 _scroll_id。
*/
func (scroll *Scroll) GetScrollId() string {
	return scroll.ScrollId
}

/*
这段代码是定义在 Scroll 结构体上的方法，用于获取当前 scroll 查询获取到的文档列表。
具体来说，这个方法返回一个包含了当前 scroll 查询获取到的文档列表的 interface 类型切片。
由于 Elasticsearch 文档的数据是 JSON 格式的，因此在这个方法中，文档是以 interface{} 类型返回的，可以根据需要进行类型转换和处理。
*/
func (scroll *Scroll) GetDocs() []interface{} {
	return scroll.Hits.Docs
}

/*
定义了一个名为 GetHitsTotal() 的方法，返回 scroll.Hits.Total.Value 的值，类型为 int。
这个方法适用于 Elasticsearch 7.x 版本的 Scroll API 中，主要用于获取 scroll 查询结果中的命中记录总数。
这个方法会返回 scroll 结构体中 Hits 字段的 Total 属性的 Value 字段值。

	Hits 表示所有命中的文档，包括文档数据和元数据；
	Total 则表示所有匹配的文档总数。

在 Elasticsearch 7.x 版本中，Total 属性是一个对象，包含了 value、relation 两个字段，
其中 value 表示总命中数，relation 则表示是否精确匹配，常见的取值为 "eq"（精确匹配）和 "gte"（大于等于）。
*/
func (scroll *ScrollV7) GetHitsTotal() int {
	return scroll.Hits.Total.Value
}

/*
定义了一个名为 GetScrollId() 的方法，返回 scroll.ScrollId 的值，类型为字符串。
这个方法适用于 Elasticsearch 7.x 版本的 Scroll API 中，主要用于获取 scroll 查询结果的 Scroll ID。
具体来说，这个方法会返回 scroll 结构体中 ScrollId 字段的值，该字段表示下一次 scroll 查询时使用的 Scroll ID。
在 Elasticsearch 7.x 版本中，Scroll ID 是每一次 scroll 查询时生成的唯一字符串，用于标识 scroll 查询的状态和位置。
在进行 scroll 查询时，我们需要使用上一次的 Scroll ID 来获取下一批结果，直到所有结果都被获取完毕为止。
因此，我们可以使用这个方法获取当前 scroll 查询结果的 Scroll ID，并将其作为下一次 scroll 查询的参数进行使用，以便获取下一批结果。
*/
func (scroll *ScrollV7) GetScrollId() string {
	return scroll.ScrollId
}

/*
定义了一个名为 GetDocs() 的方法，返回 scroll.Hits.Docs 的值，类型为接口类型。
用于获取 Scroll 查询结果中的文档列表。具体来说，Hits.Docs 是一个包含查询结果文档的数组，而该代码返回的则是这个数组。
在进行 Scroll 查询时，查询结果并不是一次性返回的，而是需要循环调用 Scroll 方法来逐步获取全部文档。当最后一次 Scroll 方法返回的结果为空时，说明已经获取了所有的查询结果。
在这个过程中，Hits.Docs 会随着不同的 Scroll 方法调用返回不同的部分查询结果文档。因此，GetDocs 方法每调用一次，就会返回当前 Scroll 查询结果中所有的文档列表。
*/
func (scroll *ScrollV7) GetDocs() []interface{} {
	return scroll.Hits.Docs
}

/*
用于将 Scroll 查询结果写入通道中的处理函数。通常，我们需要将 Scroll 查询结果写入通道，以便在迁移数据时能够对文档进行处理。
*/
func (s *Scroll) ProcessScrollResult(c *Migrator, bar *pb.ProgressBar) {

	/*
	   这是一个进度条（progress bar）的代码，用于显示搜索结果已经被处理的进度。
	   在这个例子中，进度条的总长度是搜索结果的文档数目，每次处理一个文档就会向进度条增加一格，以此来表示处理的进度。
	   其中，s.Hits.Docs 是 Elasticsearch 搜索结果的文档列表，len(s.Hits.Docs) 返回结果文档列表的长度。
	   bar.Add 方法则用来增加进度条的进度，它接收一个整数参数，表示要增加的进度数量。
	*/
	bar.Add(len(s.Hits.Docs))

	/*
		这段代码是一个处理 Elasticsearch 搜索结果的错误信息。
		当 Elasticsearch 执行一个搜索请求时，如果在执行过程中发生了错误，这些错误信息会被存储在 s.Shards.Failures 变量中。这个变量是一个数组，它包含所有失败的分片信息。
	*/

	for _, failure := range s.Shards.Failures {
		/*这个代码则用来遍历这个数组，对于每个错误信息，它会使用 json.Marshal 方法将错误信息转换成 JSON 格式的字符串*/
		reason, _ := json.Marshal(failure.Reason)
		/*
			使用 log.Errorf() 来记录 Elasticsearch 在执行搜索请求时返回的错误信息，以供后续分析和调试。
			log.Errorf() 是一个将日志信息记录到系统日志中的函数。它的好处是能够在程序的运行过程中输出各种调试信息和错误信息，并且不会中断程序的运行，也不会向用户展示这些信息。
		*/
		log.Errorf(string(reason))
	}

	// write all the docs into a channel
	/*
		将 s.Hits.Docs 中的每个文档写入到 DocChan channel 中。其中，DocChan 是一个类型为 chan map[string]interface{} 的 channel，用于存储要迁移的 Elasticsearch 文档。

		map[string]interface{} 是一个键为 string 类型，值为任意类型的 map。在这里，docI.(map[string]interface{}) 是将 docI 强制类型转换为 map[string]interface{} 类型。
		因为 Hits.Docs 的类型是 []interface{}，它可以存储任何类型的值，所以需要将每个文档转换成 map[string]interface{} 类型后才能将其发送到 DocChan channel 中。
		强制类型转换操作使用 . 和括号进行，表示将 docI 转换为 map[string]interface{} 类型。

		一个文档通常由多个字段组成，这些字段的键和值需要分别用 string 和合适的类型表示，如 map[string]interface{}{"name": "Tom", "age": 18}。
		所以这里强制类型转换的原因是将文档从 interface{} 类型转换为 map[string]interface{} 类型后，可以获取其中的每一个键值对，进而用于迁移数据。
	*/
	for _, docI := range s.Hits.Docs {
		c.DocChan <- docI.(map[string]interface{})
	}
}

/*
实现了从 Elasticsearch 中滚动查询并获取文档的过程。
若结果为空，则返回 true，表示滚动查询结束。否则，代码调用 ProcessScrollResult() 方法将获取到的文档写入一个 channel，并更新进度条。
最后，代码更新了 s.ScrollId，以便进行下一轮滚动查询。

需要注意的是，这个方法中同样使用了类型断言 scroll.(ScrollAPI) 将接口类型转换成 ScrollAPI 类型对象，以便调用其 GetDocs()、ProcessScrollResult() 和 GetScrollId() 方法。
这些方法都是 ScrollAPI 接口提供的，实现了对滚动查询结果的处理和解析。
*/
func (s *Scroll) Next(c *Migrator, bar *pb.ProgressBar) (done bool) {

	/*
		使用 Elasticsearch 的 Scroll API 进行滚动查询，并获取下一页的结果。
		其中，SourceESAPI 是 Elasticsearch 的 API 客户端，Config.ScrollTime 是滚动查询的时间，s.ScrollId 是上一次滚动查询的 ID。
		NextScroll 方法会返回滚动查询结果中的下一页数据和一个错误对象 (err)。
	*/
	scroll, err := c.SourceESAPI.NextScroll(c.Config.ScrollTime, s.ScrollId)
	if err != nil {
		log.Error(err)
		return false
	}

	/*
		这行代码是将前面获取到的滚动查询结果 scroll 转换为 ScrollAPI 类型，并以此调用 GetDocs 方法，获取该滚动查询结果页中的所有文档。
		需要注意的是，这里的转换操作使用了类型断言 (ScrollAPI)，并且需要确保 GetDocs 方法在 ScrollAPI 接口中被定义。
	*/
	docs := scroll.(ScrollAPI).GetDocs()
	/*
		判断获取到的文档数量是否大于 0，来判断当前滚动查询是否结束。
		如果查询结果为空，那么打印一条调试日志，并通过 return true 表示滚动查询结束。
		其中，log.Debug() 方法是记录日志的函数，它基于已经初始化的 log 库实例，将错误信息以 DEBUG 级别输出。
	*/
	if docs == nil || len(docs) <= 0 {
		log.Debug("scroll result is empty")
		return true
	}
	/*
	   将前面获取到的滚动查询结果 scroll 转换为 ScrollAPI 类型，并以此调用 ProcessScrollResult 方法来对该滚动查询结果页进行处理。
	   c 和 bar 都是参数，c 是用于接收查询结果的 channel，而 bar 则是用于显示查询进度的进度条控件。
	   需要注意的是，这里的转换操作使用了类型断言 (ScrollAPI)，并且需要确保 ProcessScrollResult 方法在 ScrollAPI 接口中被定义。
	*/
	scroll.(ScrollAPI).ProcessScrollResult(c, bar)

	/*
		s.ScrollId 是一个字符串类型的变量，scroll 是一个实现了 ScrollAPI 接口的对象，GetScrollId() 是 ScrollAPI 接口中定义的方法。
		这行代码的作用是将 scroll 对象的 GetScrollId() 方法返回的滚动 ID 赋值给 s.ScrollId 变量。
		滚动 ID 是 Elasticsearch 用来标识一次滚动请求的唯一标识符，以便在滚动的过程中能够持续获取数据。
		在获取第一次滚动请求的结果后，需要将滚动 ID 保存下来，以便在后续的滚动请求中使用
	*/
	s.ScrollId = scroll.(ScrollAPI).GetScrollId()

	return
}

/*
这段代码实现了从 Elasticsearch 中批量读取数据，并将其放入一个 channel 中。具体来说，它使用了 Elasticsearch 的 Scroll API，在每次迭代中获取一批文档，
直到滚动查询的结果为空或者达到指定的文档数量。对于每个成功获取的文档，代码将其写入 c.DocChan 这个 channel 中，以便进行数据迁移。
注意，这里使用了类型断言 docI.(map[string]interface{}) 将接口类型转换成了文档对象，并假设它是一个 map 类型，其 key 为 string，value 为任意类型。
在这个过程中，如果有任何文档获取失败，将会显示错误原因，并继续迭代获取下一批文档。
*/
func (s *ScrollV7) ProcessScrollResult(c *Migrator, bar *pb.ProgressBar) {

	//update progress bar
	bar.Add(len(s.Hits.Docs))

	// show any failures
	for _, failure := range s.Shards.Failures {
		reason, _ := json.Marshal(failure.Reason)
		log.Errorf(string(reason))
	}

	// write all the docs into a channel
	for _, docI := range s.Hits.Docs {
		c.DocChan <- docI.(map[string]interface{})
	}
}

func (s *ScrollV7) Next(c *Migrator, bar *pb.ProgressBar) (done bool) {

	scroll, err := c.SourceESAPI.NextScroll(c.Config.ScrollTime, s.ScrollId)
	if err != nil {
		log.Error(err)
		return false
	}

	docs := scroll.(ScrollAPI).GetDocs()
	if docs == nil || len(docs) <= 0 {
		log.Debug("scroll result is empty")
		return true
	}

	scroll.(ScrollAPI).ProcessScrollResult(c, bar)

	//update scrollId
	s.ScrollId = scroll.(ScrollAPI).GetScrollId()

	return
}
