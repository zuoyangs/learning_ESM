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
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"

	log "github.com/cihub/seelog"
)

/*
函数实现了 Migrator 类型的NewBulkWorker方法。
*/
func (c *Migrator) NewBulkWorker(docCount *int, pb *pb.ProgressBar, wg *sync.WaitGroup) {

	log.Debug("start es bulk worker")

	bulkItemSize := 0                  /*是每个批量操作的文档数*/
	mainBuf := bytes.Buffer{}          /*mainBuf是缓冲区*/
	docBuf := bytes.Buffer{}           /*docBuf是缓冲区*/
	docEnc := json.NewEncoder(&docBuf) /*是json.Encoder类型的变量，用于将文档编码为json格式*/

	idleDuration := 5 * time.Second            /*是定时器idleTimeout的周期*/
	idleTimeout := time.NewTimer(idleDuration) /* 创建了一个名为 idleTimeout 的定时器，用于检查任务空闲时间*/
	defer idleTimeout.Stop()                   /*调用了idleTimeout.Stop(),以确保定时器能够被正确地停止*/

	taskTimeOutDuration := 5 * time.Minute            /*是定时器taskTimeout的周期*/
	taskTimeout := time.NewTimer(taskTimeOutDuration) /* 创建了一个名为 taskTimeout 的定时器，用于检查任务超时时间*/
	defer taskTimeout.Stop()                          /*调用了taskTimeout.Stop(),以确保定时器能够被正确地停止*/

READ_DOCS:
	for {
		idleTimeout.Reset(idleDuration)        /*idleTimeout是一个计时器，如果程序在idleDuration时间内一直没有任何操作，那么计时器就会触发，并执行相应的操作。*/
		taskTimeout.Reset(taskTimeOutDuration) /*taskTimeout则是在程序执行任务时设置的计时器，如果任务在taskTimeOutDuration时间内没有完成，则计时器就会触发，并执行相应的操作。*/

		/*select语句用于同时处理多个channel的读写操作*/
		select {
		/*接受来自c.DocChan的数据并将其存储在docI中*/
		case docI, open := <-c.DocChan:
			var err error
			log.Trace("read doc from channel,", docI)

			/*
				如果读取的数据包含有状态码为404的信息，说明该文档不存在，就输出错误日志并跳过该数据，继续读取下一条数据。
				这样可以确保程序在读取数据时不会停止，继续处理下一条数据。同时，使用日志记录出错信息也有助于程序员在后续调试中发现程序问题。
			*/
			if status, ok := docI["status"]; ok {
				if status.(int) == 404 {
					log.Error("error: ", docI["response"])
					continue
				}
			}

			/*
				这段代码是一个错误检查，目的是确保从读取的文档中的每个文档都包含必要的字段，否则会退出循环。
				其中，对于每个文档，循环检查是否包含"_index"、"_type"、"_source"和"_id"四个键。
				如果这些键有任何一个在当前文档中不存在，则会通过break语句退出循环并跳到READ_DOCS标记处。
				这个标记和外部循环配合使用，以确保所有文档都被正确地读取并检查。如果出现任何错误，程序将以错误状态退出并返回错误消息。
			*/
			for _, key := range []string{"_index", "_type", "_source", "_id"} {
				if _, ok := docI[key]; !ok {
					break READ_DOCS
				}
			}

			var tempDestIndexName string
			var tempTargetTypeName string
			/*
				在这段代码中，docI变量是一个interface{}类型的变量，
				但是我们需要从中获取_index和_type字段的值，这些值是string类型的。
				因此，我们需要使用类型断言将这些字段强制转换成string类型，以便在后续的代码中使用。
			*/
			tempDestIndexName = docI["_index"].(string)
			tempTargetTypeName = docI["_type"].(string)

			/*根据配置文件的设置来确定数据迁移的目标索引名称*/
			if c.Config.TargetIndexName != "" {
				/*程序会检查配置文件中是否设置了目标索引名称，如果设置了，则将该名称赋值给变量tempDestIndexName*/
				tempDestIndexName = c.Config.TargetIndexName
			}

			/*根据配置文件的设置来确定数据迁移的目标类型名称*/
			if c.Config.OverrideTypeName != "" {
				/*程序会检查配置文件中是否设置了目标类型名称，如果设置了，则将该名称赋值给变量tempTargetTypeName*/
				tempTargetTypeName = c.Config.OverrideTypeName
			}

			doc := Document{
				Index:  tempDestIndexName,                        /*文档的所属索引*/
				Type:   tempTargetTypeName,                       /*文档的所属类型*/
				source: docI["_source"].(map[string]interface{}), /*源数据信息存储在source字段中*/
				Id:     docI["_id"].(string),                     /*Id表示文档的唯一标识*/
			}

			/*如果 c.Config.RegenerateID 为 true，则会将 doc.Id 设置为空字符串，否则保留原有的 ID。*/
			if c.Config.RegenerateID {
				doc.Id = ""
			}

			/*
				用来实现对字段进行重命名的功能。
				首先，代码会判断配置文件中是否有设置需要重命名的字段，如果有，
			*/
			if c.Config.RenameFields != "" {

				/* c.Config.RenameFields 是一个用都好分割的字符串，例如：_type:type, name:myname ，每个子字符串表示一个需重命名的字段。*/
				kvs := strings.Split(c.Config.RenameFields, ",")

				/*这段代码会逐一处理每个子字符串，将其拆分成需要重命名的旧字段和新字段，然后对每个旧字段进行重命名。*/
				for _, i := range kvs {
					/*这段代码是在将字符串 i 按照冒号 : 进行拆分，将拆分后得到的多个子字符串保存在一个字符串切片 fvs 中。*/
					fvs := strings.Split(i, ":")
					/*对 fvs[0] 去除首位空格，并赋值给 oldField*/
					oldField := strings.TrimSpace(fvs[0])
					/*对 fvs[0] 去除首位空格，并赋值给 newField*/
					newField := strings.TrimSpace(fvs[1])
					/*如果旧字段名为 _type 的话*/
					if oldField == "_type" {
						/*那么 doc.source 的 newField 字段将会被设置为原始文档对象的 _type 字段值。*/
						doc.source[newField] = docI["_type"].(string)
					} else {
						/*
							否则，它会保留原来的值，并将旧字段名从新文档对象中删除。
						*/
						v := doc.source[oldField]
						doc.source[newField] = v
						delete(doc.source, oldField)
					}
				}
			}

			/*
				从一个 docI 对象中获取一个名为 _routing 的键值对，然后将其值赋给一个 doc 对象的 Routing 字段。
				如果 docI 中不含有 _routing 这个键，或者 _routing 对应的值不是字符串类型，或者 _routing 对应的字符串为空，则不会对 doc 的 Routing 字段做任何修改。
			*/
			if _, ok := docI["_routing"]; ok {
				str, ok := docI["_routing"].(string)
				if ok && str != "" {
					doc.Routing = str
				}
			}

			// if channel is closed flush and gtfo
			if !open {
				goto WORKER_DONE
			}

			// sanity check
			if len(doc.Index) == 0 || len(doc.Type) == 0 {
				log.Errorf("failed decoding document: %+v", doc)
				continue
			}

			// encode the doc and and the _source field for a bulk request
			/*
				将文档编码为 Elasticsearch 批量请求的形式。
				具体来说，它将文档以 index 操作的形式编码为一个 map[string]Document，然后使用 docEnc 对象将它编码为 JSON 格式，此时包括文档源数据。
				首先，创建一个 map[string]Document 类型的变量 post，其中 key 为 "index"，value 为文档对象。
			*/
			post := map[string]Document{
				"index": doc,
			}
			/*
				使用 docEnc 对象将 post 变量进行编码，生成一个 Elasticsearch 批量请求中的一部分。
			*/
			if err = docEnc.Encode(post); err != nil {
				log.Error(err)
			}
			/*
				将文档源数据使用 docEnc 对象进行编码，也作为 Elasticsearch 批量请求中的一部分。
			*/
			if err = docEnc.Encode(doc.source); err != nil {
				log.Error(err)
			}

			/*mainBuf是一个字节缓冲区，用于存储待发送的文档*/
			mainBuf.Write(docBuf.Bytes())
			// reset for next document
			bulkItemSize++
			(*docCount)++
			docBuf.Reset()

			/*
				当mainBuf的大小mainBuf.Len()加上待处理文档的大小docBuf.Len()大于 Elasticsearch 所允许的最大值时，将触发goto CLEAN_BUFFER，即清空当前缓冲区。
			*/
			if mainBuf.Len()+docBuf.Len() > (c.Config.BulkSizeInMB * 1024 * 1024) {
				goto CLEAN_BUFFER
			}

			/*空闲时间 idleTimeout.C 到来时触发清空缓冲区操作*/
		case <-idleTimeout.C:
			log.Debug("5s no message input")
			/*idleTimeout Go 的计时器，分别在5秒后发送当前时间信号给对应的信道。如果下一轮处理有文档输入，则这两个计时器会重置；否则在超时后会触发清空缓冲区操作。*/
			goto CLEAN_BUFFER
			/*任务超时时间taskTimeout.C到来时触发清空缓冲区操作*/
		case <-taskTimeout.C:
			log.Warn("5m no message input, close worker")
			/*taskTimeout是 Go 的计时器，分别在5分钟后发送当前时间信号给对应的信道。如果下一轮处理有文档输入，则这两个计时器会重置；否则在超时后会触发清空缓冲区操作。*/
			goto WORKER_DONE
		}

		goto READ_DOCS

		/*CLEAN_BUFFER的标签的作用是在执行完一次批量操作后清空缓冲区并进入下一轮的批量操作。*/
	CLEAN_BUFFER:
		/*c.TargetESAPI.Bulk(&mainBuf)将缓冲区中的数据批量插入到目标 Elasticsearch 中*/
		c.TargetESAPI.Bulk(&mainBuf)
		/*然后打印一条日志表示已清空缓冲区并执行了批量插入操作*/
		log.Trace("clean buffer, and execute bulk insert")
		/*接着，程序会更新批量操作的大小计数器，并将其重置为0，以便开启新的一轮批量插入。*/
		pb.Add(bulkItemSize)
		bulkItemSize = 0
		if c.Config.SleepSecondsAfterEachBulk > 0 {
			/*如果程序设置了在每次执行批量插入操作后需要睡眠一段时间，那么程序会通过time.Sleep函数等待指定的时间，以保证 Elasticsearch 能够及时处理批量插入操作。*/
			time.Sleep(time.Duration(c.Config.SleepSecondsAfterEachBulk) * time.Second)
		}
	}
	/*实现了将数据批量插入到 Elasticsearch 中的功能*/
WORKER_DONE:
	/*首先判断 docBuf 里是否有数据*/
	if docBuf.Len() > 0 {
		/*
			这个代码是将 docBuf.Bytes() 的内容写入 mainBuf 中，docBuf 是一个 bytes.Buffer 类型的变量，代表着从 Elasticsearch 中读取到的一条文档。
			在执行完一些数据处理后，将 docBuf 中的内容写入到 mainBuf 变量中，最终会将整个 mainBuf 转储到目标 Elasticsearch 中。
			这个操作实现的是将一条条文档从源 Elasticsearch 中读取出来，最终转储到目标 Elasticsearch 中的过程。主要思路是通过分段读取文档的方式，降低了内存占用和提高了效率。
		*/
		mainBuf.Write(docBuf.Bytes())
		/*在成功插入数据后，将 bulkItemSize 计数器加1*/
		bulkItemSize++
	}
	/*
		通过调用 Bulk 方法来批量插入数据,
		Bulk 方法在执行插入操作时，会读取 mainBuf 中的数据，每次读取一个完整的请求，然后发送给 Elasticsearch。
	*/
	c.TargetESAPI.Bulk(&mainBuf)
	log.Trace("bulk insert")
	/*这段代码中的 pb 表示进度条对象，通过调用 pb.Add 方法来更新进度条的已完成进度。*/
	pb.Add(bulkItemSize)
	bulkItemSize = 0
	/*最后，通过调用 wg.Done() 来告知主线程当前协程已完成任务*/
	wg.Done()
}
