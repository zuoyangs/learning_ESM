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
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sync"

	"github.com/cheggaaa/pb"
	log "github.com/cihub/seelog"
)

/*检查文件是否存在。*/
func checkFileIsExist(filename string) bool {
	var exist = true
	/*
		os.Stat() 是 go 的一个函数，用于获取文件或目录的基本信息，如文件大小，修改时间，文件权限等。
		os.IsNotExist() 是 go 中的一个函数，用于检查指定的错误是否表示文件或目录不存在的情况。os.IsNotExist(err error) bool 接受一个 error 类型的参数，然后返回一个布尔值。
		当参数 err 表示一个文件或目录不存在的错误时，返回 true，否则返回 false。
	*/
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

/*读取文件并将其解析为 JSON 的函数*/
func (m *Migrator) NewFileReadWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	log.Debug("start reading file")
	f, err := os.Open(m.Config.DumpInputFile)
	if err != nil {
		log.Error(err)
		return
	}

	defer f.Close()
	r := bufio.NewReader(f)
	lineCount := 0
	for {
		/*按行读取一个文件中的数据*/
		line, err := r.ReadString('\n')
		if io.EOF == err || nil != err {
			break
		}
		/*使用计数器变量(lineCount)来记录文件中的行数*/
		lineCount += 1
		js := map[string]interface{}{}

		/*将读取的行数据解析成json数据*/
		err = DecodeJson(line, &js)
		if err != nil {
			log.Error(err)
			continue
		}
		/*发送到通道 m.DocChan*/
		m.DocChan <- js
		/*每读取一行就让进度条(pb)加1*/
		pb.Increment()
	}

	defer f.Close()
	log.Debug("end reading file")
	close(m.DocChan)
	wg.Done()
}

/*用于将 Elasticsearch 中的数据写入到一个文件中*/
func (c *Migrator) NewFileDumpWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	var f *os.File
	var err1 error

	/*判断文件是否存在，如果存在，*/
	if checkFileIsExist(c.Config.DumpOutFile) {
		/*
			os.OpenFile()函数会打开指定路径的 c.Config.DumpOutFile 文件，并返回一个额 *os.File 类型的文件。
			os.O_APPEND：表示在已有的文件内容的后面追加新的内容。
			os.O_WRONLY：表示只写模式打开文件。
			os.ModeAppend：是一个文件的模式，它表示如果文件存在，则在文件末尾追加写操作的数据。
			还有其他的，类似：
				os.O_RDONLY: 表示只读模式打开文件。
				os.O_WRONLY: 表示只写模式打开文件。
				os.O_RDWR:   表示读写模式打开文件。
				os.O_APPEND: 表示在文件末尾添加数据而不是覆盖原有内容。
				os.O_CREATE: 表示如果文件不存在，就创建该文件。
				os.O_TRUNC:  表示打开文件时清空文件内容。
				os.O_EXCL: 与os.O_CREATE同时使用，表示如果文件已经存在，则不进行创建，返回一个错误。
				os.O_SYNC: 打开文件时，每次写入都会被同步到硬盘上，类似于fsync()函数。
				os.O_NONBLOCK: 如果文件无法立即打开，不会等待，而是立即返回一个错误。
			os.ModeAppend 和 os.O_APPEND 区别：
				os.ModeAppend 是文件的模式，指定该模式后程序可以向文件中追加内容，不会清空文件。如果该文件不存在，会创建一个新的文件并拥有该模式。
				os.O_APPEND 则是文件打开的选项，指定该选项后程序可以在文件末尾追加内容，而不是覆盖之前的内容。如果该文件不存在，会返回一个错误。
		*/
		f, err1 = os.OpenFile(c.Config.DumpOutFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err1 != nil {
			log.Error(err1)
			return
		}

	} else {
		f, err1 = os.Create(c.Config.DumpOutFile)
		if err1 != nil {
			log.Error(err1)
			return
		}
	}
	/*
	   这段代码是使用 go 语言标准库中的 bufio 包中的 NewWriter 方法创建一个将数据写入文件 f 中的缓存 Writer 对象 w。
	   通过使用缓存来写入数据可以提高程序写入文件的效率，因为它避免了对文件的多次访问和IO操作次数的减少。 在之后的操作中，使用 w.Write 方法将数据写入缓存中。
	   当缓存满时，它将自动将缓存中的数据刷新到物理磁盘上的文件中。如果想确认数据已经被写入磁盘，可以调用 w.Flush() 方法来刷新缓存并将所有数据写入文件中。
	*/
	w := bufio.NewWriter(f)

READ_DOCS:
	for {

		/*
			从 channel c.DocChan 中读取一个文档并将其赋值给变量 docI；
			变量 open 会返回一个布尔值，用于指示通道是否已经被关闭。
			这样可以避免在通道关闭前读取到空值。如果没有文档可读取或者通道已经关闭，那么程序将结束循环并退出。
		*/
		docI, open := <-c.DocChan
		/*
			程序将检查文档 docI 中是否包含一个名为 status 的键。如果包含，那么将检查 status 的值是否为 404。
			这个检查主要是用来处理 Elasticsearch 查询时出现的错误，避免这些错误对程序的正常运行造成影响。
		*/
		if status, ok := docI["status"]; ok {
			/*如果是，那么表明在查询 Elasticsearch 索引时出现了错误。*/
			if status.(int) == 404 {
				/*程序将打印一条错误日志，并执行 continue 语句，继续循环读取下一个文档。*/
				log.Error("error: ", docI["response"])
				continue
			}
		}

		/*
			对读取到的文档数据进行完整性检查，确保其中包含了 Elasticsearch 数据库中所必需的字段，以便程序可以正确地处理数据。
			代码通过遍历一个包含字段名的字符串数组 {"_index", "_type", "_source", "_id"} ，逐一检查文档 docI 是否包含了这些字段。
		*/
		for _, key := range []string{"_index", "_type", "_source", "_id"} {
			/*
				如果文档中缺少了任何一个必需的字段，那么 ok 变量将返回 false，并通过 break 语句中断循环以继续读取下一个文档。
				这样的检查可以确保程序不会处理缺少必要字段的文档，从而避免出现错误。例如，如果文档中缺少 _id 字段，那么程序在处理文档时可能会引发异常。
				因此，通过进行必要的完整性检查，可以保证程序在处理数据时能够正确地读取和使用必要的字段。
			*/
			if _, ok := docI[key]; !ok {
				break READ_DOCS
			}
		}

		/*将 docI 这个数据结构编码为 JSON 格式的字节数组，其中 jsr 表示 JSON 序列化之后的字节数组，err 则是 json.Marshal 调用过程中可能出现的错误。*/
		jsr, err := json.Marshal(docI)
		/*
			将 JSON 序列化之后的字节数组转换成字符串形式，并打印出来。
			log.Trace 是 log 包中提供的一个日志级别，用于记录比 Debug 更详细的日志信息。
			log 包提供了六个不同级别的日志记录方法，分别是：Trace、Debug、Info、Warn、Error 和 Fatal。
		*/
		log.Trace(string(jsr))
		if err != nil {
			log.Error(err)
		}
		/*
			将 jsr 的内容以字符串的形式写入到 n 中。
			w 表示一个实现了 io.Writer 接口的对象，通过 WriteString 方法将 jsr 内容转换为字符串并写入 w 中。最终函数返回写入的字节数和可能出现的错误信息。
		*/
		n, err := w.WriteString(string(jsr))
		if err != nil {
			log.Error(n, err)
		}
		w.WriteString("\n")
		pb.Increment()

		// if channel is closed flush and gtfo
		if !open {
			goto WORKER_DONE
		}
	}

WORKER_DONE:
	w.Flush()
	f.Close()

	wg.Done()
	log.Debug("file dump finished")
}
