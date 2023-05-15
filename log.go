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
	"strings"

	log "github.com/cihub/seelog"
)

/*
设置日志级别、输出格式和日志位置的函数，使用的是 seelog 库。
函数的参数 logLevel 表示设定的日志级别，例如 info、warning、error 等。
*/
func setInitLogging(logLevel string) {

	/*
		strings.ToLower() 的作用是将一个字符串中的所有字符转换为小写形式。它的函数签名如下： func ToLower(s string) string
	*/
	logLevel = strings.ToLower(logLevel)

	/*
		通过动态生成 XML 配置字符串，将日志的最小级别和文件输出路径进行了动态配置，并定义了日志格式。
		定义了一个名为 testConfig 的字符串变量，这个变量的值是一个 XML 配置字符串，用于配置 seelog 日志系统。
			minlevel 属性的值通过程序动态生成;
			path 属性的值是指定了日志文件的路径;
			format 属性的值是指定日志信息的格式，其中包含了日期、时间、日志级别、文件名和行号、函数名等信息。
	*/
	testConfig := `
	<seelog  type="sync" minlevel="`
	testConfig = testConfig + logLevel
	testConfig = testConfig + `">
		<outputs formatid="main">
			<filter levels="error">
				<file path="./esm.log"/>
			</filter>
			<console formatid="main" />
		</outputs>
		<formats>
			<format id="main" format="[%Date(01-02) %Time] [%LEV] [%File:%Line,%FuncShort] %Msg%n"/>
		</formats>
	</seelog>`

	/*
		通过一个动态生成的 XML 配置字符串来创建一个 seelog 日志系统的 logger 对象。
		log.LoggerFromConfigAsString() 函数使用了 testConfig 变量中存储的 XML 配置字符串来创建一个 logger 对象，并将其赋值给 logger 变量。
		如果创建过程中没有出现错误，那么 err 变量的值将为 nil。如果出现了错误，err 变量的值将是一个非空的 error 对象，可以通过检查该对象的值来确定具体的错误原因。
		这样就可以方便地在 Go 语言代码中使用 seelog 日志系统进行日志记录。
	*/
	logger, err := log.LoggerFromConfigAsString(testConfig)
	if err != nil {
		log.Error("init config error,", err)
	}
	/*
		用新创建的 logger 对象替换当前的默认 logger。
		log.ReplaceLogger() 函数将 logger 变量中存储的 logger 对象替换掉当前默认的 logger 对象，并将操作结果存储在 err 变量中。
		如果没有出现错误，那么 err 变量的值将为 nil，此时新的 logger 对象就已经生效了。
		如果出现了错误，err 变量的值将是一个非空的 error 对象，可以通过检查该对象的值来确定具体的错误原因。
		这样就可以方便地在 Go 语言应用程序中使用自定义的 seelog 日志系统。
	*/
	err = log.ReplaceLogger(logger)
	if err != nil {
		log.Error("init config error,", err)
	}
}
