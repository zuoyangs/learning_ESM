package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	_ "runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	log "github.com/cihub/seelog"
	goflags "github.com/jessevdk/go-flags"
	"github.com/mattn/go-isatty"
)

func main() {

	/*
		调用 Go 的 runtime 包中的两个函数：GOMAXPROCS 和 NumCPU。
		NumCPU 函数返回当前机器的 CPU 核心数量，
		GOMAXPROCS 则用于设置程序的最大可用 CPU 核心数量。
		通过将 GOMAXPROCS 的返回值设置为 NumCPU 的返回值，可以让程序在多核 CPU 上实现并行处理，提高程序的性能。
	*/
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 开启一个 goroutine
	go func() {

		//log.Infof("pprof listen at: http://%s/debug/pprof/", app.httpprof)
		//http.NewServeMux() 函数创建了一个可以注册处理程序的新的 ServeMux 实例
		mux := http.NewServeMux()

		/*
			使用 Gorilla Mux 路由框架创建一个路由，并处理 "/debug/pprof/" 的请求。处理函数使用了默认的 ServeMux，用于将请求路由到相应的处理器。
			用来进行性能剖析和调试，便于查看应用程序的资源使用情况和性能瓶颈。
		*/
		mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
			http.DefaultServeMux.ServeHTTP(w, r)
		})

		// register metrics handler
		// 这段代码的作用是注册一个 HTTP 的 pprof 处理器，用于性能分析。在该处理器中，我们采用默认的 ServeMux 对象来处理 HTTP 请求，并将其绑定到 /debug/pprof/ 路径下
		endpoint := http.ListenAndServe("0.0.0.0:6060", mux)

		// 打印了一条日志，表明 pprof 服务器已经停止运行
		log.Debug("stop pprof server: %v", endpoint)
	}()

	var err error

	/*
		定义了一个 Config 结构体。
		c := &Config{}这行代码的作用是创建了一个Config类型的实例，然后将该实例的地址分配给了变量c。
		这个过程被称为取地址操作或者创建指针，这样也可以直接使用指针对结构体进行操作，优化性能和节约内存。
	*/
	c := &Config{}
	//定义了一个 Migrator 结构体
	migrator := Migrator{}
	//将 Migrator 的 Config 字段设置为一个 Config 结构体
	migrator.Config = c

	/*
		使用 goflags "github.com/jessevdk/go-flags" 包解析 c 变量（类型为指向 Config 结构体的指针），
		通过执行 goflags.Parse(c)，将命令行参数解析并赋值给 c，同时也会检查参数是否存在无效的选项或参数，
		需要注意的是，此处将解析结果的第一个值赋值给空标识符 _，表示忽略该值，仅仅只是为了符合 goflags 包的函数签名，实际用处为解析命令行参数并更新 c 对应的属性。
	*/
	_, err = goflags.Parse(c)

	// 如果解析失败（即 err != nil），则将错误日志输出并返回。
	if err != nil {
		log.Error(err)
		return
	}

	//初始化日志记录器
	setInitLogging(c.LogLevel)

	// 判断用户是否传入必要参数
	if len(c.SourceEs) == 0 && len(c.DumpInputFile) == 0 {
		log.Error("no input, type --help for more details")
		return
	}
	if len(c.TargetEs) == 0 && len(c.DumpOutFile) == 0 {
		log.Error("no output, type --help for more details")
		return
	}

	if c.SourceEs == c.TargetEs && c.SourceIndexNames == c.TargetIndexName {
		log.Error("migration output is the same as the output")
		return
	}

	// 首先检查标准输出是否为Terminal或者CygwinTerminal。如果是，则showBar变量被设置为true，否则被设置为false。
	var showBar bool = false
	if isatty.IsTerminal(os.Stdout.Fd()) {
		showBar = true
	} else if isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		showBar = true
	} else {
		showBar = false
	}

	//至少输出一次
	if c.RepeatOutputTimes < 1 {
		c.RepeatOutputTimes = 1
	} else {
		log.Info("source data will repeat send to target: ", c.RepeatOutputTimes, " times, the document id will be regenerated.")
	}

	if c.RepeatOutputTimes > 0 {

		for i := 0; i < c.RepeatOutputTimes; i++ {

			if c.RepeatOutputTimes > 1 {
				log.Info("repeat round: ", i+1)
			}

			/*
				该代码初始化了一个有缓冲的通道，用于在不同的goroutine之间传递文档。
				通道的类型为map[string]interface{}，缓冲区大小为c.BufferCount。这里的map类型可以容纳任何键值对，以便存储不同类型的Elasticsearch文档。
			*/
			migrator.DocChan = make(chan map[string]interface{}, c.BufferCount)

			/*
				定义了1个名为 srcESVersion 的指针类型变量，类型为 *ClusterVersion ,
				该变量用于存储源 Elasticsearch 集群的版本信息。
			*/
			var srcESVersion *ClusterVersion

			// create a progressbar and start a docCount
			/*
				定义了一个名为outputBar的指针类型变量，类型为pb.ProgressBar。该变量用于创建一个带前缀为“Output”的新进度条，并将其分配给outputBar变量，
				pb是一个外部包，github.com/cheggaaa/pb 的简称，提供了一个易于使用的进度条和计时器。在此处它被用于创建一个新的进度条以显示输出的进度。
			*/
			var outputBar *pb.ProgressBar = pb.New(1).Prefix("Output ")

			/*
				这行代码的含义是声明了一个名为fetchBar的变量并初始化一个进度条,
				这个进度条是使用第三方库pb中的函数New创建的，其中传入的参数表示进度条的总长度为 1，
				进度条的前缀设置为"Scroll"。
			*/
			var fetchBar = pb.New(1).Prefix("Scroll")

			wg := sync.WaitGroup{}

			//dealing with input
			// 处理从输入源（c.SourceEs）读取数据并进行处理,检查c.SourceEs是否有值，如果有，则会根据输入源的类型选择不同的处理方式
			if len(c.SourceEs) > 0 {

				//dealing with basic auth
				/*
					这个条件语句的作用是检查c.SourceEsAuthStr的长度是否大于0，以及是否包含冒号。
					其中len()函数用于获取字符串的长度，strings.Contains()函数用于判断一个字符串是否包含另一个字符串。
					如果这个条件满足，那么就会执行相应的操作。在这个程序中，这个条件语句的作用是判断是否需要对输入源进行身份验证，如果需要，那么进入相应的身份验证流程。
				*/
				if len(c.SourceEsAuthStr) > 0 && strings.Contains(c.SourceEsAuthStr, ":") {

					/*
						判断成功后，就把身份认证所需的用户名和密码分别放入Auth结构体的User和Pass字段中，并将该结构体赋值给migrator.SourceAuth变量
						strings.Split()方法将字符串c.SourceEsAuthStr按照冒号拆分为两个字符串，并将它们存入一个名为authArray的字符串切片中，
						然后通过切片的索引访问这两个字符串获取用户名和密码，并分别赋值给Auth结构体中的User和Pass字段
					*/
					authArray := strings.Split(c.SourceEsAuthStr, ":")
					auth := Auth{User: authArray[0], Pass: authArray[1]}
					migrator.SourceAuth = &auth
				}

				//get source es version
				/*
					获取输入源ES的版本并根据版本创建相应的API对象
					该方法的第一个参数是输入源ES的地址，第二个参数是用于身份认证（如果需要）的Auth结构体指针，第三个参数是ES的代理地址（如果有的话）
				*/
				srcESVersion, errs := migrator.ClusterVersion(c.SourceEs, migrator.SourceAuth, migrator.Config.SourceProxy)
				if errs != nil {
					return
				}

				/*
					针对 es 5、6、7 不同的 API 对象，将 Host，Compress，Auth，HttpProxy等属性进行赋值。
					最后将创建好的 API 对象，分别赋值给 migrator.SourceESAPI，这么做的主要原因是因为 ES 在不同版本的 API 接口发生了变化。
				*/
				if strings.HasPrefix(srcESVersion.Version.Number, "7.") {
					log.Debug("source es is V7,", srcESVersion.Version.Number)
					api := new(ESAPIV7)
					api.Host = c.SourceEs
					api.Compress = c.Compress
					api.Auth = migrator.SourceAuth
					api.HttpProxy = migrator.Config.SourceProxy
					migrator.SourceESAPI = api
				} else if strings.HasPrefix(srcESVersion.Version.Number, "6.") {
					log.Debug("source es is V6,", srcESVersion.Version.Number)
					api := new(ESAPIV6)
					api.Compress = c.Compress
					api.Host = c.SourceEs
					api.Auth = migrator.SourceAuth
					api.HttpProxy = migrator.Config.SourceProxy
					migrator.SourceESAPI = api
				} else if strings.HasPrefix(srcESVersion.Version.Number, "5.") {
					log.Debug("source es is V5,", srcESVersion.Version.Number)
					api := new(ESAPIV5)
					api.Host = c.SourceEs
					api.Compress = c.Compress
					api.Auth = migrator.SourceAuth
					api.HttpProxy = migrator.Config.SourceProxy
					migrator.SourceESAPI = api
				} else {
					log.Debug("source es is not V5,", srcESVersion.Version.Number)
					api := new(ESAPIV0)
					api.Host = c.SourceEs
					api.Compress = c.Compress
					api.Auth = migrator.SourceAuth
					api.HttpProxy = migrator.Config.SourceProxy
					migrator.SourceESAPI = api
				}

				// 确保 c.ScrollSliceSize 属性小于1时候，置位1
				if c.ScrollSliceSize < 1 {
					c.ScrollSliceSize = 1
				}

				//根据输入源(c)中设定的游标(Cursor)大小，分批从源ES中读取数据
				totalSize := 0
				finishedSlice := 0

				/*
					在进行数据迁移的过程中，通过 Elasticsearch 的 scroll API 来批量拉取原索引中的文档数据，并分片进行处理。
					具体来说，循环的次数是由 c.ScrollSliceSize 变量决定的，每一次循环通过 NewScroll 方法来创建一个 scroll 对象，
					该对象会拉取一个固定数量（c.DocBufferCount）的文档数据，并在固定的时间内（c.ScrollTime）内保持数据的可访问性，以供后续查看和处理。
					Query 参数是查询条件，slice 和 c.ScrollSliceSize 用来确定当前循环处理的块的起始位置和大小（分片）。
					Fields 参数则指定了需要获取的字段列表。
				*/
				for slice := 0; slice < c.ScrollSliceSize; slice++ {
					scroll, err := migrator.SourceESAPI.NewScroll(c.SourceIndexNames, c.ScrollTime, c.DocBufferCount, c.Query, slice, c.ScrollSliceSize, c.Fields)
					//在每一次循环中，如果创建 scroll 对象失败，会输出错误日志并退出函数。
					if err != nil {
						log.Error(err)
						return
					}

					/*
						将 scroll 对象转换为实现了 ScrollAPI 接口的类型temp,
						把 scroll 对象强制转换为 ScrollAPI 接口类型，我们就可以在之后对这个对象进行更高级别的操作和处理，而不用担心会发生类型错误。
					*/
					temp := scroll.(ScrollAPI)

					/*
						累加每个分片中查询结果的总命中数，因此需要将每个分片中命中数相加并赋值给 totalSize 变量。
						其中 temp 是一个存储查询结果的结构体，它包含了分片的查询结果信息，包括总命中数，命中的数据文档等。
						GetHitsTotal() 是 temp 结构体的一个方法，用于获取该分片查询结果的总命中数。
					*/
					totalSize += temp.GetHitsTotal()

					/*
						判断当前查询结果是否有命中的文档，并且是否使用了 scroll 参数。
						其中，scroll 参数是一种分批获取数据的方式，它可以在 Elasticsearch 中实现快速滚动查询,
						temp.GetDocs() 是一个方法，用于获取当前查询结果命中的文档列表。
					*/
					if scroll != nil && temp.GetDocs() != nil {

						/*
							temp.GetHitsTotal() 是一个方法，用于获取当前查询结果的总命中数。
							如果该命中数为0，则说明没有任何文档被查询到，此时就会输出错误日志信息并退出程序执行。
							这样可以避免后续处理数据的代码因为没有任何文档的情况而出现异常。
						*/
						if temp.GetHitsTotal() == 0 {
							log.Error("can't find documents from source.")
							return
						}

						//一个匿名的 go 协程.用语处理数据的逻辑。这里创建了协程，是为了方便数据读取和处理的过程可以并发执行，提高程序运行效率。
						go func() {

							//调用 wg.Add(1) 方法，将 WaitGroup 的计数器加1，表示有一个任务需要等待完成。
							wg.Add(1)

							/*
								开始处理当前查询结果集中的所有文档。该方法会处理当前查询结果集中第一页的数据，
								并将第一页的数据放入到 migrator.DocChan 通道中。DocChan 是一个用于存储要处理的数据文档的通道。
							*/
							temp.ProcessScrollResult(&migrator, fetchBar)

							/*
								for循环是个无限循环，直到 temp.Next(&migrator, fetchBar) 函数返回 true 才会退出循环。
								进入下一页面的查询结果，并将查询结果中的文档放入 migrator.DocChan 通道中。
								如果返回 false，则说明查询结果已经全部读取完成。
							*/
							for temp.Next(&migrator, fetchBar) == false {
							}

							/*
								用于控制台中输出进度条。
								如果需要显示进度条，则会调用 fetchBar.Finish() 方法，这个方法会结束当前进度条的显示。
								如果 showBar 为 false，则不会有任何输出。
								这种参数化的设计模式可以提高代码的可复用性和灵活性，使得代码更易于扩展和维护。
							*/
							if showBar {
								fetchBar.Finish()
							}

							// finished, close doc chan and wait for goroutines to be done
							// wg.Done() 函数会通知 WaitGroup 程序，表示一个goroutine已经完成了任务，从而维护 WaitGroup 中的计数器。
							wg.Done()

							// finishedSlice 变量会自增1，用于表示已经完成的数据块数量.
							finishedSlice++

							//clean up final results
							//如果finishedSlice 的值等于 c.ScrollSliceSize，则意味着已经处理完了指定数量的数据块，就需要进行最后一些清理工作了。
							if finishedSlice == c.ScrollSliceSize {
								log.Debug("closing doc chan")

								/*
									会调用 close(migrator.DocChan) 函数来关闭 DocChan 通道，这样就能通知后台 goroutine 停止工作。
									最终结果是通过该代码段来正确关闭和整理迁移过程中的各种任务和资源，以确保程序能够正常地结束。
								*/
								close(migrator.DocChan)
							}
						}()
					}
				}

				/*
					在设置进度条的总进度值。其中，totalSize 是数据的总大小，如果大于 0，则将其设置为进度条的总值。
					fetchBar 和 outputBar 是两个进度条对象，分别用来表示数据 fetch（获取）和 output（输出）的进度。
					通过设置 Total 属性，可以让进度条显示正确的总进度。
				*/
				if totalSize > 0 {
					fetchBar.Total = int64(totalSize)
					outputBar.Total = int64(totalSize)
				}

				// 判断是否指定了输入文件 c.DumpInputFile，如果指定了，则会将文件内容读取出来，并根据读取到的行数创建相应数量的进度条。
			} else if len(c.DumpInputFile) > 0 {

				//read file stream
				/*
					wg.Add(1) 是用来向 WaitGroup 中添加一个任务计数的。
					这里为什么要 wg.Add(1)呢，是因为后面，需要 open(c.DumpInputFile)。
					所以，这个任务是用来读取文件数据并进行处理的，并且需要等待这个任务完成才能执行 wg.Wait() 后面的代码。
				*/
				wg.Add(1)
				f, err := os.Open(c.DumpInputFile)
				if err != nil {
					log.Error(err)
					return
				}
				//get file lines
				lineCount := 0
				defer f.Close()

				/*
					go 语言中的 `bufio` 包提供了带缓冲的阅读器（`bufio.Reader`）和写入器（`bufio.Writer`），用于在程序和 I/O 设备之间更高效地传输数据。
					`bufio.NewReader()` 是 `bufio` 包中用于创建读取器对象的一个函数。它将一个实现了 `io.Reader` 接口的对象 `f` 转换为一个具有读取缓冲区的读取器对象。
					该函数会返回一个 `*bufio.Reader` 类型的指针，你可以使用它的 `Read()` 方法从缓冲区中读取数据。
					在使用 `bufio.Reader` 时，最好使用它提供的带缓冲的读取方式，因为这种读取方式会减少与 I/O 设备的读写次数，从而提高程序的效率。
					通常情况下，你可以使用 `bufio.NewReader()` 创建一个默认缓冲区大小的带缓冲的读取器对象，接着可以使用 `io.Copy()` 或 `reader.Read()` 函数从底层读取器中读取数据。这样可以提高程序的效率。
					在这里使用 `bufio.NewReader()` 将文件转换为 `*bufio.Reader` 对象，可以方便地逐行读取文件内容。
				*/
				r := bufio.NewReader(f)
				for {

					/*
						ReadString() 函数通常用于逐行读取文本文件。通过传递换行符 \n 作为分隔符，可以依次读取每行文本内容。
						ReadString() 函数在读取到指定的分隔符后会将其包含在读取到的字符串中返回。
						注意：ReadString() 函数返回的字符串包含结尾的换行符。
					*/
					_, err := r.ReadString('\n')

					/*
						在读取数据时，当读取到文件末尾时，程序会返回EOF（End Of File）错误，此时我们需要停止读取。
						如果发生了 nil != err，我们需要退出循环并打印错误信息，以避免程序处理错误数据。
					*/
					if io.EOF == err || nil != err {
						break
					}
					lineCount += 1
				}

				/*
					logrus 中的 Trace() 方法，用于输出记录日志等级的 Trace 日志信息和行数 lineCount。通过记录日志，
					我们可以知道当前程序正在执行到哪一行代码，以及在解决程序问题时有所帮助。
					通常日志的等级包括 Debug、Info、Warning、Error 和 Fatal 等，开发者可以根据需要选择合适的等级进行记录。
				*/
				log.Trace("file line,", lineCount)

				/*
					progress bars(pbar) 中的 pb.New() 方法，用于创建两个进度条，一个用于读取文件进度（fetchBar），另一个用于输出进度（outputBar）。
					其中参数 lineCount 表示进度条的总长度。方法 Prefix() 用于为进度条添加前缀，使进度条更易于理解。
					通过这些进度条，我们可以清楚地了解程序执行的进度，以及任务的完成情况，从而更好地监控程序运行状态，快速了解和解决问题。
				*/
				fetchBar := pb.New(lineCount).Prefix("Read")
				outputBar = pb.New(lineCount).Prefix("Output ")

				f.Close()

				/*
					创建一个文件读取工作器，使用了名为 migrator.NewFileReadWorker 的函数。该函数的参数包括两个，分别是 fetchBar 和 wg。
					其中，fetchBar 是进度条组件，用于显示工作进度；wg 是同步等待组件(在 Go 语言中通常称为 WaitGroup )，用于等待所有协程完成其工作。
				*/
				go migrator.NewFileReadWorker(fetchBar, &wg)

			}

			/*
				定义一个名为 pool 的指针变量，它的类型是 *pb.Pool。简单来说，它表示一个 pb.Pool 类型的指针，
				即指向一个进度池（Pool）对象的指针。
			*/
			var pool *pb.Pool

			//只有当 showBar 为 true 时才创建和启动进度条池。如果该变量为 false，则不会进行进度条相关的操作。
			if showBar {

				/*
					使用 pb.StartPool() 函数来创建进度条池，并将组件 fetchBar 和 outputBar 作为参数传递给该函数。
					如果出现错误，则会触发 panic 异常，使程序崩溃。同时，函数还会返回一个指向进度条池的指针 pool，以便后续使用。
				*/
				pool, err = pb.StartPool(fetchBar, outputBar)
				if err != nil {
					panic(err)
				}
			}

			/*
				检查是否设置了目标 Elasticsearch 的认证信息。
					如果设置了认证信息，则将认证信息放入 migrator.TargetAuth 中，其中 c.TargetEsAuthStr 表示认证信息，格式为 "username:password"。
					如果没有设置认证信息，则 migrator.TargetAuth 为 nil，即不需要进行认证。
			*/
			if len(c.TargetEs) > 0 {

				/*
					判断 c.TargetEsAuthStr 是否含有冒号是为了确定它是否包含认证信息（通常是用户名和密码），冒号用于分隔用户名和密码。
					如果包含认证信息，那么可以将其拆分成用户名和密码，以便后续进行目标Elasticsearch的连接认证。
				*/
				if len(c.TargetEsAuthStr) > 0 && strings.Contains(c.TargetEsAuthStr, ":") {
					authArray := strings.Split(c.TargetEsAuthStr, ":")
					auth := Auth{User: authArray[0], Pass: authArray[1]}
					migrator.TargetAuth = &auth
				}

				/*
					通过调用该函数，程序可以获得目标Elasticsearch集群的版本信息，以便后续进行版本兼容性检查和其他操作。
					同时，如果在执行函数时出现了错误，这些错误也会被捕获并存储在 errs 变量中，以便稍后检查和处理。
					这段代码是调用了 migrator 包中的 ClusterVersion 函数，并传递了三个参数：c.TargetEs、migrator.TargetAuth 和 migrator.Config.TargetProxy。
					该函数的作用是获取目标 Elasticsearch 集群的版本信息和可能的错误。其中，
					c.TargetEs 是目标Elasticsearch地址（包括协议、主机名和端口号），
					migrator.TargetAuth 是包含身份验证信息的字符串（通常是用户名和密码），
					migrator.Config.TargetProxy 是HTTP代理服务器地址（如果需要的话）。
				*/
				descESVersion, errs := migrator.ClusterVersion(c.TargetEs, migrator.TargetAuth, migrator.Config.TargetProxy)
				if errs != nil {
					return
				}

				/*
					根据 Elasticsearch 版本号选择对应版本的 API 实现，如果版本号以 7 开头，则 api := new(ESAPIV7)，
					最后将 api 对象存储到 migrator.TargetESAPI 变量中。
				*/
				if strings.HasPrefix(descESVersion.Version.Number, "7.") {
					log.Debug("target es is V7,", descESVersion.Version.Number)
					api := new(ESAPIV7)
					api.Host = c.TargetEs
					api.Auth = migrator.TargetAuth
					api.HttpProxy = migrator.Config.TargetProxy
					migrator.TargetESAPI = api
				} else if strings.HasPrefix(descESVersion.Version.Number, "6.") {
					log.Debug("target es is V6,", descESVersion.Version.Number)
					api := new(ESAPIV6)
					api.Host = c.TargetEs
					api.Auth = migrator.TargetAuth
					api.HttpProxy = migrator.Config.TargetProxy
					migrator.TargetESAPI = api
				} else if strings.HasPrefix(descESVersion.Version.Number, "5.") {
					log.Debug("target es is V5,", descESVersion.Version.Number)
					api := new(ESAPIV5)
					api.Host = c.TargetEs
					api.Auth = migrator.TargetAuth
					api.HttpProxy = migrator.Config.TargetProxy
					migrator.TargetESAPI = api
				} else {
					//如果版本不是 5，6，7，则 api := new(ESAPIV0)
					log.Debug("target es is not V5,", descESVersion.Version.Number)
					api := new(ESAPIV0)
					api.Host = c.TargetEs
					api.Auth = migrator.TargetAuth
					api.HttpProxy = migrator.Config.TargetProxy
					migrator.TargetESAPI = api

				}

				log.Debug("start process with mappings")

				/*
					srcESVersion 不为 nil。是说，在进行索引迁移之前，需要获取源索引的 Elasticsearch 版本信息。
					c.CopyIndexMappings 为 true。是说，需要进行索引映射的迁移操作。如果不需要进行索引映射的迁移操作，则该代码块不会执行。
					descESVersion.Version.Number[0] 与 srcESVersion.Version.Number[0] 不相等, 是说，
					源索引和目标索引使用的 Elasticsearch 主版本号不同。这可能会导致索引映射的迁移失败。
					如果任意一条判断失败，则直接返回，不进入 if 内部执行。
				*/
				if srcESVersion != nil && c.CopyIndexMappings && descESVersion.Version.Number[0] != srcESVersion.Version.Number[0] {
					//如果条件都满足，则会输出下面这条错误信息：提示用户无法进行跨大版本的映射数据迁移，需要用户自行手动更新映射配置文件。
					log.Error(srcESVersion.Version, "=>", descESVersion.Version, ",cross-big-version mapping migration not avaiable, please update mapping manually :(")
					return
				}

				/*
					实现了一个定时器和循环，用于检查数据迁移所需的两个 Elasticsearch 集群是否就绪，如果不就绪则等待一段时间后再次检查。
					c.SourceEs 是一个源 Elasticsearch 集群的 URL 地址列表，
					migrator 是一个迁移器对象，migrator.ClusterReady() 方法用于检测指定 Elasticsearch 集群的状态是否正常，
					如果正常则返回一个状态对象和一个布尔值，如果布尔值为真则表示集群状态正常。
					如果集群状态不正常，则会等待一段时间后再次检测。
					定义了一个闲置时间阈值 idleDuration，time.Duration 表示时间段，在这里，它的单位是 秒（time.Second），表示 1 秒钟的持续时间。因此 idleDuration 表示持续 3 秒。
				*/
				idleDuration := 3 * time.Second
				/*
					创建一个名为 timer 的定时器，它将在 idleDuration 时间之后触发。
					在 go 语言中，我们可以使用 time.NewTimer(idleDuration) 函数创建一个定时器 ，其中 idleDuration 参数表示定时器的持续时间。
					当定时器的持续时间到到时，定时器将自动出阿发并向其通道（即 time.C）发送一个时间值。
					我们可以通过 <-time.C 语句从通道中接受时间值来等待定时器触发。
					在这后续的代码中，我们可以打看到 timer.Reset() 函数，执行这个函数，则定时器会重新设置持续时间为 idleDuration 。
				*/
				timer := time.NewTimer(idleDuration)

				/*
					Timer 是 Go 语言标准库 time 包中的一种类型，表示了一个定时器。每当我们需要在一定时间后执行某些操作时，就可以使用 Timer 类型。
					Timer 类型的常规用法是，向定时器发送一个 time.Duration【djʊˈreɪʃn】 类型的时间间隔，然后等待这个时间间隔过后，可以读取定时器的通道 C，并从中获取当前时间。
					C 是一个只读通道，使用 <-timer.C 语法可以从定时器读取当前时间。在定时器到期前，读取 C 会被阻塞。
					Timer type 分为两个字段：C 和 r。其中，
					C 是 <-chan Time 类型的只读通道，用于定时器到期后发送当前时间值；
					r 是 runtimeTimer 类型，是实际的底层计时器。
						type Timer struct {
							C <-chan Time
							r runtimeTimer
						}

						func (t *Timer) Stop() bool {
							if t.r.f == nil {
								panic("time: Stop called on uninitialized Timer")
							}
							return stopTimer(&t.r)
						}
					timer.Stop() 是 time.Timer 类型的一个方法，用于停止当前计时器的执行。
					如果计时器尚未执行或已经到期，则 timer.Stop() 操作将返回 false，否则返回 true。
				*/
				defer timer.Stop()

				/*
					通过一个无限循环体，不断进行以下逻辑判断：当源集群 c.SourceEs 不为空时，检查其是否就绪，
					若不就绪则等待指定时间 idleDuration 后再次检查；同样的逻辑判断也适用于目标集群 c.TargetEs。
				*/
				for {
					//timer.Reset() 方法来重新设置定时器的超时时间，从而让定时器重新计时
					timer.Reset(idleDuration)
					//判断源 ES 是否就绪进行数据迁移。如果源 ES 尚未就绪，代码将重复检查直到就绪。
					if len(c.SourceEs) > 0 {
						/*
							migrator.ClusterReady 的作用是检查给定的 ES 是否可以使用，如果不行，则返回错误。如果 ES 正常运行，
							就会返回 ES 集群的名称和状态信息。如果集群不可用，就会返回错误信息。
						*/
						if status, ready := migrator.ClusterReady(migrator.SourceESAPI); !ready {
							log.Infof("%s at %s is %s, delaying migration ", status.Name, c.SourceEs, status.Status)

							/*
								将 timer.C 通道（即计时器）传给 <- 操作符。<-timer.C 表示从 timer.C 通道接收到了一个值，这个操作将会阻塞，
								一直等到 timer.C 通道发出通知。通常情况下，这种用法可以用来实现定时操作，让程序在指定时间点做出响应。
								在这里，当源 ES 集群不可用时，程序会暂停执行直到计时器发出通知，继而再次尝试操作 ES 集群。
							*/
							<-timer.C
							continue
						}
					}

					/*
						首先判断目标 ES 集群的数量是否大于0。
						这段代码的意义在于，在进行数据迁移之前，确保目标 ES 集群就绪，以避免数据迁移失败。
					*/
					if len(c.TargetEs) > 0 {
						//如果是，则使用 ClusterReady 方法检查目标 ES 集群的状态是否就绪。
						if status, ready := migrator.ClusterReady(migrator.TargetESAPI); !ready {
							log.Infof("%s at %s is %s, delaying migration ", status.Name, c.TargetEs, status.Status)
							<-timer.C
							continue
						}
					}
					//如果源、目标 ES 集群就绪，则退出循环。
					break
				}

				if len(c.SourceEs) > 0 {

					/*
						调用 migrator.SourceESAPI 的 GetIndexMappings 方法，并存储在 indexNames, indexCount 中，
						c.CopyAllIndexes 是个布尔值，表示是否复制所有的索引。
						c.SourceIndexNames 是一个字符串切片，里面存储了要复制的索引名称列表,
						两个参数作用，帮助确定在源 Elasticsearch 中需要复制哪些索引。
						具体见 domain.go 的
						type config struct{
							...
						 		SourceIndexNames    string `short:"x" long:"src_indexes" description:"indexes name to copy,support regex and comma separated list" default:"_all"`
						 		CopyAllIndexes      bool   `short:"a" long:"all"     description:"copy indexes starting with . and _"`
						 	...
						}
					*/
					indexNames, indexCount, sourceIndexMappings, err := migrator.SourceESAPI.GetIndexMappings(c.CopyAllIndexes, c.SourceIndexNames)

					if err != nil {
						log.Error(err)
						return
					}

					/*
						map[string]interface{}{} 表示一个空的字典类型。这是因为 map 是一种数据结构，用于存储键值对，需要指定键和值的类型。
						在这种情况下，键类型是 string，值类型是 interface{}，{} 表示没有初始化时的初始状态，因此表示空字典类型。

						定义了一个 sourceIndexRefreshSettings 变量，它是一个空的字典类型，用于存储源 Elasticsearch 中每个索引的刷新设置。
						在后面的代码中，这个变量将被用来获取每个索引的刷新设置，并且在新的 Elasticsearch 中设置相应的刷新间隔。
						如果不设置这个变量，新的 Elasticsearch 中的索引会继承源 Elasticsearch 中索引的默认刷新间隔，这可能导致性能问题或数据丢失。
						因此，在迁移时，需要确定新的 Elasticsearch 中每个索引的刷新间隔，以确保数据完整、一致和稳定。
						Elasticsearch 中的索引刷新是指将事务性操作（例如插入、更新、删除等）写入磁盘，并使其在内部数据结构（称为 segment）中可搜索。
						PUT index_name/_settings
							{
							  "index": {
							    "refresh_interval": "5s"
							  }
							}
					*/
					sourceIndexRefreshSettings := map[string]interface{}{}

					/*
						log 包是 Go 语言标准库中的一个包，主要负责打印日志信息。
						log.Debugf() 实际上是使用 log 包的 Debugf() 方法打印格式化的调试信息。
						log 包提供了三个级别的日志输出方法：Print、Panic 和 Fatal。其中，
							Print 用于输出普通的调试信息，
							Panic 用于输出错误信息并导致程序崩溃，
							Fatal 用于输出严重的错误信息并使程序退出。
						在这些方法中，还有各种格式化输出的方式，如 Printf、Println、Panicf、Panicln、Fatalf 和 Fatalln 等。
						log 包还提供了 Logger 类型，它可以通过设置输出级别和自定义输出格式等来实现更高级别的日志记录。
						我们可以使用 New 方法创建一个 Logger 类型的实例，然后使用它的各种输出方法来记录日志，如 logger.Printf。

						Debugf() 方法是 log 包中的一个方法，它提供了与 Printf() 方法类似的函数签名，可以使用任意数量的参数进行格式化输出，
						并将输出信息打印到日志中。因此，它是一种格式化输出的方式，用于输出调试级别的日志信息。

						需要注意的是，Debugf() 方法只有在程序使用了 Debug 级别的输出级别时才会输出，否则这些信息不会出现在日志中。
						因此，Debugf() 方法常常被用于输出一些只在调试时有用的信息，从而避免在正式环境中产生过多的日志信息。
					*/
					log.Debugf("indexCount: %d", indexCount)

					if indexCount > 0 {

						//override indexnames to be copy
						/*
							在 Elasticsearch 迁移代码实现中，
							c.SourceIndexNames 是一个切片，它包含要从源群集复制的索引名称。
							如果在执行迁移操作之前指定了要复制的索引名称，这个切片将被设置为非空值。
							在这种情况下，如果 indexCount 大于 0，也就是复制的索引数大于 0，那么就需要覆盖现有的 c.SourceIndexNames 值。
							代码 c.SourceIndexNames = indexNames 的作用是使用 indexNames 重写 c.SourceIndexNames 索引名称切片，以便在迁移操作期间复制正确的索引。
						*/
						c.SourceIndexNames = indexNames

						// copy index settings if user asked
						/*
							根据用户是否要求复制索引设置，或者是否指定了要复制的分片数，对索引设置进行复制，并获取源索引的设置。
							如果用户特别指定了要复制索引设置，或者指定了要复制的分片数，则需要将索引设置从源索引复制到目标索引。

							换种方式来说，如果用户通过设置 c.CopyIndexSettings 为 true 来特别指定要复制索引设置，或者通过 c.ShardsCount 指定要复制的分片数，那么就需要复制索引设置。
							在这种情况下，代码会调用 Elasticsearch 的 RESTful API，获取源索引的设置，保存在 sourceIndexSettings 变量中，以供后面的代码使用。

						*/
						if c.CopyIndexSettings || c.ShardsCount > 0 {
							log.Info("start settings/mappings migration..")

							//get source index settings
							/*
								在获取 Elasticsearch 中指定索引的设置。
								migrator.SourceESAPI 是 Elasticsearch 的 API 客户端，
								GetIndexSettings 是它提供的一个获取索引设置的方法。
								c.SourceIndexNames 就是指定的源索引名称。
								sourceIndexSettings 则是返回的 IndexSettings 结构体指针。
							*/
							var sourceIndexSettings *Indexes
							sourceIndexSettings, err := migrator.SourceESAPI.GetIndexSettings(c.SourceIndexNames)
							log.Debug("source index settings:", sourceIndexSettings)
							if err != nil {

								/*
									如果在获取源索引设置的过程中出现错误，代码会将错误信息记录在日志文件中，log.Error(err)并不会让程序停止下来，程序会继续执行。
								*/
								log.Error(err)
								/*
									如果获取源索引设置的过程中出现了错误，为了避免继续执行出错，我们使用log.Error函数记录错误信息，并使用 return 语句使函数立即结束执行，程序会退出。
								*/
								return
							}

							/*
								获取目标 ElasticSearch 索引的设置时可能会出现错误，但是这个错误不需要终止迁移操作，因为在创建新的索引时可以使用默认设置或指定的设置。
							*/
							targetIndexSettings, err := migrator.TargetESAPI.GetIndexSettings(c.TargetIndexName)
							if err != nil {

								/*
									在这里，我们只是使用 log.Debug 记录错误信息，方便后续调试问题，而不需要停止程序的执行。
									这也是为什么使用 log.Debug 函数用来记录错误信息，而不是使用被认为是比较严重的 log.Error 函数。
								*/
								log.Debug(err)
							}
							log.Debug("target IndexSettings", targetIndexSettings)

							//if there is only one index and we specify the dest indexname
							/*
								首先检查是否只有一个源索引，并且指定了目标索引名称。
								如果满足这些条件，则将源索引的设置更改为目标索引的设置，并将源索引从设置映射中删除。
								最后，通过调用 log.Debug() 函数将源索引的设置打印到日志中，以便我们可以检查设置是否正确更新。
							*/
							if c.SourceIndexNames != c.TargetIndexName && (len(c.TargetIndexName) > 0) && indexCount == 1 {
								log.Debugf("only one index,so we can rewrite indexname, src:%v, dest:%v ,indexCount:%d", c.SourceIndexNames, c.TargetIndexName, indexCount)

								/*
									*sourceIndexSettings 是一个指向索引设置映射的指针，
									[c.TargetIndexName] 和 [c.SourceIndexNames] 是映射中的键。
									因为 *sourceIndexSettings 是一个指针，所以使用 [] 访问其中的数据。
									(*sourceIndexSettings)[c.TargetIndexName] 表达式表示将映射中 c.SourceIndexNames 键的值转移到 c.TargetIndexName 键中。
								*/
								(*sourceIndexSettings)[c.TargetIndexName] = (*sourceIndexSettings)[c.SourceIndexNames]

								/*
									删除 *sourceIndexSettings 中的一些元素，元素的值是 c.SourceIndexNames。
									其中 *sourceIndexSettings 是一个指针类型的变量，它指向一个 []IndexSetting 类型的切片。
									c.SourceIndexNames 也是一个切片类型的变量，存储了需要删除的索引名称。
								*/
								delete(*sourceIndexSettings, c.SourceIndexNames)
								log.Debug(sourceIndexSettings)
							}

							/*
								将源索引设置迁移到一个新的目标索引上。它使用目标索引设置覆盖源索引设置，并删除一些不必要的设置参数以适合新的环境。
								首先，遍历源索引设置切片 (*sourceIndexSettings)
							*/
							for name, idx := range *sourceIndexSettings {
								log.Debug("dealing with index,name:", name, ",settings:", idx)

								/*
								  getEmptyIndexSettings() 函数使用了一个空的 map[string]interface{}{} 来存储 Elasticsearch 索引配置。
								  通过修改这个空 map 中的 key-value 对，逐步构建 Elasticsearch 索引配置。可以按照需要添加不同的设置和属性。
								*/
								tempIndexSettings := getEmptyIndexSettings()

								/*
									用一个布尔类型的变量 targetIndexExist 来表示目标索引是否存在，初始值为 false。
								*/
								targetIndexExist := false

								//if target index settings is exist and we don't copy settings, we use target settings
								/*
									如果有目标索引的设置存在（即 targetIndexSettings 不为 nil）
								*/
								if targetIndexSettings != nil {

									//if target es have this index and we dont copy index settings
									/*
										那么就判断目标 Elasticsearch 中是否有该索引（即判断 name 是否在 targetIndexSettings 中），
									*/
									if val, ok := (*targetIndexSettings)[name]; ok {

										/*
											如果有，则设置 targetIndexExist 为 true 并将该索引的设置提取为 tempIndexSettings。
										*/
										targetIndexExist = true
										tempIndexSettings = val.(map[string]interface{})
									}

									/*
										如果 RecreateIndex 为 true，
									*/
									if c.RecreateIndex {
										/*
											则需要先删除目标 Elasticsearch 中的该索引，并将 targetIndexExist 设置为 false，以便后续创建该索引。
										*/
										migrator.TargetESAPI.DeleteIndex(name)
										targetIndexExist = false
									}
								}

								//copy index settings
								if c.CopyIndexSettings {

									//将名为 name 的键对应的值从 sourceIndexSettings 中取出来，并将其断言为 map[string]interface{} 类型。
									tempIndexSettings = ((*sourceIndexSettings)[name]).(map[string]interface{})
								}

								//check map elements
								if _, ok := tempIndexSettings["settings"]; !ok {
									/*
										在 Elasticsearch 中，索引的设置是以 JSON 格式表示的，包含了各种有关于索引的配置信息，
										例如副本数、分片数、分词器等等。因此，我们需要提前创建一个空的字典对象来接收并存放这样的设置信息。
									*/
									tempIndexSettings["settings"] = map[string]interface{}{}
								}

								if _, ok := tempIndexSettings["settings"].(map[string]interface{})["index"]; !ok {
									/*
										在 tempIndexSettings 字典中，为 settings 键所对应的字典创建一个名为 index 的字典，并将其赋值给 settings 字典的 index 键。
										在 Elasticsearch 中，索引设置可以包含多个级别，而 index 级别是其中最高的级别。在这个级别下，我们可以对索引的各种特性进行配置，如分片数、副本数、分词器等。
										因此，我们需要在 settings 中添加一个 index 键来存储这些设置。
										最后，为了方便我们后续的操作，在 tempIndexSettings 的 settings 中存放了一个 index 的空字典，以便我们在这个空字典中添加更多的索引级别的设置信息。

										map[string]interface{}{} 是一个 go 语言的空字符映射，
											map 是一种内置的数据结构，用于存储键值对，其中键必须是唯一的；
											string 是一个表示字符串的数据类型；
											interface{} 是一个接口类型，可以表示任何类型的值。
										map[string]interface{}{} 表示一个空的字符串与任何类型值对应的映射，这意味着可以向其中添加任何键值对，
										其中键是字符串，而值可以是任何类型的数据（例如整数、浮点数、字符串、结构体、数组等）。

										首先通过 .(map[string]interface{}) 将 tempIndexSettings["settings"] 转换为了map[string]interface{} 类型，然后将其中键名为 index 的值取出来。
										(map[string]interface{})["index"] 表示从 tempIndexSettings["settings"] 中取出键名为 index 的元素，并将其强制类型转换为 map[string]interface{} 类型的值，最终得到这个值。
									*/
									tempIndexSettings["settings"].(map[string]interface{})["index"] = map[string]interface{}{}
								}
								/*
									这是一个用于解析 Elasticsearch 索引设置中的刷新时间间隔的代码。
									首先从源索引的设置列表中获取名称为 name 的设置，然后将其转换为 map[string]interface{} 类型并进一步获取其中的 settings 字段，
									接着获取 index 字段，再从中获取 refresh_interval 字段。最后，将其值赋给一个名为 sourceIndexRefreshSettings 的变量，这个变量应该是一个 map 类型。
								*/
								sourceIndexRefreshSettings[name] = ((*sourceIndexSettings)[name].(map[string]interface{}))["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"]

								//set refresh_interval
								/*
									设置 Elasticsearch 索引的刷新间隔、副本数。
									将 tempIndexSettings 中的 refresh_interval 设置为 -1，即禁用自动刷新；
									将 number_of_replicas 设置为 0，即副本数为零。
									tempIndexSettings 变量应该是一个 map[string]interface{} 类型的数据。
								*/
								tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"] = -1
								tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"] = 0

								/*
									tempIndexSettings 中的设置列表中删除 number_of_shards 设置项，这是因为索引的分片数应该在创建索引时由我们自己设置，而不是在迁移时再次设置。
								*/
								delete(tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "number_of_shards")

								//copy indexsettings and mappings
								/*
									在进行 Elasticsearch 数据的迁移时，若目标索引已经存在，则更改目标索引的设置，否则创建一个新的索引，并根据传入的参数进行设置。
								*/
								if targetIndexExist {
									log.Debug("update index with settings,", name, tempIndexSettings)
									//override shard settings

									/*
										设置索引分片数量的，其中 c.ShardsCount 是用户传入的参数，表示想要创建的索引分片数量。
										如果 c.ShardsCount 大于 0，就将它赋值给 tempIndexSettings 中的 "number_of_shards"。
										这个设置会影响到索引数据的分布和并行度，一般需要考虑到硬件资源和数据量等因素，合理设置分片数量可以提高查询效率和扩展性。
									*/
									if c.ShardsCount > 0 {
										tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"] = c.ShardsCount
									}

									/*
										migrator.TargetESAPI 对象调用 UpdateIndexSettings 方法，并传入了两个参数: 索引名称 name 和 索引设置 tempIndexSettings。
										这个方法的作用是更新索引的设置，包括分片数量、副本数量、分词器和停用词等。如果出现错误，该方法会返回一个非空的 err 对象。
										我们通常需要检查这个对象，以便及时处理任何错误。
									*/
									err := migrator.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings)
									if err != nil {
										log.Error(err)
									}
								} else {

									/*
										判断用户是否设置了索引分片数量(c.ShardsCount)。
									*/
									if c.ShardsCount > 0 {

										/*
											若设置了，则将分片数量更新到 tempIndexSettings 中的 "number_of_shards" 设置项中。
											需要注意的是，tempIndexSettings 是一个类型为 map[string]interface{} 的变量，里面储存了索引的各种设置信息，因此我们需要通过类型断言来访问和修改具体的设置项。
											具体来说，tempIndexSettings["settings"] 是一个 interface{} 类型，所以我们需要进行类型断言 (map[string]interface{}) 将它转换为 map[string]interface{} 类型，
											再通过链式调用访问到具体的设置项。若用户没有设置分片数量，则该段代码不会执行。
										*/
										tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"] = c.ShardsCount
									}

									log.Debug("create index with settings,", name, tempIndexSettings)

									/*
										创建一个新的 Elasticsearch 索引，其中 name 是索引名称，tempIndexSettings 是包含新索引设置的结构体。
									*/
									err := migrator.TargetESAPI.CreateIndex(name, tempIndexSettings)
									if err != nil {
										log.Error(err)
									}

								}

							}

							/*
								对 Elasticsearch 索引的映射(mapping)进行设置和更新。
							*/
							if c.CopyIndexMappings {

								/*
									判断 c.SourceIndexNames 是否等于 c.TargetIndexName，并且
									c.TargetIndexName 数量大于 0，并且 indexCount 等于 1 。
									同时 TargetIndexName 的长度大于 0，则会继续执行下面的代码块。否则，会跳过代码块直接执行下一行代码。
								*/
								if c.SourceIndexNames != c.TargetIndexName && (len(c.TargetIndexName) > 0) && indexCount == 1 {
									log.Debugf("only one index,so we can rewrite indexname, src:%v, dest:%v ,indexCount:%d", c.SourceIndexNames, c.TargetIndexName, indexCount)
									(*sourceIndexMappings)[c.TargetIndexName] = (*sourceIndexMappings)[c.SourceIndexNames]
									delete(*sourceIndexMappings, c.SourceIndexNames)
									log.Debug(sourceIndexMappings)
								}

								/*
									这个for 循环，主要是将源索引的映射信息应用到目标索引上。
								*/
								for name, mapping := range *sourceIndexMappings {

									/*
										遍历了 *sourceIndexMappings 切片。每次循环通过 name 取出一个源索引名称，以及该索引的所有属性信息 mapping，包括映射参数和索引设置等。
										在每次循环中，使用了
											migrator.TargetESAPI.UpdateIndexMapping(name, mapping.(map[string]interface{})["mappings"].(map[string]interface{}))
										来应用映射信息到目标索引。在此处也使用了错误检查，如果有错误，会将错误信息打印到日志中。
									*/
									err := migrator.TargetESAPI.UpdateIndexMapping(name, mapping.(map[string]interface{})["mappings"].(map[string]interface{}))
									if err != nil {
										log.Error(err)
									}
								}
							}

							log.Info("settings/mappings migration finished.")
						}

					} else {
						log.Error("index not exists,", c.SourceIndexNames)
						return
					}

					defer migrator.recoveryIndexSettings(sourceIndexRefreshSettings)
				} else if len(c.DumpInputFile) > 0 {
					//check shard settings
					//TODO support shard config
				}

			}

			log.Info("start data migration..")

			//start es bulk thread
			/*
				启动 Elasticsearch 批量写入线程或者文件导出线程。
				如果 TargetEs 不为空。
			*/
			if len(c.TargetEs) > 0 {
				log.Debug("start es bulk workers")

				/*
					`Prefix()` 方法是进度条的一个函数，它可以为进度条添加前缀。
					例如，`outputBar.Prefix("Bulk")` 那么进度条会在每个更新中显示前缀为 "Bulk" 的信息。这个函数的作用是为进度条的输出文本添加一个前缀，让输出的信息更加清晰明了。
				*/
				outputBar.Prefix("Bulk")
				var docCount int
				wg.Add(c.Workers)
				for i := 0; i < c.Workers; i++ {

					/*
						在 Elasticsearch 中执行批量数据迁移的 Go 库 migrator 的代码，其中 NewBulkWorker() 函数创建了一个 BulkWorker 对象。
						这个函数接受三个参数：
							docCount 是一个指向整数的指针，用于统计已经迁移的文档数量。
							outputBar 是一个进度条，用于显示迁移进度。
							wg 是指向 sync.WaitGroup 类型的指针，用于控制并发执行的 goroutine 数量。
						这个函数会返回一个 *migration.BulkWorker 类型的指针，可以用于后续的操作。
						通常情况下，我们会在一个 goroutine 中执行迁移操作，在另外一个 goroutine 中监听信号，收到信号后调用 Stop() 方法终止迁移进程。
						在 Stop() 被调用后，迁移进程会结束，并清空所有未提交的操作。

					*/
					go migrator.NewBulkWorker(&docCount, outputBar, &wg)
				}

				/*如果 DumpOutFile 不为空字符串，就会开启导出并写入文件的操作。*/
			} else if len(c.DumpOutFile) > 0 {

				/*
					从 Elasticsearch 中导出数据并写入到文件中的代码片段
				*/
				outputBar.Prefix("Write")
				wg.Add(1)

				/*
					使用了 migrator 包中的 NewFileDumpWorker 函数来创建一个文件 dump 的 worker 实例。
					创建一个 FileDumpWorker 对象，用于将 Elasticsearch 中的数据导出并写入指定的输出文件中。这个函数接受两个参数：
						outputBar 是一个进度条，用于显示导出进度。
						wg 是一个 sync.WaitGroup，用于控制并发执行的 goroutine 数量。
					然后，在一个新的 goroutine 中执行导出操作，并将进度条的前缀设置为 "Write"。
					最后，调用 wg.Add(1) 方法将一个等待的 goroutine 数量加 1，以便在所有 goroutine 执行完成后使用 wg.Wait() 等待所有 goroutine 结束。

				*/
				go migrator.NewFileDumpWorker(outputBar, &wg)
			}

			wg.Wait()

			if showBar {

				/*
					如果 showBar 为 true，即需要显示进度条，那么输出进度条结束信息（outputBar.Finish()）。
				*/
				outputBar.Finish()

				/*
					关闭数据库连接池（pool.Stop()）。
				*/
				pool.Stop()

			}
		}

	}

	log.Info("data migration finished.")
}

/*
Migrator 结构体中的一个方法，用于从源索引中恢复索引设置（settings）到目标索引。
sourceIndexRefreshSettings 参数是一个 map，表示源索引名称及其设置，该参数将用于覆盖目标索引的对应设置。
*/
func (c *Migrator) recoveryIndexSettings(sourceIndexRefreshSettings map[string]interface{}) {
	//update replica and refresh_interval
	for name, interval := range sourceIndexRefreshSettings {
		tempIndexSettings := getEmptyIndexSettings()

		/*方法中根据设置的 refresh_interval 值来更新目标索引的设置。*/
		tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"] = interval

		//tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"] = 1
		/*
			通过调用 TargetESAPI 的 UpdateIndexSettings 方法，将新的索引设置更新到目标索引中。
		*/
		c.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings)

		/*如果 Migrator 的 Config 中配置了 Refresh 为 true*/
		if c.Config.Refresh {

			/*则会在设置完成后调用 TargetESAPI 的 Refresh 方法来强制刷新索引以确保新的设置得到正确应用。*/
			c.TargetESAPI.Refresh(name)
		}
	}
}

/*
获取 Elasticsearch 集群版本的方法。
使用了 Get 方法向 Elasticsearch 集群发送 GET 请求获取集群版本信息。
如果请求成功，会将响应体的内容写入 ClusterVersion 结构体中并返回，否则返回获取失败的错误信息。
在获取成功后，还会将响应体关闭，释放资源。
*/
func (c *Migrator) ClusterVersion(host string, auth *Auth, proxy string) (*ClusterVersion, []error) {

	url := fmt.Sprintf("%s", host)
	resp, body, errs := Get(url, auth, proxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs
	}

	log.Debug(body)

	version := &ClusterVersion{}
	err := json.Unmarshal([]byte(body), version)

	if err != nil {
		log.Error(body, errs)
		return nil, errs
	}
	return version, nil
}

/*
Migrator 结构体的 ClusterReady 方法，主要作用是检查 Elasticsearch 集群是否就绪。
它会先调用传入的 ESAPI 接口获取集群健康状态，再根据是否等待绿色状态来确定是否就绪。
*/

func (c *Migrator) ClusterReady(api ESAPI) (*ClusterHealth, bool) {

	/*
		api.ClusterHealth 是一个 Elasticsearch API 方法，用于获取集群的健康状态。
		通过调用该方法，可以查看集群中各个节点的状态以及索引和分片的状态等信息。
	*/
	health := api.ClusterHealth()

	/*
		c.Config.WaitForGreen 是一个配置项，用于设置是否等待 Elasticsearch 集群的状态变为绿色（green）再执行后续操作。
		通常在索引迁移等场景下，为了避免在不稳定状态下进行操作，应该等待集群变为绿色再进行操作。
		如果该配置项为 true，则需要等待集群状态变为绿色，否则直接返回 health 和 true。
	*/
	if !c.Config.WaitForGreen {
		return health, true
	}

	/*
		如果 Elasticsearch 集群的健康状态为 red，则会直接返回 health 和 false，说明集群不可用。
	*/
	if health.Status == "red" {
		return health, false
	}

	/*
		在 Elasticsearch 健康检查中用于判断是否需要等待集群健康状态为绿色。
		如果 Config.WaitForGreen 为 false，即不需要等待集群健康状态为绿色，那么只要集群健康状态为黄色，就可以认为集群已经可用，并返回健康状态和 true。
		如果 Config.WaitForGreen 为 true，则需要等待集群健康状态为绿色，再返回健康状态和 true。
	*/
	if c.Config.WaitForGreen == false && health.Status == "yellow" {
		return health, true
	}

	/* 如果绿色状态，健康状态是就绪。 */
	if health.Status == "green" {
		return health, true
	}

	return health, false
}
