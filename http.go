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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	log "github.com/cihub/seelog"
	"github.com/parnurzeal/gorequest"
	"infini.sh/framework/core/util"
	"infini.sh/framework/lib/fasthttp"
)

/*
为 HTTP 请求设置基本认证信息。这样，就可以在客户端向服务端发送请求时携带认证信息，以便服务端可以验证身份是否合法。
在服务端接收到请求后，可以从请求头中获取认证信息，进行身份验证。
总体来说，这段函数的作用是为 HTTP 请求添加基本认证信息，以便访问需要身份验证的资源。
*/
func BasicAuth(req *fasthttp.Request, user, pass string) {
	msg := fmt.Sprintf("%s:%s", user, pass)
	/*
		将 user 和 pass 组成的字符串进行 base64 编码，构造成 Authorization 请求头。
		Authorization 请求头是一种表示客户端认证信息的 HTTP 请求头部字段，用于发送客户端的身份认证凭证。这个字段包括 Bearer、Basic、Digest、HOBA、Mutual 等认证方式。
		其中，Basic 是最基本的认证方式，它的值由用户名和密码组成的字符串，经过 base64 编码后添加到请求头中。
	*/
	encoded := base64.StdEncoding.EncodeToString([]byte(msg))
	/*添加到 req 表示的 HTTP 请求中*/
	req.Header.Add("Authorization", "Basic "+encoded)
}

/*用 go 实现的 HTTP GET 请求的函数。这段代码可以通过传递一个 URL，进行 GET 请求，若需要认证，则需要传入一个 Auth 结构体，并且支持设置代理。*/
func Get(url string, auth *Auth, proxy string) (*http.Response, string, []error) {

	/*
		github.com/parnurzeal/gorequest 是一个 Go 语言编写的 HTTP 请求库，它提供了一个名为 gorequest 的工具集，用于简化 HTTP 请求的构造和发送过程。
		其中，gorequest.New() 函数是 gorequest 工具集中最基本的函数，用于创建一个新的 HTTP 请求构造器。
		使用 gorequest.New() 创建的请求构造器 request 是一个 gorequest.SuperAgent 类型的对象，它提供了一系列方法，用于设置请求的 URL、HTTP 方法、请求头、请求参数、请求体等内容。
		需要注意的是，虽然 gorequest 库提供了一些便利的函数来简化 HTTP 请求的构造和发送过程，但在实际开发中，为了确保代码的可维护性和可扩展性，我们建议使用 Go 标准库提供的 net/http 包来进行 HTTP 请求。
	*/
	request := gorequest.New()

	/*用来创建一个 HTTP 传输客户端对象的，其中包含了一些客户端的配置项。*/
	tr := &http.Transport{
		/*DisableKeepAlives: 这个属性用来禁用 HTTP/1.1 中的 Keep-Alive 长连接特性，也就是禁用了 TCP 连接的复用功能。如果设置为 true，每次请求都会新建一个 TCP 连接。*/
		DisableKeepAlives: true,
		/*DisableCompression: 这个属性用来禁止客户端对服务器返回的内容进行压缩编码，该属性默认值为 false，即启用压缩编码。*/
		DisableCompression: false,
		/*
			TLSClientConfig: 这个属性用来配置客户端传输时的 TLS 加密协议的一些选项，比如是否跳过 TLS 证书验证等。
			其中，InsecureSkipVerify 属性为 true 时，客户端将不会验证服务端 TLS 证书的合法性，这样可以在 HTTPS 传输中避免证书校验造成的错误。
		*/
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	/*
		这段代码是将之前创建的 http.Transport 对象 tr 赋值给 request 对象的 Transport 属性，从而指定了这次请求的传输方式和配置。
		具体来说，这个功能是通过将 tr 对象赋值给 request 的 Transport 属性实现的。这样，每次通过 request 发送请求时，都会使用之前配置的传输方式和参数。
		相比默认的 http.DefaultTransport，使用自定义的 Transport 对象可以更好地满足一些个性化的传输需求，如对代理、证书校验等的定制。
	*/
	request.Transport = tr

	if auth != nil {
		/*
			这段代码会根据 auth 变量的值对 HTTP 请求添加基础认证信息。如果 auth 不为 nil，则会使用 auth.User 和 auth.Pass 字段的值来构造基础认证信息，并将其添加到 HTTP 请求的头部。
			基础认证信息是一种常用的 HTTP 认证方式，用于在 HTTP 请求头中通过用户名和密码的方式进行身份验证。
			基础认证信息的格式为 "Authorization: Basic [username:password]"，其中，"[username:password]" 是用户名和密码的组合字符串，在 RFC 2617 （https://datatracker.ietf.org/doc/html/rfc2617） 中有详细的规定。
			通常，开发人员可以直接使用 SetBasicAuth 方法来添加基础认证信息，而无需手动拼接字符串。
		*/
		request.SetBasicAuth(auth.User, auth.Pass)
	}

	//request.Type("application/json")
	/*判断是否设置了 HTTP 请求的代理，如果 proxy 参数不为空，则设置该请求使用的代理 地址为传入的 proxy 参数，具体实现可能是通过设置请求的 Transport 来实现代理的转发的。*/
	if len(proxy) > 0 {
		request.Proxy(proxy)
	}
	/*
		这段代码依赖 gorequest 第三方库。
		使用了 request.Get(url) 创建了一个 GET 请求实例，并制定访问的 URL。
		End()方法发送该请求，并返回三个值：resp(响应)、body(响应体)和 errs(请求错误)
	*/
	resp, body, errs := request.Get(url).End()
	return resp, body, errs

}

/*
用 go 语言实现发送 post 请求的函数。其中使用了第三方库 "github.com/parnurzeal/gorequest"。该函数支持传入 URL、认证信息、请求体和代理信息等参数，并返回响应和可能的错误。
*/
func Post(url string, auth *Auth, body string, proxy string) (*http.Response, string, []error) {
	/*
		github.com/parnurzeal/gorequest 是一个 Go 语言编写的 HTTP 请求库，它提供了一个名为 gorequest 的工具集，用于简化 HTTP 请求的构造和发送过程。
		其中，gorequest.New() 函数是 gorequest 工具集中最基本的函数，用于创建一个新的 HTTP 请求构造器。
		使用 gorequest.New() 创建的请求构造器 request 是一个 gorequest.SuperAgent 类型的对象，它提供了一系列方法，用于设置请求的 URL、HTTP 方法、请求头、请求参数、请求体等内容。
		需要注意的是，虽然 gorequest 库提供了一些便利的函数来简化 HTTP 请求的构造和发送过程，但在实际开发中，为了确保代码的可维护性和可扩展性，我们建议使用 Go 标准库提供的 net/http 包来进行 HTTP 请求。
	*/
	request := gorequest.New()
	/*用来创建一个 HTTP 传输客户端对象的，其中包含了一些客户端的配置项。*/
	tr := &http.Transport{
		/*DisableKeepAlives: 这个属性用来禁用 HTTP/1.1 中的 Keep-Alive 长连接特性，也就是禁用了 TCP 连接的复用功能。如果设置为 true，每次请求都会新建一个 TCP 连接。*/
		DisableKeepAlives: true,
		/*DisableCompression: 这个属性用来禁止客户端对服务器返回的内容进行压缩编码，该属性默认值为 false，即启用压缩编码。*/
		DisableCompression: false,
		/*
			TLSClientConfig: 这个属性用来配置客户端传输时的 TLS 加密协议的一些选项，比如是否跳过 TLS 证书验证等。
			其中，InsecureSkipVerify 属性为 true 时，客户端将不会验证服务端 TLS 证书的合法性，这样可以在 HTTPS 传输中避免证书校验造成的错误。
		*/
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	request.Transport = tr

	if auth != nil {
		request.SetBasicAuth(auth.User, auth.Pass)
	}

	//request.Type("application/json")

	if len(proxy) > 0 {
		request.Proxy(proxy)
	}

	request.Post(url)

	if len(body) > 0 {
		request.Send(body)
	}

	return request.End()
}

/*
用 go 语言编写的创建 HTTP DELETE 请求的函数。
该函数接受一个 http.Client 对象、请求方法和 URL 作为参数，返回创建的请求对象及可能的错误。
*/
func newDeleteRequest(client *http.Client, method, urlStr string) (*http.Request, error) {
	/*首先，该函数判断请求方法是否为空。*/
	if method == "" {
		// We document that "" means "GET" for Request.Method, and people have
		// relied on that from NewRequest, so keep that working.
		// We still enforce validMethod for non-empty methods.
		/*如果为空，则将请求方法设置为 "GET"，因为在请求方法为空时，NewRequest 函数会被默认认为是创建 GET 请求。*/
		method = "GET"
	}
	/*如果请求方法非空，就通过 http.Parse 函数解析给定的 URL，并将解析后的 URL 和请求方法设置到新的请求对象中。*/
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	/*用于创建一个 http.Request 实例，其中包括了 请求的方法，URL，协议版本，Host等信息。*/
	req := &http.Request{
		Method:     method,            /* method 是一个字符串，表示请求的方法，例如 GET、POST 等等 */
		URL:        u,                 /* u 是一个 *url.URL 结构体，表示请求的 URL，包含 scheme、host、path、query、fragment 等信息 */
		Proto:      "HTTP/1.1",        /* "HTTP/1.1" 表示请求使用的协议版本，这个字段一般不需要手动设置。如果需要使用 HTTP/2 协议，可以使用 http2.Transport */
		ProtoMajor: 1,                 /*表示协议版本号的主版本号和次版本号，对于 HTTP/1.1 来说，这两个字段的值就是 1 和 1。*/
		ProtoMinor: 1,                 /*表示协议版本号的主版本号和次版本号，对于 HTTP/1.1 来说，这两个字段的值就是 1 和 1。*/
		Header:     make(http.Header), /*创建了一个空的 http.Header，用来存储请求头信息，类型是 map[string][]string。如果你需要给请求设置自定义头信息，可以通过这个 Header 对象进行设置。*/
		Host:       u.Host,            /*u.Host 表示请求的 Host，一般就是 URL 中的 Host 部分。*/
	}
	return req, nil
}

//
//func GzipHandler(req *http.Request) {
//	var b bytes.Buffer
//	var buf bytes.Buffer
//	g := gzip.NewWriter(&buf)
//
//	_, err := io.Copy(g, &b)
//	if err != nil {
//		panic(err)
//		//slog.Error(err)
//		return
//	}
//}

var client *http.Client = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: false,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}
var fastHttpClient = &fasthttp.Client{
	TLSConfig: &tls.Config{InsecureSkipVerify: true},
}

/*
HTTP 请求的处理函数。通过传入不同的参数，可以用它来构建不同的请求。
我们使用 fasthttp 包提供的 AcquireRequest 和 AcquireResponse 函数分别创建一个 HTTP 请求和响应结构体，用于后续的操作。
这样做是为了减少内存分配和 GC 压力，因为这两个结构体在程序中会被频繁使用。

	compress 参数用来表示是否开启压缩。
	method 参数表示 HTTP 请求的方法，比如 GET、POST 等。
	loadUrl 参数表示要请求的 URL。
	auth 参数是一个包含认证信息的结构体指针。
	body 参数是请求体的字节数组。
	proxy 参数是一个字符串，表示 HTTP 请求的代理地址（如果有的话）。
*/
func DoRequest(compress bool, method string, loadUrl string, auth *Auth, body []byte, proxy string) (string, error) {

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	//defer fasthttp.ReleaseRequest(req)   // <- do not forget to release
	//defer fasthttp.ReleaseResponse(resp) // <- do not forget to release

	req.SetRequestURI(loadUrl)
	req.Header.SetMethod(method)

	//req.Header.Set("Content-Type", "application/json")

	if compress {
		req.Header.Set("Accept-Encoding", "gzip")
		req.Header.Set("content-encoding", "gzip")
	}

	if auth != nil {
		req.URI().SetUsername(auth.User)
		req.URI().SetPassword(auth.Pass)
	}

	if len(body) > 0 {

		//if compress {
		//	_, err := fasthttp.WriteGzipLevel(req.BodyWriter(), data.Bytes(), fasthttp.CompressBestSpeed)
		//	if err != nil {
		//		panic(err)
		//	}
		//} else {
		//	//req.SetBody(body)
		//	req.SetBodyStreamWriter(func(w *bufio.Writer) {
		//		w.Write(data.Bytes())
		//		w.Flush()
		//	})
		//
		//}

		if compress {
			_, err := fasthttp.WriteGzipLevel(req.BodyWriter(), body, fasthttp.CompressBestSpeed)
			if err != nil {
				panic(err)
			}
		} else {
			req.SetBody(body)

			//req.SetBodyStreamWriter(func(w *bufio.Writer) {
			//	w.Write(body)
			//	w.Flush()
			//})
		}
	}

	err := fastHttpClient.Do(req, resp)

	if err != nil {
		panic(err)
	}
	if resp == nil {
		panic("empty response")
	}

	log.Debug("received status code", resp.StatusCode, "from", string(resp.Header.Header()), "content", util.SubString(string(resp.Body()), 0, 500), req)

	if resp.StatusCode() == http.StatusOK || resp.StatusCode() == http.StatusCreated {

	} else {
		//log.Error("received status code", resp.StatusCode, "from", string(resp.Header.Header()), "content", string(resp.Body()), req)
	}

	//if compress{
	//	data,err:= resp.BodyGunzip()
	//	return string(data),err
	//}

	return string(resp.Body()), nil
}

func Request(method string, r string, auth *Auth, body *bytes.Buffer, proxy string) (string, error) {

	//TODO use global client
	//client = &http.Client{}
	//
	//if(len(proxy)>0){
	//	proxyURL, err := url.Parse(proxy)
	//	if(err!=nil){
	//		log.Error(err)
	//	}else{
	//		transport := &http.Transport{
	//			Proxy: http.ProxyURL(proxyURL),
	//			DisableKeepAlives: true,
	//			DisableCompression: false,
	//		}
	//		client = &http.Client{Transport: transport}
	//	}
	//}
	//
	//tr := &http.Transport{
	//	DisableKeepAlives: true,
	//	DisableCompression: false,
	//	TLSClientConfig: &tls.Config{
	//		InsecureSkipVerify: true,
	//},
	//}
	//
	//client.Transport=tr

	var err error
	var reqest *http.Request
	if body != nil {
		reqest, err = http.NewRequest(method, r, body)
	} else {
		reqest, err = newDeleteRequest(client, method, r)
	}

	if err != nil {
		panic(err)
	}

	if auth != nil {
		reqest.SetBasicAuth(auth.User, auth.Pass)
	}

	reqest.Header.Set("Content-Type", "application/json")

	//enable gzip
	//reqest.Header.Set("Content-Encoding", "gzip")
	//GzipHandler(reqest)
	//

	resp, errs := client.Do(reqest)
	if errs != nil {
		log.Error(util.SubString(errs.Error(), 0, 500))
		return "", errs
	}

	if resp != nil && resp.Body != nil {
		//io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		return "", errors.New("server error: " + string(b))
	}

	respBody, err := ioutil.ReadAll(resp.Body)

	log.Error(util.SubString(string(respBody), 0, 500))

	if err != nil {
		log.Error(util.SubString(string(err.Error()), 0, 500))
		return string(respBody), err
	}

	if err != nil {
		return string(respBody), err
	}
	io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()
	return string(respBody), nil
}

/*
DecodeJson 函数使用 json 包解码一个 JSON 字符串到 Go 的结构体或接口
*/
func DecodeJson(jsonStream string, o interface{}) error {

	/*
		strings.NewReader() 是 Go 语言标准库中的一个函数，用于创建一个包含指定字符串内容的 io.Reader 类型对象。
		在 Go 语言中，io.Reader 接口表示一个可以不断读取数据的对象。
		strings.NewReader() 函数会将给定字符串转化为一个实现了 io.Reader 接口的对象，从而可以方便地将字符串传递给需要 io.Reader 类型参数的函数。
		例如，在使用 json.NewDecoder() 时，我们通常需要传递一个实现了 io.Reader 接口的输入流对象，以便从中读取 JSON 数据进行解码。
		使用 strings.NewReader() 函数可以方便地将一个 JSON 格式的字符串转换为一个实现了 io.Reader 接口的输入流对象，从而可以被传递给 json.NewDecoder() 函数进行解码。

		json.NewDecoder() 是 Go 语言标准库中的一个函数，用于创建一个新的 JSON 解码器（Decoder）。
		该函数传入一个 io.Reader 类型参数，用于读取 JSON 数据流并进行解码。
		JSON 解码器是 encoding/json 包中的一种类型，可以将 JSON 数据流解码为 Go 语言中的数据类型，例如结构体、切片、map 等。
		因此，使用 json.NewDecoder() 可以轻松地解析 JSON 数据流，方便我们使用 Go 语言对 JSON 数据进行处理。
	*/
	decoder := json.NewDecoder(strings.NewReader(jsonStream))

	/*
		UseNumber() 是 Go 语言开发中在处理 JSON 时可以非常有用的一个方法。
		当我们使用 json.Unmarshal() 函数解码 JSON 数据时，默认情况下 Go 会将 JSON 数值都解析为 float64 类型。
		但是，有时候我们需要将 JSON 中的数字作为精确数值处理，而不是近似数值。例如类似货币金额或者日期时间值等需要保持精度的值。

		这时候就可以使用 UseNumber() 方法使 json.Unmarshal() 将 JSON 数字解析为 json.Number 类型，而不是默认的 float64 类型。
		这样可以在不丢失精度的情况下对 JSON 数值进行操作和处理。
	*/
	decoder.UseNumber()

	/*
		decoder.Decode() 是 Decoder 类型的一个方法，用于从输入流中读取下一个 JSON 值并将其解码为 Go 值。
		其中，Decoder 是一个带有缓冲区的解码器，它可以从输入流中读取 JSON 数据，并将其解码为 Go 值，例如 map[string]interface{} 或 []interface{}。
		使用 decoder.Decode() 方法可以将解码器中的数据按照 JSON 编码格式进行解码，并存储在与解码值类型相匹配的变量中。
	*/
	if err := decoder.Decode(o); err != nil {
		fmt.Println("error:", err)
		return err
	}
	return nil
}

/*
 */
func DecodeJsonBytes(jsonStream []byte, o interface{}) error {

	/*
		json.NewDecoder() 函数是 Go 语言内置的函数，它将字节流解码为 JSON 值。它的签名如下：
			func NewDecoder(r io.Reader) *Decoder
		NewDecoder() 函数将一个实现了 io.Reader 接口的对象作为输入，返回一个解析 JSON 的解码器 Decoder 的指针。
		参数 r 可以是任何实现了 io.Reader 接口的对象，比如文件、网络连接、从内存中读取的字节等。

		bytes.NewReader(jsonStream) 是一个创建 io.Reader 接口的函数，这里的 jsonStream 是一个存储了 JSON 数据的字节切片。
		bytes.NewReader() 函数的作用是以 jsonStream 作为底层数据源创建一个新的读取器，该读取器实现了 io.Reader 接口。
		这个新的读取器可以被传递给 json.NewDecoder() 函数，从而对其进行解码。
		这两个函数的组合使用，可以将一个包含 JSON 数据的字节切片解码为相应的 Go 数据类型。
	*/
	decoder := json.NewDecoder(bytes.NewReader(jsonStream))
	// UseNumber causes the Decoder to unmarshal a number into an interface{} as a Number instead of as a float64.
	decoder.UseNumber()

	if err := decoder.Decode(o); err != nil {
		fmt.Println("error:", err)
		return err
	}
	return nil
}
