/*
Copyright Medcl (m AT medcl.net)

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
	"errors"
	"io"
)

//https://golangtc.com/t/5a49e2104ce40d740bbbc515

/*缓冲区结构体（buffer struct），用于存储读取到的数据*/
type buffer struct {
	reader io.Reader /*存储读取器（io.Reader）*/
	buf    []byte    /*存储数据的字节数组*/
	start  int       /*标记已读取（start）数据的索引*/
	end    int       /*标记未读取（end）数据的索引*/
}

/*这是一个函数，功能是创建一个新的缓冲区对象。这个缓冲区对象包含一个io.Reader类型的实例、一个byte切片和两个整数类型的指针（用于记录读入和读出的位置）。*/
func newBuffer(reader io.Reader, len int) buffer {

	/*创建一个长度为len的byte切片*/
	buf := make([]byte, len)

	/*返回一个buffer结构体，内部包含了刚才创建的byte切片、io.Reader对象以及读入/读出的位置*/
	return buffer{reader, buf, 0, 0}
}

/*
这是一个方法，它属于 type buffer struct{} 结构体的方法"。
它表示计算当前“buffer”类型结构体变量的有效字节长度，即end-start。这个Len()方法返回的是一个整型值。
*/
func (b *buffer) Len() int {
	return b.end - b.start
}

/*
将buffer中有用的字节前移的函数。
当buffer中的字节达到容量上限时，需要将已经处理过的字节前移，腾出空间给后续的字节。
*/
func (b *buffer) grow() {

	/*
		首先判断缓冲区中已经处理过的字节的起始点b.start是否为0，如果已经位于缓冲区起始位置，则已经没有可以前移的字节。
	*/
	if b.start == 0 {
		return
	}
	/*
		在 go 语言标准库中，copy() 函数用于将一个 slice 的元素复制到另外一个slice 中。
		copy 函数签名：func copy(dst, src []T) int ，其中，
			dst 是目标 slice，
			src 是源 slice，T 是 slice 中元素的类型。
		copy 函数将会把 src 中的元素复制到 dst 中，并返回实际复制的元素个数。
		如果 dst 的长度小于 src 的长度， copy 函数只会复制 dst 长度的元素。
		如果 dst 的长度大于 src 的长度，则剩余的 dst 元素将保持原值不变。
		注意的是，copy 函数不会做任何的自动扩容，也就是说如果 dst 的容量不够，需要先手动扩容。

		copy() 函数在这里的作用是将缓冲区中的数据从起始位置 start 到结束位置 end 复制到缓冲区头部，即从位置 0 开始。
		缓冲区的数据存储在一个 b.buf 的字节数组中，而 b.start 和 b.end 则记录了当前缓冲区中有效数据的起始和结束位置。
		先计算出需要复制的字节数（即 b.end-b.start），然后使用切片表达式 b.buf[b.start:b.end] 提取出这些数据，最后将其复制到缓冲区头部 b.buf 的开始位置（即 b.buf[0:]）。
		这个复制过程相当于把缓冲区中的有效数据移动到数组的开始位置，让后续的写入操作可以继续使用缓冲区。

	*/
	copy(b.buf, b.buf[b.start:b.end])
	b.end -= b.start
	b.start = 0
}

/*
从一个reader中读取数据，并将其存储到buffer中。
该方法是阻塞式的。如果reader阻塞，这将导致整个程序阻塞。因此，在使用该方法时应谨慎考虑，可能需要在程序中使用goroutine来保证数据的及时读取。
*/
func (b *buffer) readFromReader() (int, error) {

	/*
		调用上面 buffer 的 grow() 方法。
	*/
	b.grow()

	/*调用了reader的Read()方法来将数据读取到buffer的空闲空间中，并将读取的字节数n返回*/
	n, err := b.reader.Read(b.buf[b.end:])
	if err != nil {
		return n, err
	}
	b.end += n
	return n, nil
}

/*
用于在缓冲区中获取n个字节长度的数据，而不移动缓冲区的指针位置
*/
func (b *buffer) seek(n int) ([]byte, error) {

	/*首先判断缓冲区中是否有足够的数据*/
	if b.end-b.start >= n {

		/*如果有，则将缓冲区中从start到start+n的字节切片出来，并返回*/
		buf := b.buf[b.start : b.start+n]
		return buf, nil
	}
	/*否则，返回一个错误信息"not enough"*/
	return nil, errors.New("not enough")
}

/*
用来读取缓冲区中的特定字段。
参数offset代表要跳过的字段数量，参数n表示要读取的字段数量。
*/
func (b *buffer) read(offset, n int) []byte {

	/*首先通过加上offset来更新start的值*/
	b.start += offset
	/*然后根据传入的n参数，从缓冲区中读取对应的字段，并将读取到的字段存储到buf中*/
	buf := b.buf[b.start : b.start+n]
	/*最后，再次更新start的值，并返回buf*/
	b.start += n
	return buf
}
