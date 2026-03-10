// Copyright 2019-2022 The Zuoyebang-Stored and Zuoyebang-Bitalosdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.

package manual

import (
	"unsafe"
)

const bufferSize = int(unsafe.Sizeof(Buffer{}))

type Buffer struct {
	buf []byte
	ref refcnt
}

func NewBuffer(size int) (*Buffer, []byte) {
	b := New(bufferSize + size)
	bf := (*Buffer)(unsafe.Pointer(&b[0]))
	bf.buf = b[bufferSize:]
	bf.ref.init(1)
	return bf, b[bufferSize:]
}

func (b *Buffer) Acquire() {
	b.ref.acquire()
}

func (b *Buffer) Release() {
	if b.ref.release() {
		b.free()
	}
}

func (b *Buffer) free() {
	n := bufferSize + cap(b.buf)
	buf := (*[MaxArrayLen]byte)(unsafe.Pointer(b))[:n:n]
	b.buf = nil
	Free(buf)
}
