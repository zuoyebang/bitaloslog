// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitaloslog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/zuoyebang/bitaloslog/internal/base"
)

const (
	atVersionDefault uint16 = iota + 1
)

const (
	atHeaderSize   = 18
	atHeaderOffset = tableDataOffset
	atDataOffset   = atHeaderOffset + atHeaderSize

	itemOffsetSize = 4
	itemHeaderLen  = 2
)

type arrayTable struct {
	tbl         *table
	filename    string
	header      *atHeader
	itemsOffset []uint32
	intIndex    []byte
	size        uint32
	num         int
}

type atHeader struct {
	version        uint16
	num            uint32
	dataOffset     uint32
	intIndexOffset uint32
	mapIndexOffset uint32
}

type atOptions struct {
	useMapIndex       bool
	usePrefixCompress bool
	useBlockCompress  bool
	useBitrieCompress bool
	blockSize         uint32
}

func checkArrayTable(obj interface{}) {
	s := obj.(*arrayTable)
	if s.tbl != nil {
		fmt.Fprintf(os.Stderr, "arrayTable(%s) buffer was not freed\n", s.tbl.path)
		os.Exit(1)
	}
}

var _ flushable = (*arrayTable)(nil)

func newArrayTable(path string) (*arrayTable, error) {
	tbl, err := openTable(path, defaultTableOptions)
	if err != nil {
		return nil, err
	}

	at := &arrayTable{
		tbl:      tbl,
		filename: base.GetFilePathBase(path),
		num:      0,
	}

	var headerOffset uint32
	headerOffset, err = at.tbl.alloc(atHeaderSize)
	if err != nil {
		return nil, err
	}
	if headerOffset != atHeaderOffset {
		return nil, errors.New("tblSize is not large enough to hold the arrayTable header")
	}

	at.header = &atHeader{
		dataOffset: atDataOffset,
		version:    atVersionDefault,
	}

	return at, nil
}

func openArrayTable(path string) (*arrayTable, error) {
	tbl, err := openTable(path, &tableOptions{openType: tableReadMmap})
	if err != nil {
		return nil, err
	}

	at := &arrayTable{
		tbl: tbl,
	}

	at.readHeader()
	at.num = int(at.header.num)
	at.size = at.tbl.Size()
	at.dataIntBuffer()

	return at, nil
}

func (a *arrayTable) writeHeader() {
	buf := a.tbl.getBytes(atHeaderOffset, atHeaderSize)
	binary.BigEndian.PutUint16(buf[0:2], a.header.version)
	binary.BigEndian.PutUint32(buf[2:6], a.header.num)
	binary.BigEndian.PutUint32(buf[6:10], a.header.dataOffset)
	binary.BigEndian.PutUint32(buf[10:14], a.header.intIndexOffset)
	binary.BigEndian.PutUint32(buf[14:18], a.header.mapIndexOffset)
}

func (a *arrayTable) readHeader() {
	buf := a.tbl.getBytes(atHeaderOffset, atHeaderSize)
	a.header = &atHeader{}
	a.header.version = binary.BigEndian.Uint16(buf[0:2])
	a.header.num = binary.BigEndian.Uint32(buf[2:6])
	a.header.dataOffset = binary.BigEndian.Uint32(buf[6:10])
	a.header.intIndexOffset = binary.BigEndian.Uint32(buf[10:14])
	a.header.mapIndexOffset = binary.BigEndian.Uint32(buf[14:18])
}

func (a *arrayTable) dataIntBuffer() {
	a.intIndex = a.tbl.getBytes(a.header.intIndexOffset, a.getIntIndexSize())
}

func (a *arrayTable) getVersion() uint32 {
	return uint32(a.header.version)
}

func (a *arrayTable) getIntIndexSize() uint32 {
	return uint32(a.num * itemOffsetSize)
}

func (a *arrayTable) set(key, value []byte) (uint32, error) {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	sz := keySize + valueSize + itemHeaderLen
	offset, err := a.tbl.alloc(sz)
	if err != nil {
		return 0, err
	}

	a.num++
	a.itemsOffset = append(a.itemsOffset, offset)
	itemBuf := a.tbl.getBytes(offset, sz)
	binary.BigEndian.PutUint16(itemBuf[0:itemHeaderLen], uint16(keySize))
	copy(itemBuf[itemHeaderLen:itemHeaderLen+keySize], key)
	copy(itemBuf[itemHeaderLen+keySize:sz], value)

	return sz, nil
}

func (a *arrayTable) writeFinish() error {
	intSize := a.getIntIndexSize()
	intIndexOffset, err := a.tbl.alloc(intSize)
	if err != nil {
		return err
	}
	intIndexBuf := a.tbl.getBytes(intIndexOffset, intSize)
	intIndexPos := 0
	for i := range a.itemsOffset {
		binary.BigEndian.PutUint32(intIndexBuf[intIndexPos:intIndexPos+itemOffsetSize], a.itemsOffset[i])
		intIndexPos += itemOffsetSize
	}

	var mapIndexOffset uint32

	a.size = a.tbl.Size()
	a.itemsOffset = nil
	a.header.num = uint32(a.num)
	a.header.intIndexOffset = intIndexOffset
	a.header.mapIndexOffset = mapIndexOffset
	a.writeHeader()

	if err = a.tbl.mmapReadTruncate(int(a.size)); err != nil {
		return err
	}

	a.dataIntBuffer()

	return nil
}

func (a *arrayTable) getItemOffset(i int) uint32 {
	if i < 0 || i >= a.num {
		return 0
	}

	pos := i * itemOffsetSize
	itemOffset := binary.BigEndian.Uint32(a.intIndex[pos : pos+itemOffsetSize])

	return itemOffset
}

func (a *arrayTable) getKey(i int) []byte {
	itemOffset := a.getItemOffset(i)
	if itemOffset == 0 {
		return nil
	}

	keySize := uint32(binary.BigEndian.Uint16(a.tbl.getBytes(itemOffset, itemHeaderLen)))
	key := a.tbl.getBytes(itemOffset+itemHeaderLen, keySize)
	return key
}

func (a *arrayTable) getKV(i int) ([]byte, []byte) {
	itemOffset := a.getItemOffset(i)
	if itemOffset == 0 {
		return nil, nil
	}

	var itemSize uint32
	if i == a.num-1 {
		itemSize = a.header.intIndexOffset - itemOffset
	} else {
		itemSize = a.getItemOffset(i+1) - itemOffset
	}

	keySize := uint32(binary.BigEndian.Uint16(a.tbl.getBytes(itemOffset, itemHeaderLen)))
	keyOffset := itemOffset + itemHeaderLen
	key := a.tbl.getBytes(keyOffset, keySize)
	valueSize := itemSize - keySize - itemHeaderLen
	value := a.tbl.getBytes(keyOffset+keySize, valueSize)

	return key, value
}

func (a *arrayTable) get(key []byte) ([]byte, bool, InternalKeyKind) {
	pos := a.findKeyIndexPos(key)
	k, v := a.getKV(pos)
	if k == nil {
		return nil, false, InternalKeyKindInvalid
	}
	return v, true, InternalKeyKindSet
}

func (a *arrayTable) itemCount() int {
	return a.num
}

func (a *arrayTable) totalBytes() uint64 {
	return uint64(a.tbl.Size())
}

func (a *arrayTable) inuseBytes() uint64 {
	return uint64(a.tbl.Size())
}

func (a *arrayTable) getModTime() int64 {
	return a.tbl.modTime
}

func (a *arrayTable) close() error {
	if a.tbl == nil {
		return nil
	}

	if err := a.tbl.close(); err != nil {
		return err
	}

	a.tbl = nil
	return nil
}

func (a *arrayTable) readyForFlush() bool {
	return true
}

func (a *arrayTable) path() string {
	if a.tbl == nil {
		return ""
	}
	return a.tbl.path
}

func (a *arrayTable) idxFilePath() string {
	return ""
}

func (a *arrayTable) empty() bool {
	return a.num == 0
}

func (a *arrayTable) findKeyIndexPos(key []byte) int {
	return sort.Search(a.num, func(i int) bool {
		k := a.getKey(i)
		return bytes.Compare(k, key) != -1
	})
}
