// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitree

import (
	"bufio"
	"encoding/binary"
	"sync"

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/mmap"
	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

const (
	metaHeaderOffset = 0
	metaHeaderLen    = 16
	metaFieldOffset  = 16
	metaFieldLen     = 1024
	metaMagicLen     = 8
	metaFooterLen    = metaMagicLen
	metaMagic        = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	metaLen          = metaHeaderLen + metaFieldLen + metaMagicLen
	footerOffset     = metaLen - metaFooterLen
)

const (
	fieldOffsetMinUnfCompactFileNum = metaFieldOffset
	fieldOffsetNextFileNum          = fieldOffsetMinUnfCompactFileNum + 8
)

const (
	metaVersion1 uint16 = 1
)

type metadata struct {
	header              *metaHeader
	file                *mmap.MMap
	fs                  vfs.FS
	dirname             string
	logger              base.Logger
	mu                  sync.RWMutex
	minUnCompactFileNum FileNum
	nextFileNum         FileNum
}

type metaHeader struct {
	version uint16
}

type metaOptions struct {
	fs     vfs.FS
	path   string
	logger base.Logger
}

func openMetadata(opts *metaOptions) (*metadata, error) {
	m := &metadata{
		fs:      opts.fs,
		dirname: opts.path,
		logger:  opts.logger,
	}

	path := base.MakeFilepath(opts.fs, m.dirname, base.FileTypeMeta, 0)
	if utils.IsNotExist(path) {
		if err := m.create(path); err != nil {
			return nil, err
		}
	}

	file, err := mmap.Open(path, 0)
	if err != nil {
		return nil, err
	}

	m.file = file
	m.header = &metaHeader{
		version: m.file.ReadUInt16At(metaHeaderOffset),
	}
	m.minUnCompactFileNum = FileNum(m.file.ReadUInt64At(fieldOffsetMinUnfCompactFileNum))
	m.nextFileNum = FileNum(m.file.ReadUInt64At(fieldOffsetNextFileNum))
	if m.nextFileNum == 0 {
		m.nextFileNum = 1
	}

	return m, nil
}

func (m *metadata) create(filename string) (err error) {
	var metaFile vfs.File
	var meta *bufio.Writer

	metaFile, err = m.fs.Create(filename)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = m.fs.Remove(filename)
		}
		if metaFile != nil {
			err = metaFile.Close()
		}
	}()

	var buf [metaLen]byte
	meta = bufio.NewWriterSize(metaFile, metaLen)
	binary.LittleEndian.PutUint16(buf[metaHeaderOffset:metaHeaderOffset+2], metaVersion1)
	copy(buf[footerOffset:footerOffset+metaFooterLen], metaMagic)
	if _, err = meta.Write(buf[:]); err != nil {
		return err
	}
	if err = meta.Flush(); err != nil {
		return err
	}
	if err = metaFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (m *metadata) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.file.Close()
}

func (m *metadata) getNextFileNum() FileNum {
	m.mu.Lock()
	defer m.mu.Unlock()

	x := m.nextFileNum
	m.nextFileNum++
	m.file.WriteUInt64At(uint64(m.nextFileNum), fieldOffsetNextFileNum)
	return x
}

func (m *metadata) getMinUnCompactFileNum() FileNum {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.minUnCompactFileNum
}

func (m *metadata) setMinUnCompactFileNum(fn FileNum) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.minUnCompactFileNum = fn
	m.file.WriteUInt64At(uint64(m.minUnCompactFileNum), fieldOffsetMinUnfCompactFileNum)
}
