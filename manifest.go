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

package bitaloslog

import (
	"bufio"
	"encoding/binary"
	"sync/atomic"

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/errors"
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
	fieldOffsetMinUnflushedLogNum = metaFieldOffset
	fieldOffsetNextFileNum        = fieldOffsetMinUnflushedLogNum + 8
	fieldOffsetLastSeqNum         = fieldOffsetNextFileNum + 8
	fieldOffsetPlainDbAtFileNum   = fieldOffsetLastSeqNum + 8
)

const (
	metaVersion1 uint16 = 1
)

type metadata struct {
	atomic struct {
		logSeqNum     uint64
		visibleSeqNum uint64
	}

	header  *metaHeader
	file    *mmap.MMap
	fs      vfs.FS
	dirname string
	logger  Logger

	lastSeqNum         uint64
	minUnflushedLogNum FileNum
	nextFileNum        FileNum
	plainDbAtFileNum   FileNum
}

type metaHeader struct {
	version uint16
}

type metaSet struct {
	minUnflushedLogNum FileNum
	nextFileNum        FileNum
	lastSeqNum         uint64
	plainDbAtFileNum   FileNum
}

func openMetadata(dirname string, opts *Options) (*metadata, error) {
	m := &metadata{
		fs:      opts.FS,
		dirname: dirname,
		logger:  opts.Logger,
	}

	path := base.MakeFilepath(opts.FS, dirname, fileTypeMeta, 0)
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
	m.minUnflushedLogNum = FileNum(m.file.ReadUInt64At(fieldOffsetMinUnflushedLogNum))
	m.nextFileNum = FileNum(m.file.ReadUInt64At(fieldOffsetNextFileNum))
	m.lastSeqNum = m.file.ReadUInt64At(fieldOffsetLastSeqNum)
	m.plainDbAtFileNum = FileNum(m.file.ReadUInt64At(fieldOffsetPlainDbAtFileNum))
	if m.nextFileNum == 0 {
		m.nextFileNum = 1
	}
	if m.lastSeqNum != 0 {
		m.atomic.logSeqNum = m.lastSeqNum + 1
	} else {
		m.atomic.logSeqNum = 1
	}

	m.markFileNumUsed(m.minUnflushedLogNum)

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
	return m.file.Close()
}

func (m *metadata) writeMetaEdit(me *metaSet) {
	if me.minUnflushedLogNum != 0 {
		m.minUnflushedLogNum = me.minUnflushedLogNum
		m.file.WriteUInt64At(uint64(m.minUnflushedLogNum), fieldOffsetMinUnflushedLogNum)
	}

	if me.nextFileNum != 0 {
		m.nextFileNum = me.nextFileNum
		m.file.WriteUInt64At(uint64(m.nextFileNum), fieldOffsetNextFileNum)
	}

	if me.lastSeqNum != 0 {
		m.lastSeqNum = me.lastSeqNum
		m.file.WriteUInt64At(m.lastSeqNum, fieldOffsetLastSeqNum)
	}

	if me.plainDbAtFileNum != 0 {
		m.plainDbAtFileNum = me.plainDbAtFileNum
		m.file.WriteUInt64At(uint64(m.plainDbAtFileNum), fieldOffsetPlainDbAtFileNum)
	}
}

func (m *metadata) markFileNumUsed(fileNum FileNum) {
	if m.nextFileNum <= fileNum {
		m.nextFileNum = fileNum + 1
	}
}

func (m *metadata) markLogSeqNum(maxSeqNum uint64) {
	if m.atomic.logSeqNum < maxSeqNum {
		m.atomic.logSeqNum = maxSeqNum
	}
}

func (m *metadata) getLogSeqNum() uint64 {
	return atomic.LoadUint64(&m.atomic.logSeqNum)
}

func (m *metadata) getNextFileNum() FileNum {
	x := m.nextFileNum
	m.nextFileNum++
	return x
}

func (m *metadata) apply(me *metaSet) error {
	if me.minUnflushedLogNum != 0 {
		if me.minUnflushedLogNum < m.minUnflushedLogNum || m.nextFileNum <= me.minUnflushedLogNum {
			return errors.Errorf("inconsistent metaSet minUnflushedLogNum %s", me.minUnflushedLogNum)
		}
	}

	me.nextFileNum = m.nextFileNum
	logSeqNum := atomic.LoadUint64(&m.atomic.logSeqNum)
	me.lastSeqNum = logSeqNum - 1
	if logSeqNum == 0 {
		m.logger.Errorf("logSeqNum must be a positive integer: %d", logSeqNum)
	}

	m.writeMetaEdit(me)

	return nil
}
