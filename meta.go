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
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/mmap"
	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

const (
	smetaVersion1 uint16 = 1 + iota
)

const (
	smetaFieldSeqNumOffset = metaFieldOffset

	smetaFieldNumberGap     = 256 << 10
	smetaFieldNumberGapMask = 256<<10 - 1
)

type sequentialMeta struct {
	path   string
	logger base.Logger
	header *metaHeader
	file   *mmap.MMap
	fs     vfs.FS
	mu     sync.RWMutex
	seqNum atomic.Uint64
}

func openSequentialMeta(dirname string, opts *Options) (m *sequentialMeta, err error) {
	m = &sequentialMeta{
		fs:     opts.FS,
		logger: opts.Logger,
	}
	m.path = base.MakeFilepath(m.fs, dirname, fileTypeMeta, 0)
	if utils.IsNotExist(m.path) {
		if err = m.create(); err != nil {
			return nil, err
		}
	}
	if err = m.load(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *sequentialMeta) create() (err error) {
	var metaFile vfs.File
	metaFile, err = m.fs.Create(m.path)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = m.fs.Remove(m.path)
		}
	}()

	var buf [metaLen]byte
	for i := range buf {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[metaHeaderOffset:], smetaVersion1)
	copy(buf[footerOffset:footerOffset+metaFooterLen], metaMagic)
	if _, err = metaFile.Write(buf[:]); err != nil {
		return err
	}
	if err = metaFile.Close(); err != nil {
		return err
	}
	return nil
}

func (m *sequentialMeta) close() error {
	err := m.file.Close()
	return err
}

func (m *sequentialMeta) load() (err error) {
	m.file, err = mmap.Open(m.path, 0)
	if err != nil {
		return err
	}

	m.header = &metaHeader{}
	m.header.version = m.file.ReadUInt16At(metaHeaderOffset)
	seqNum := m.getSeqNum()
	m.seqNum.Store(seqNum + smetaFieldNumberGap)

	return nil
}

func (m *sequentialMeta) getSeqNum() uint64 {
	return m.file.ReadUInt64At(smetaFieldSeqNumOffset)
}

func (m *sequentialMeta) getCurrentSeqNum() uint64 {
	return m.seqNum.Load()
}

func (m *sequentialMeta) getNextSeqNum() uint64 {
	newSeqNum := m.seqNum.Add(1)
	if newSeqNum&smetaFieldNumberGapMask == 0 {
		m.writeUint64(newSeqNum, smetaFieldSeqNumOffset)
	}
	return newSeqNum
}

func (m *sequentialMeta) writeUint64(n uint64, pos int) {
	m.mu.Lock()
	m.file.WriteUInt64At(n+smetaFieldNumberGap, pos)
	m.mu.Unlock()
}
