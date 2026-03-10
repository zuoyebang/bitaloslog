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
	"sync/atomic"

	"github.com/zuoyebang/bitaloslog/internal/arenaskl"
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/errors"
	"github.com/zuoyebang/bitaloslog/internal/manual"
)

func memTableEntrySize(keyBytes, valueBytes int) uint64 {
	return uint64(arenaskl.MaxNodeSize(uint32(keyBytes)+8, uint32(valueBytes)))
}

var memTableEmptySize = func() uint32 {
	var pointSkl arenaskl.Skiplist
	arena := arenaskl.NewArena(make([]byte, 16<<10))
	pointSkl.Reset(arena, bytes.Compare)
	return arena.Size()
}()

type memTableOptions struct {
	*Options
	size      int
	logSeqNum uint64
}

func newMemTable(opts memTableOptions) *memTable {
	mem := &memTable{
		cmp:       opts.Comparer.Compare,
		equal:     opts.Comparer.Equal,
		logSeqNum: opts.logSeqNum,
		logger:    opts.Logger,
		size:      opts.size,
		arenaBuf:  manual.New(opts.size),
	}
	mem.writerRefs.Store(1)

	arena := arenaskl.NewArena(mem.arenaBuf)
	mem.skl.Reset(arena, mem.cmp)
	return mem
}

type memTable struct {
	cmp        Compare
	equal      Equal
	arenaBuf   []byte
	skl        arenaskl.Skiplist
	reserved   uint32
	writerRefs atomic.Int32
	logSeqNum  uint64
	logger     base.Logger
	size       int
	isFull     atomic.Bool
}

func (m *memTable) writerRef() {
	m.writerRefs.Add(1)
}

func (m *memTable) writerUnref() bool {
	switch v := m.writerRefs.Add(-1); {
	case v == 0:
		return true
	default:
		return false
	}
}

func (m *memTable) readyForFlush() bool {
	return m.writerRefs.Load() == 0
}

func (m *memTable) get(key []byte) ([]byte, bool, InternalKeyKind) {
	return m.skl.Get(key)
}

func (m *memTable) setWriteFull() {
	m.isFull.Store(true)
}

func (m *memTable) isWriteFull() bool {
	return m.isFull.Load()
}

func (m *memTable) add(key InternalKey, values []byte) error {
	return m.skl.Add(key, values)
}

func (m *memTable) apply(batch *Batch, seqNum uint64) error {
	if seqNum < m.logSeqNum {
		return errors.Errorf("bitaloslog: batch seqnum %d is less than memtable creation seqnum %d", seqNum, m.logSeqNum)
	}

	var err error
	var ins arenaskl.Inserter

	startSeqNum := seqNum
	for r := batch.Reader(); ; seqNum++ {
		kind, ukey, value, ok := r.Next()
		if !ok {
			break
		}

		switch kind {
		case InternalKeyKindLogData:
			seqNum--
		default:
			ikey := base.MakeInternalKey(ukey, seqNum, kind)
			err = ins.Add(&m.skl, ikey, value)
		}
		if err != nil {
			return err
		}
	}
	if seqNum != startSeqNum+uint64(batch.Count()) {
		return errors.Errorf("bitaloslog: inconsistent batch count: %d vs %d", seqNum, startSeqNum+uint64(batch.Count()))
	}
	return nil
}

func (m *memTable) newIter(o *IterOptions) internalIterator {
	return m.skl.NewIter(o.GetLowerBound(), o.GetUpperBound())
}

func (m *memTable) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	return m.skl.NewFlushIter(bytesFlushed)
}

func (m *memTable) availBytes() uint32 {
	a := m.skl.Arena()
	if m.writerRefs.Load() == 1 {
		m.reserved = a.Size()
	}
	return a.Capacity() - m.reserved
}

func (m *memTable) inuseBytes() uint64 {
	return uint64(m.skl.Size() - memTableEmptySize)
}

func (m *memTable) totalBytes() uint64 {
	return uint64(m.skl.Arena().Capacity())
}

func (m *memTable) close() error {
	return nil
}

func (m *memTable) empty() bool {
	return m.skl.Size() <= memTableEmptySize
}

func (m *memTable) prepare(batch *Batch) error {
	if batch.memTableSize > uint64(m.availBytes()) {
		return base.ErrTableFull
	}

	m.reserved += uint32(batch.memTableSize)
	m.writerRef()
	return nil
}

func (m *memTable) prepareBySize(newSize uint32) error {
	if newSize > m.availBytes() {
		return base.ErrTableFull
	}

	m.reserved += newSize
	m.writerRef()
	return nil
}
