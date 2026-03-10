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
	"sync"

	"github.com/zuoyebang/bitaloslog/bitree"
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/manual"
	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

type sequentialDB struct {
	db           *DB
	meta         *sequentialMeta
	logger       Logger
	dirname      string
	dataDir      vfs.File
	memTableSize int

	readState struct {
		sync.RWMutex
		val *readState
	}

	mem struct {
		sync.RWMutex

		mutable *memTable
		queue   flushableList

		compact struct {
			cond     sync.Cond
			flushing bool
		}
	}

	btree *bitree.Bitree
}

func (s *sequentialDB) Close() (err error) {
	s.mem.Lock()
	defer s.mem.Unlock()

	for s.mem.compact.flushing {
		s.mem.compact.cond.Wait()
	}

	if err = s.runFlush(s.mem.queue); err != nil {
		s.logger.Errorf("sequentialDB close runFlush err:%s", err)
	}

	s.readState.val.unref()
	for _, mem := range s.mem.queue {
		mem.readerUnref()
	}

	err = utils.FirstError(err, s.btree.Close())
	err = utils.FirstError(err, s.meta.close())
	err = utils.FirstError(err, s.dataDir.Close())

	s.logger.Infof("sequentialDB close finish")
	return err
}

func (s *sequentialDB) Get(key []byte) ([]byte, func(), error) {
	rs := s.loadReadState()

	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		value, exist, kind := m.get(key)
		if exist {
			switch kind {
			case InternalKeyKindSet, InternalKeyKindPrefixDelete:
				return value, func() { rs.unref() }, nil
			case InternalKeyKindDelete:
				rs.unref()
				return nil, nil, ErrNotFound
			}
		}
	}

	rs.unref()

	value, exist, closer := s.btree.Get(key)
	if exist {
		return value, closer, nil
	}

	return nil, nil, ErrNotFound
}

func (s *sequentialDB) Set(key, value []byte, kind InternalKeyKind) error {
	newSize := uint32(memTableEntrySize(len(key), len(value)))
	seqNum := s.meta.getNextSeqNum()
	ikey := base.MakeInternalKey(key, seqNum, kind)
	s.mem.Lock()
	s.makeRoomForWrite(newSize)
	mem := s.mem.mutable
	s.mem.Unlock()

	defer func() {
		if mem.writerUnref() {
			s.mem.Lock()
			s.maybeScheduleFlush()
			s.mem.Unlock()
		}
	}()

	return mem.add(ikey, value)
}

func (s *sequentialDB) NewIter(o *IterOptions) *Iterator {
	rs := s.loadReadState()
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		alloc:               buf,
		cmp:                 s.db.cmp,
		equal:               s.db.equal,
		iter:                &buf.merging,
		split:               s.db.split,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		readState:           rs,
	}
	if o != nil {
		dbi.opts = *o
	}
	dbi.opts.Logger = s.logger

	mlevels := buf.mlevels[:0]
	numMergingLevels := len(rs.memtables) + 1
	if numMergingLevels > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, numMergingLevels)
	}

	for i := len(rs.memtables) - 1; i >= 0; i-- {
		mem := rs.memtables[i]
		mlevels = append(mlevels, mergingIterLevel{iter: mem.newIter(&dbi.opts)})
	}

	btreeIter := s.btree.NewIter(&dbi.opts)
	mlevels = append(mlevels, mergingIterLevel{iter: btreeIter})

	buf.merging.init(&dbi.opts, dbi.cmp, mlevels...)
	return dbi
}

func (s *sequentialDB) Compact(key []byte) {
	s.btree.CompactTo(key)
}

func (s *sequentialDB) makeWalFilename(fileNum FileNum) string {
	return base.MakeFilepath(s.db.opts.FS, s.dirname, fileTypeLog, fileNum)
}

func (s *sequentialDB) makeAtFilename(fileNum FileNum) string {
	return base.MakeFilepath(s.db.opts.FS, s.dirname, base.FileTypeArrayTable, fileNum)
}

func (s *sequentialDB) manualFlush() error {
	flushed, err := s.manualAsyncFlush()
	if err != nil {
		return err
	}
	if flushed != nil {
		<-flushed
	}
	return nil
}

func (s *sequentialDB) manualAsyncFlush() (<-chan struct{}, error) {
	if s.db.IsClosed() {
		return nil, ErrClosed
	}

	s.mem.Lock()
	defer s.mem.Unlock()
	empty := true
	for i := range s.mem.queue {
		if !s.mem.queue[i].empty() {
			empty = false
			break
		}
	}
	if empty {
		return nil, nil
	}
	flushed := s.mem.queue[len(s.mem.queue)-1].flushed
	s.makeRoomForWrite(0)
	return flushed, nil
}

func (s *sequentialDB) walPreallocateSize() int {
	size := s.memTableSize
	size = (size / 10) + size
	return size
}

func (s *sequentialDB) newMemTable() (*memTable, *flushableEntry) {
	mem := newMemTable(memTableOptions{
		Options: s.db.opts,
		size:    s.memTableSize,
	})

	entry := newFlushableEntry(mem, 0, 0)
	entry.release = func() {
		manual.Free(mem.arenaBuf)
		mem.arenaBuf = nil
	}

	return mem, entry
}

func (s *sequentialDB) makeRoomForWrite(newSize uint32) {
	var size uint64
	var entry *flushableEntry
	force := newSize == 0
	stalled := false
	for {
		if newSize > 0 {
			err := s.mem.mutable.prepareBySize(newSize)
			if err == nil {
				if stalled {
					s.logger.Error("[SEQDB] write stall ending")
				}
				return
			}
		} else if !force {
			if stalled {
				s.logger.Error("[SEQDB] write stall ending")
			}
			return
		}

		size = 0
		for i := range s.mem.queue {
			size += s.mem.queue[i].totalBytes()
		}
		if size >= uint64(s.db.opts.SequentialMemTableStopWritesThreshold*s.memTableSize) {
			if !stalled {
				stalled = true
				s.logger.Error("[SEQDB] write stall beginning: memtable count limit reached")
			}
			s.mem.compact.cond.Wait()
			continue
		}

		immMem := s.mem.mutable
		s.mem.mutable, entry = s.newMemTable()
		s.mem.queue = append(s.mem.queue, entry)
		s.updateReadState()
		if immMem.writerUnref() {
			s.maybeScheduleFlush()
		}
		force = false
	}
}

func (s *sequentialDB) loadReadState() *readState {
	s.readState.RLock()
	state := s.readState.val
	state.ref()
	s.readState.RUnlock()
	return state
}

func (s *sequentialDB) updateReadState() {
	rs := &readState{
		memtables: s.mem.queue,
	}
	rs.refcnt.Store(1)

	for _, mem := range rs.memtables {
		mem.readerRef()
	}

	s.readState.Lock()
	old := s.readState.val
	s.readState.val = rs
	s.readState.Unlock()

	if old != nil {
		old.unref()
	}
}
