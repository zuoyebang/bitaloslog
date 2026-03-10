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
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitaloslog/internal/arenaskl"
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/errors"
	"github.com/zuoyebang/bitaloslog/internal/invariants"
	"github.com/zuoyebang/bitaloslog/internal/manual"
	"github.com/zuoyebang/bitaloslog/internal/record"
	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

type plainDB struct {
	db                          *DB
	logger                      Logger
	dirname                     string
	dataDir                     vfs.File
	walDirname                  string
	walDir                      vfs.File
	disableWAL                  bool
	commit                      *commitPipeline
	logRecycler                 logRecycler
	memTableSize                int
	memTableStopWritesThreshold uint64
	largeBatchThreshold         uint64

	readState struct {
		sync.RWMutex
		val *plainReadState
	}

	mu struct {
		sync.Mutex
		meta     *metadata
		arrtable *flushableEntry

		log struct {
			queue []fileInfo
			*record.LogWriter
		}

		mem struct {
			cond      sync.Cond
			mutable   *memTable
			queue     flushableList
			switching bool
		}

		compact struct {
			cond     sync.Cond
			flushing bool
		}
	}
}

func (p *plainDB) Close() (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.mu.compact.flushing {
		p.mu.compact.cond.Wait()
	}

	if p.mu.log.LogWriter != nil {
		err = utils.FirstError(err, p.mu.log.Close())
	}

	p.readState.val.unref()

	for _, mem := range p.mu.mem.queue {
		mem.readerUnref()
	}

	if p.mu.arrtable != nil {
		p.mu.arrtable.readerUnref()
	}

	err = utils.FirstError(err, p.dataDir.Close())
	err = utils.FirstError(err, p.mu.meta.close())

	p.logger.Infof("plainDB close finish")
	return err
}

func (p *plainDB) Get(key []byte) ([]byte, func(), error) {
	rs := p.loadReadState()

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

	if rs.arrtable != nil {
		value, exist, _ := rs.arrtable.get(key)
		if exist {
			return value, func() { rs.unref() }, nil
		}
	}

	rs.unref()
	return nil, nil, ErrNotFound
}

func (p *plainDB) NewIter(o *IterOptions) *Iterator {
	rs := p.loadReadState()
	seqNum := atomic.LoadUint64(&p.mu.meta.atomic.visibleSeqNum)
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		alloc:               buf,
		cmp:                 p.db.cmp,
		equal:               p.db.equal,
		iter:                &buf.merging,
		split:               p.db.split,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		plainReadState:      rs,
	}
	if o != nil {
		dbi.opts = *o
	}
	dbi.opts.Logger = p.logger

	mlevels := buf.mlevels[:0]
	numMergingLevels := 1

	for i := len(rs.memtables) - 1; i >= 0; i-- {
		mem := rs.memtables[i]
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		numMergingLevels++
	}

	var atIter internalIterator
	if rs.arrtable != nil {
		atIter = rs.arrtable.newIter(&dbi.opts)
		numMergingLevels++
	}

	if numMergingLevels > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, numMergingLevels)
	}

	for i := len(rs.memtables) - 1; i >= 0; i-- {
		mem := rs.memtables[i]
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		mlevels = append(mlevels, mergingIterLevel{
			iter: mem.newIter(&dbi.opts),
		})
	}

	if rs.arrtable != nil {
		mlevels = append(mlevels, mergingIterLevel{
			iter: atIter,
		})
	}

	buf.merging.init(&dbi.opts, dbi.cmp, mlevels...)
	return dbi
}

func (p *plainDB) manualFlush() error {
	flushed, err := p.manualAsyncFlush()
	if err != nil {
		return err
	}
	if flushed != nil {
		<-flushed
	}
	return nil
}

func (p *plainDB) manualAsyncFlush() (<-chan struct{}, error) {
	if p.db.IsClosed() {
		return nil, ErrClosed
	}

	p.commit.mu.Lock()
	defer p.commit.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	empty := true
	for i := range p.mu.mem.queue {
		if !p.mu.mem.queue[i].empty() {
			empty = false
			break
		}
	}
	if empty {
		return nil, nil
	}
	flushed := p.mu.mem.queue[len(p.mu.mem.queue)-1].flushed
	err := p.makeRoomForWrite(nil)
	if err != nil {
		return nil, err
	}
	return flushed, nil
}

func (p *plainDB) makeWalFilename(fileNum FileNum) string {
	return base.MakeFilepath(p.db.opts.FS, p.dirname, fileTypeLog, fileNum)
}

func (p *plainDB) makeAtFilename(fileNum FileNum) string {
	return base.MakeFilepath(p.db.opts.FS, p.dirname, base.FileTypeArrayTable, fileNum)
}

func (p *plainDB) newMemTable(logNum FileNum, logSeqNum uint64) (*memTable, *flushableEntry) {
	mem := newMemTable(memTableOptions{
		Options:   p.db.opts,
		size:      p.memTableSize,
		logSeqNum: logSeqNum,
	})

	entry := newFlushableEntry(mem, logNum, logSeqNum)
	entry.release = func() {
		manual.Free(mem.arenaBuf)
		mem.arenaBuf = nil
	}

	return mem, entry
}

func (p *plainDB) newArrayTable(path string, fn FileNum, exist bool) (*arrayTable, *flushableEntry, error) {
	var err error
	var at *arrayTable

	if exist {
		at, err = openArrayTable(path)
	} else {
		at, err = newArrayTable(path)
	}
	if err != nil {
		return nil, nil, err
	}

	invariants.SetFinalizer(at, checkArrayTable)

	entry := newFlushableEntry(at, fn, 0)
	entry.release = func() {
		if err = at.close(); err != nil {
			p.db.opts.Logger.Errorf("arrayTable close fail file:%s err:%v", path, err)
		}

		if entry.obsolete {
			p.db.deleteObsoleteFile(path)
		}
	}

	return at, entry, nil
}

func (p *plainDB) walPreallocateSize() int {
	size := p.memTableSize
	size = (size / 10) + size
	return size
}

func (p *plainDB) commitApply(b *Batch, mem *memTable) error {
	if b.flushable != nil {
		return nil
	}

	err := mem.apply(b, b.SeqNum())
	if err != nil {
		return err
	}

	if mem.writerUnref() {
		p.mu.Lock()
		p.maybeScheduleFlush()
		p.mu.Unlock()
	}
	return nil
}

func (p *plainDB) commitWrite(b *Batch, syncWG *sync.WaitGroup, syncErr *error) (*memTable, error) {
	repr := b.Repr()

	if b.flushable != nil {
		b.flushable.setSeqNum(b.SeqNum())
		if !p.disableWAL {
			if _, err := p.mu.log.SyncRecord(repr, syncWG, syncErr); err != nil {
				return nil, err
			}
		}
	}

	p.mu.Lock()
	err := p.makeRoomForWrite(b)
	mem := p.mu.mem.mutable
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if p.disableWAL {
		return mem, nil
	}

	if b.flushable == nil {
		if _, err = p.mu.log.SyncRecord(repr, syncWG, syncErr); err != nil {
			return nil, err
		}
	}

	return mem, err
}

func (p *plainDB) makeRoomForWrite(b *Batch) error {
	force := b == nil || b.flushable != nil
	stalled := false
	for {
		if p.mu.mem.switching {
			p.mu.mem.cond.Wait()
			continue
		}
		if b != nil && b.flushable == nil {
			if err := p.mu.mem.mutable.prepare(b); err != base.ErrTableFull {
				if stalled {
					p.logger.Error("[PLAINDB] write stall ending")
				}
				return err
			}
		} else if !force {
			if stalled {
				p.logger.Error("[PLAINDB] write stall ending")
			}
			return nil
		}

		{
			var size uint64
			for i := range p.mu.mem.queue {
				size += p.mu.mem.queue[i].totalBytes()
			}
			if size >= p.memTableStopWritesThreshold {
				if !stalled {
					stalled = true
					p.logger.Error("[PLAINDB] write stall beginning: memtable count limit reached")
				}
				p.mu.compact.cond.Wait()
				continue
			}
		}

		var newLogNum FileNum
		var newLogFile vfs.File
		var newLogSize uint64
		var prevLogSize uint64
		var err error

		if !p.disableWAL {
			newLogNum = p.mu.meta.getNextFileNum()
			p.mu.mem.switching = true
			prevLogSize = uint64(p.mu.log.Size())

			if p.mu.log.queue[len(p.mu.log.queue)-1].fileSize < prevLogSize {
				p.mu.log.queue[len(p.mu.log.queue)-1].fileSize = prevLogSize
			}

			p.mu.Unlock()

			err = p.mu.log.Close()
			newLogName := p.makeWalFilename(newLogNum)

			var recycleLog fileInfo
			var recycleOK bool
			if err == nil {
				recycleLog, recycleOK = p.logRecycler.peek()
				if recycleOK {
					recycleLogName := p.makeWalFilename(recycleLog.fileNum)
					newLogFile, err = p.db.opts.FS.ReuseForWrite(recycleLogName, newLogName)
				} else {
					newLogFile, err = p.db.opts.FS.Create(newLogName)
				}
			}

			if err == nil && recycleOK {
				var finfo os.FileInfo
				finfo, err = newLogFile.Stat()
				if err == nil {
					newLogSize = uint64(finfo.Size())
				}
			}

			if err == nil {
				err = p.walDir.Sync()
			}

			if err != nil && newLogFile != nil {
				newLogFile.Close()
			} else if err == nil {
				newLogFile = vfs.NewSyncingFile(newLogFile, vfs.SyncingFileOptions{
					BytesPerSync:    p.db.opts.WALBytesPerSync,
					PreallocateSize: p.walPreallocateSize(),
				})
			}

			if recycleOK {
				err = utils.FirstError(err, p.logRecycler.pop(recycleLog.fileNum))
			}

			p.logger.Infof("[PLAINDB] WAL created %s (recycled %s)", newLogNum, recycleLog.fileNum)

			p.mu.Lock()
			p.mu.mem.switching = false
			p.mu.mem.cond.Broadcast()
		}

		if err != nil {
			p.logger.Errorf("panic: makeRoomForWrite err:%s", err)
			return err
		}

		if !p.disableWAL {
			p.mu.log.queue = append(p.mu.log.queue, fileInfo{fileNum: newLogNum, fileSize: newLogSize})
			p.mu.log.LogWriter = record.NewLogWriter(newLogFile, newLogNum)
		}

		immMem := p.mu.mem.mutable
		imm := p.mu.mem.queue[len(p.mu.mem.queue)-1]
		imm.logSize = prevLogSize
		imm.flushForced = imm.flushForced || (b == nil)

		if b != nil && b.flushable != nil {
			entry := newFlushableEntry(b.flushable, imm.logNum, b.SeqNum())
			entry.release = func() {}
			p.mu.mem.queue = append(p.mu.mem.queue, entry)
			imm.logNum = 0
		}

		var logSeqNum uint64
		if b != nil {
			logSeqNum = b.SeqNum()
			if b.flushable != nil {
				logSeqNum += uint64(b.Count())
			}
		} else {
			logSeqNum = p.mu.meta.getLogSeqNum()
		}

		var entry *flushableEntry
		p.mu.mem.mutable, entry = p.newMemTable(newLogNum, logSeqNum)
		p.mu.mem.queue = append(p.mu.mem.queue, entry)
		p.updateReadState()
		if immMem.writerUnref() {
			p.maybeScheduleFlush()
		}
		force = false
	}
}

func (p *plainDB) loadReadState() *plainReadState {
	p.readState.RLock()
	state := p.readState.val
	state.ref()
	p.readState.RUnlock()
	return state
}

func (p *plainDB) updateReadState() {
	rs := &plainReadState{
		memtables: p.mu.mem.queue,
		arrtable:  p.mu.arrtable,
	}
	rs.refcnt.Store(1)

	for _, mem := range rs.memtables {
		mem.readerRef()
	}

	if rs.arrtable != nil {
		rs.arrtable.readerRef()
	}

	p.readState.Lock()
	old := p.readState.val
	p.readState.val = rs
	p.readState.Unlock()

	if old != nil {
		old.unref()
	}
}

func (p *plainDB) passedFlushThreshold() bool {
	var n int
	var size uint64
	for ; n < len(p.mu.mem.queue)-1; n++ {
		if !p.mu.mem.queue[n].readyForFlush() {
			break
		}
		if p.mu.mem.queue[n].flushForced {
			size += uint64(p.memTableSize)
		} else {
			size += p.mu.mem.queue[n].totalBytes()
		}
	}
	if n == 0 {
		return false
	}

	minFlushSize := uint64(p.memTableSize) / 2
	return size >= minFlushSize
}

func (p *plainDB) replayWAL(filename string, logNum FileNum) (maxSeqNum uint64, err error) {
	file, err := p.db.opts.FS.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		b               Batch
		buf             bytes.Buffer
		mem             *memTable
		entry           *flushableEntry
		toFlush         flushableList
		rr              = record.NewReader(file, logNum)
		offset          int64
		lastFlushOffset int64
	)

	flushMem := func() {
		if mem == nil {
			return
		}
		var logSize uint64
		if offset >= lastFlushOffset {
			logSize = uint64(offset - lastFlushOffset)
		}
		lastFlushOffset = offset
		entry.logSize = logSize
		toFlush = append(toFlush, entry)
		mem, entry = nil, nil
	}

	ensureMem := func(seqNum uint64) {
		if mem != nil {
			return
		}
		mem, entry = p.newMemTable(logNum, seqNum)
	}

	for {
		offset = rr.Offset()
		r, err := rr.Next()
		if err == nil {
			_, err = io.Copy(&buf, r)
		}
		if err != nil {
			if err == io.EOF {
				break
			} else if record.IsInvalidRecord(err) {
				break
			}
			return 0, err
		}

		if buf.Len() < batchHeaderLen {
			return 0, errors.Errorf("bitaloslog: corrupt log file %s (num %s)", filename, logNum)
		}

		b = Batch{
			db: p.db,
		}
		b.SetRepr(buf.Bytes())
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + uint64(b.Count())

		if b.memTableSize >= p.largeBatchThreshold {
			flushMem()
			b.data = append([]byte(nil), b.data...)
			b.flushable = newFlushableBatch(&b, p.db.opts.Comparer)
			entry := newFlushableEntry(b.flushable, logNum, b.SeqNum())
			entry.readerRefs.Add(1)
			toFlush = append(toFlush, entry)
		} else {
			ensureMem(seqNum)
			if err = mem.prepare(&b); err != nil && err != arenaskl.ErrArenaFull {
				return 0, err
			}
			for err == arenaskl.ErrArenaFull {
				flushMem()
				ensureMem(seqNum)
				err = mem.prepare(&b)
				if err != nil && err != arenaskl.ErrArenaFull {
					return 0, err
				}
			}
			if err = mem.apply(&b, seqNum); err != nil {
				return 0, err
			}
			mem.writerUnref()
		}
		buf.Reset()
	}
	flushMem()

	if len(toFlush) > 0 {
		if p.mu.arrtable != nil {
			toFlush = append(toFlush, p.mu.arrtable)
		}
		c := newFlush(p.db.opts, toFlush)
		atEntry, err := p.runFlush(c, len(toFlush))
		if err != nil {
			return 0, err
		}
		p.mu.arrtable = atEntry
		for i := range toFlush {
			toFlush[i].setObsolete()
			toFlush[i].readerUnref()
		}
	}

	return maxSeqNum, err
}
