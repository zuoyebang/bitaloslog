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
	"fmt"
	"runtime/debug"
	"time"

	"github.com/zuoyebang/bitaloslog/internal/humanize"
	"github.com/zuoyebang/bitaloslog/internal/invariants"
	"github.com/zuoyebang/bitaloslog/internal/iterator"
	"github.com/zuoyebang/bitaloslog/internal/utils"
)

func (s *sequentialDB) newArrayTable(path string, fn FileNum, exist bool) (*arrayTable, *flushableEntry, error) {
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
			s.logger.Errorf("arrayTable close fail file:%s err:%v", path, err)
		}

		if entry.obsolete {
			s.db.deleteObsoleteFile(path)
		}
	}

	return at, entry, nil
}

func (s *sequentialDB) maybeScheduleFlush() {
	if s.mem.compact.flushing || s.db.IsClosed() || len(s.mem.queue) <= 1 {
		return
	}

	s.mem.compact.flushing = true

	go s.flush()
}

func (s *sequentialDB) flush() {
	defer func() {
		if r := recover(); r != any(nil) {
			s.logger.Errorf("[SEQDB] flush panic err:%v stack:%s", r, string(debug.Stack()))
		}
	}()

	s.mem.Lock()
	defer s.mem.Unlock()

	defer func() {
		s.mem.compact.flushing = false
		s.maybeScheduleFlush()
		s.mem.compact.cond.Broadcast()
	}()

	var n int
	for ; n < len(s.mem.queue)-1; n++ {
		if !s.mem.queue[n].readyForFlush() {
			break
		}
	}
	if n == 0 {
		return
	}

	var flushed flushableList
	err := s.runFlush(s.mem.queue[:n])
	if err == nil {
		flushed = s.mem.queue[:n]
		s.mem.queue = s.mem.queue[n:]
		s.updateReadState()
	}

	s.mem.Unlock()
	defer s.mem.Lock()

	for i := range flushed {
		flushed[i].readerUnref()
		close(flushed[i].flushed)
	}
}

func (s *sequentialDB) runFlush(flushing flushableList) (err error) {
	s.mem.Unlock()
	defer s.mem.Lock()

	var (
		flushIter     internalIterator
		bytesIterated uint64
		bytesWritten  int64
		keyWritten    int64
		keyFail       int64
	)

	flushNum := len(flushing)
	if flushNum == 1 {
		if flushing[0].empty() {
			return nil
		}

		flushIter = flushing[0].newFlushIter(nil, &bytesIterated)
	} else {
		iters := make([]internalIterator, 0, flushNum)
		for i := range flushing {
			if flushing[i].empty() {
				continue
			}
			iters = append(iters, flushing[i].newFlushIter(nil, &bytesIterated))
		}

		if len(iters) == 0 {
			return nil
		}

		flushIter = iterator.NewMergingIter(s.logger, s.db.cmp, iters...)
	}

	iter := &compactionIter{
		cmp:  s.db.cmp,
		iter: flushIter,
	}
	defer func() {
		err = utils.FirstError(err, iter.Close())
	}()

	writer := s.btree.NewFlusher()
	defer func() {
		err = writer.Finish()
	}()

	logTag := fmt.Sprintf("[SEQDB] flushing %d memTable to bitree", flushNum)
	startTime := time.Now()
	s.logger.Infof("%s start", logTag)
	defer func() {
		costTime := time.Since(startTime)
		cost := costTime.Seconds()
		s.logger.Infof("%s done iterated(%s) written(%s) keys(%d) keysFail(%d), in %.3fs, output rate %s/s",
			logTag,
			humanize.Uint64(bytesIterated),
			humanize.Int64(bytesWritten),
			keyWritten,
			keyFail,
			cost,
			humanize.Uint64(uint64(float64(bytesWritten)/cost)))
	}()

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		if err = writer.Set(key.UserKey, val); err != nil {
			keyFail++
			continue
		}

		bytesWritten += int64(key.Size() + len(val))
		keyWritten++
	}

	return nil
}
