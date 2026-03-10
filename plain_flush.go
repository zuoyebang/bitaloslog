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

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/humanize"
)

func (p *plainDB) maybeScheduleFlush() {
	if p.mu.compact.flushing || p.db.IsClosed() || len(p.mu.mem.queue) <= 1 {
		return
	}

	if !p.passedFlushThreshold() {
		return
	}

	p.mu.compact.flushing = true

	go p.flush()
}

func (p *plainDB) flush() {
	defer func() {
		if r := recover(); r != any(nil) {
			p.db.opts.Logger.Errorf("[PLAINDB] flush panic err:%v stack:%s", r, string(debug.Stack()))
		}
	}()

	p.mu.Lock()
	defer p.mu.Unlock()

	defer func() {
		p.mu.compact.flushing = false
		p.maybeScheduleFlush()
		p.mu.compact.cond.Broadcast()
	}()

	if err := p.flush1(); err != nil {
		p.db.opts.Logger.Errorf("[PLAINDB] flush1 err:%s", err)
	}
}

func (p *plainDB) flush1() error {
	var n int
	var flushing flushableList

	for ; n < len(p.mu.mem.queue)-1; n++ {
		if !p.mu.mem.queue[n].readyForFlush() {
			break
		}
	}

	if n == 0 {
		return nil
	}

	minUnflushedLogNum := p.mu.mem.queue[n].logNum
	for i := 0; i < n; i++ {
		if !p.disableWAL {
			logNum := p.mu.mem.queue[i].logNum
			if logNum >= minUnflushedLogNum {
				return errFlushInvariant
			}
		}

		flushing = append(flushing, p.mu.mem.queue[i])
	}

	if p.mu.arrtable != nil {
		flushing = append(flushing, p.mu.arrtable)
	}

	c := newFlush(p.db.opts, flushing)
	atEntry, err := p.runFlush(c, n)
	if err == nil {
		sme := &metaSet{
			minUnflushedLogNum: minUnflushedLogNum,
			plainDbAtFileNum:   atEntry.logNum,
		}
		err = p.mu.meta.apply(sme)
	}

	if err == nil {
		p.mu.mem.queue = p.mu.mem.queue[n:]
		p.mu.arrtable = atEntry
		p.updateReadState()
	}

	p.doDeleteObsoleteFiles()

	p.mu.Unlock()
	defer p.mu.Lock()

	if err == nil {
		for i := range flushing {
			flushing[i].setObsolete()
			flushing[i].readerUnref()
			close(flushing[i].flushed)
		}
	}

	return err
}

func (p *plainDB) runFlush(c *compaction, memNum int) (atEntry *flushableEntry, err error) {
	p.mu.Unlock()
	defer p.mu.Lock()

	iter := &compactionIter{
		cmp:  c.cmp,
		iter: c.newInputIter(),
	}

	defer func() {
		iter.Close()
		if err != nil && atEntry != nil {
			atEntry.setObsolete()
			atEntry.readerUnref()
		}
	}()

	logTag := fmt.Sprintf("[PLAINDB] flushing %d memTable to arrayTable", memNum)
	startTime := time.Now()
	p.db.opts.Logger.Infof("%s start", logTag)
	defer func() {
		costTime := time.Since(startTime)
		cost := costTime.Seconds()
		p.db.opts.Logger.Infof("%s done iterated(%s) written(%s) keys(%d) keysFail(%d), in %.3fs, output rate %s/s",
			logTag,
			humanize.Uint64(c.bytesIterated),
			humanize.Int64(c.bytesWritten),
			c.keyWritten,
			c.keyFail,
			cost,
			humanize.Uint64(uint64(float64(c.bytesWritten)/cost)))
	}()

	var (
		n  uint32
		at *arrayTable
	)

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		if at == nil {
			newLogNum := p.mu.meta.getNextFileNum()
			path := p.makeAtFilename(newLogNum)
			at, atEntry, err = p.newArrayTable(path, newLogNum, false)
			if err != nil {
				return nil, err
			}
		}

		n, err = at.set(key.UserKey, val)
		if err != nil {
			continue
		}

		c.bytesWritten += int64(n)
		c.keyWritten++
	}

	if at != nil {
		if err = at.writeFinish(); err != nil {
			return nil, err
		}
	}

	return atEntry, nil
}

func (p *plainDB) doDeleteObsoleteFiles() {
	var obsoleteLogs []fileInfo
	for i := range p.mu.log.queue {
		if p.mu.log.queue[i].fileNum >= p.mu.meta.minUnflushedLogNum {
			obsoleteLogs = p.mu.log.queue[:i]
			p.mu.log.queue = p.mu.log.queue[i:]
			break
		}
	}

	p.mu.Unlock()
	defer p.mu.Lock()

	for _, f := range obsoleteLogs {
		if p.logRecycler.add(f) {
			continue
		}

		filename := p.makeWalFilename(f.fileNum)
		p.db.deleteObsoleteFile(filename)

		p.logger.Infof("[PLAINDB] WAL deleted %s", f.fileNum)
	}
}

func (p *plainDB) scanObsoleteFiles(list []string) {
	if p.mu.compact.flushing {
		return
	}

	var obsoleteLogs []fileInfo
	for _, filename := range list {
		ft, fn, ok := base.ParseFilename(p.db.opts.FS, filename)
		if ok && ft == fileTypeLog && fn < p.mu.meta.minUnflushedLogNum {
			fi := fileInfo{fileNum: fn}
			if stat, err := p.db.opts.FS.Stat(filename); err == nil {
				fi.fileSize = uint64(stat.Size())
			}
			obsoleteLogs = append(obsoleteLogs, fi)
		}
	}

	p.mu.log.queue = merge(p.mu.log.queue, obsoleteLogs)
}
