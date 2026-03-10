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
	"path/filepath"
	"sort"

	"github.com/zuoyebang/bitaloslog/bitree"
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/compress"
	"github.com/zuoyebang/bitaloslog/internal/errors"
	"github.com/zuoyebang/bitaloslog/internal/options"
	"github.com/zuoyebang/bitaloslog/internal/record"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

func Open(dirname string, opts *Options) (db *DB, err error) {
	var optsPool *options.OptionsPool
	opts = opts.Clone().EnsureDefaults()
	opts.Logger.Infof("open bitaloslog start")
	if opts.private.optspool == nil {
		optsPool = opts.ensureOptionsPool(nil)
	} else {
		optsPool = opts.private.optspool
	}

	d := &DB{
		dirname:    dirname,
		opts:       opts,
		optspool:   optsPool,
		cmp:        opts.Comparer.Compare,
		equal:      opts.Comparer.Equal,
		split:      opts.Comparer.Split,
		compressor: compress.SetCompressor(opts.CompressionType),
	}

	if err = opts.FS.MkdirAll(dirname, 0755); err != nil {
		return nil, err
	}
	d.dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	fileLock, err := opts.FS.Lock(base.MakeFilepath(opts.FS, dirname, fileTypeLock, 0))
	if err != nil {
		d.dataDir.Close()
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	if !d.opts.DisablePlainDB {
		if err = d.newPlainDB(); err != nil {
			return nil, err
		}
	}

	if err = d.newSequentialDB(); err != nil {
		return nil, err
	}

	d.fileLock, fileLock = fileLock, nil
	d.opts.Logger.Infof("open bitaloslog success")
	return d, nil
}

func (d *DB) newPlainDB() error {
	p := &plainDB{
		db:                  d,
		logger:              d.opts.Logger,
		memTableSize:        d.opts.PlainMemTableSize,
		largeBatchThreshold: uint64((d.opts.PlainMemTableSize - int(memTableEmptySize)) / 2),
		logRecycler:         logRecycler{limit: d.opts.PlainMemTableStopWritesThreshold + 1},
		disableWAL:          d.opts.PlainDisableWAL,
	}
	p.memTableStopWritesThreshold = uint64(d.opts.PlainMemTableStopWritesThreshold * p.memTableSize)
	p.dirname = filepath.Join(d.dirname, "plaindb")
	p.walDirname = p.dirname

	var err error
	if err = d.opts.FS.MkdirAll(p.dirname, 0755); err != nil {
		return err
	}
	p.dataDir, err = d.opts.FS.OpenDir(p.dirname)
	if err != nil {
		return err
	}

	if err = d.opts.FS.MkdirAll(p.walDirname, 0755); err != nil {
		return err
	}
	p.walDir, err = d.opts.FS.OpenDir(p.walDirname)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.meta, err = openMetadata(p.dirname, d.opts)
	if err != nil {
		return errors.Errorf("open meta fail err:%s", err)
	}

	p.commit = newCommitPipeline(commitEnv{
		logSeqNum:     &p.mu.meta.atomic.logSeqNum,
		visibleSeqNum: &p.mu.meta.atomic.visibleSeqNum,
		apply:         p.commitApply,
		write:         p.commitWrite,
		useQueue:      !p.disableWAL,
	})

	p.mu.mem.cond.L = &p.mu.Mutex
	p.mu.compact.cond.L = &p.mu.Mutex

	var entry *flushableEntry
	p.mu.mem.mutable, entry = p.newMemTable(0, p.mu.meta.atomic.logSeqNum)
	p.mu.mem.queue = append(p.mu.mem.queue, entry)

	ls, err := d.opts.FS.List(p.walDirname)
	if err != nil {
		return err
	}
	type fileNumAndName struct {
		num  FileNum
		name string
	}
	var logFiles []fileNumAndName
	var maxSeqNum uint64
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}

		p.mu.meta.markFileNumUsed(fn)

		switch ft {
		case fileTypeLog:
			if fn >= p.mu.meta.minUnflushedLogNum {
				logFiles = append(logFiles, fileNumAndName{fn, filename})
			}
			if p.logRecycler.minRecycleLogNum <= fn {
				p.logRecycler.minRecycleLogNum = fn + 1
			}
		case FileTypeArrayTable:
			atPath := d.opts.FS.PathJoin(p.dirname, filename)
			if fn == p.mu.meta.plainDbAtFileNum {
				_, p.mu.arrtable, err = p.newArrayTable(atPath, fn, true)
				if err != nil {
					return err
				}
			} else {
				p.db.deleteObsoleteFile(atPath)
			}
		}
	}

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].num < logFiles[j].num
	})

	for _, lf := range logFiles {
		walFilename := d.opts.FS.PathJoin(p.walDirname, lf.name)
		maxSeqNum, err = p.replayWAL(walFilename, lf.num)
		if err != nil {
			return err
		}
		d.opts.Logger.Infof("[PLAINDB] replayWAL ok wal:%s maxSeqNum:%d", walFilename, maxSeqNum)
		p.mu.meta.markFileNumUsed(lf.num)
		p.mu.meta.markLogSeqNum(maxSeqNum)
	}

	newLogNum := p.mu.meta.getNextFileNum()
	sme := &metaSet{minUnflushedLogNum: newLogNum}
	if err = p.mu.meta.apply(sme); err != nil {
		return err
	}

	if !p.disableWAL {
		newLogName := p.makeWalFilename(newLogNum)
		p.mu.log.queue = append(p.mu.log.queue, fileInfo{fileNum: newLogNum, fileSize: 0})
		logFile, err := d.opts.FS.Create(newLogName)
		if err != nil {
			return err
		}
		if err = p.walDir.Sync(); err != nil {
			return err
		}

		p.mu.mem.queue[len(p.mu.mem.queue)-1].logNum = newLogNum

		logFile = vfs.NewSyncingFile(logFile, vfs.SyncingFileOptions{
			BytesPerSync:    d.opts.WALBytesPerSync,
			PreallocateSize: p.walPreallocateSize(),
		})
		p.mu.log.LogWriter = record.NewLogWriter(logFile, newLogNum)
		p.logger.Infof("[PLAINDB] WAL created %s", newLogNum)
	}

	p.updateReadState()
	p.scanObsoleteFiles(ls)
	p.doDeleteObsoleteFiles()
	d.plaindb = p
	p.mu.meta.atomic.visibleSeqNum = p.mu.meta.atomic.logSeqNum
	p.logger.Infof("open plainDB success")
	return nil
}

func (d *DB) newSequentialDB() error {
	s := &sequentialDB{
		db:           d,
		logger:       d.opts.Logger,
		memTableSize: d.opts.SequentialMemTableSize,
	}
	s.dirname = filepath.Join(d.dirname, "sequentialdb")

	var err error
	if err = d.opts.FS.MkdirAll(s.dirname, 0755); err != nil {
		return err
	}
	s.dataDir, err = d.opts.FS.OpenDir(s.dirname)
	if err != nil {
		return err
	}

	s.mem.compact.cond.L = &s.mem.RWMutex

	s.mem.Lock()
	defer s.mem.Unlock()

	s.meta, err = openSequentialMeta(s.dirname, d.opts)
	if err != nil {
		return errors.Errorf("init bitalosdb meta fail err:%s", err)
	}

	bitreeOpts := d.optspool.CloneBitreeOptions()
	bitreeOpts.MaxStSize = d.opts.SuperTableMaxSize
	bitreeOpts.SuperTableKeyLength = d.opts.SequentialKeyLength
	s.btree, err = bitree.OpenBitree(s.dirname, bitreeOpts)
	if err != nil {
		return err
	}

	var entry *flushableEntry
	s.mem.mutable, entry = s.newMemTable()
	s.mem.queue = append(s.mem.queue, entry)

	s.updateReadState()
	d.seqdb = s
	s.logger.Infof("open sequentialDB success")
	return nil
}
