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
	"io"
	"sync/atomic"

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/compress"
	"github.com/zuoyebang/bitaloslog/internal/errors"
	"github.com/zuoyebang/bitaloslog/internal/options"
	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

var (
	ErrNotFound = base.ErrNotFound
	ErrClosed   = errors.New("bitaloslog: closed")
)

const (
	DBTypePlainDB int = 1 + iota
	DBTypeSequentialDB
)

type DB struct {
	dirname    string
	opts       *Options
	optspool   *options.OptionsPool
	cmp        Compare
	equal      Equal
	split      Split
	compressor compress.Compressor
	fileLock   io.Closer
	dataDir    vfs.File
	closed     atomic.Bool
	plaindb    *plainDB
	seqdb      *sequentialDB
}

func (d *DB) Close() (err error) {
	if d.IsClosed() {
		return ErrClosed
	}

	d.opts.Logger.Infof("bitaloslog close start")
	d.closed.Store(true)

	if !d.opts.DisablePlainDB {
		err = utils.FirstError(err, d.plaindb.Close())
	}

	err = utils.FirstError(err, d.seqdb.Close())
	err = utils.FirstError(err, d.fileLock.Close())
	err = utils.FirstError(err, d.dataDir.Close())

	d.optspool.Close()
	d.opts.Logger.Infof("bitaloslog close finish")

	return err
}

func (d *DB) IsClosed() bool {
	return d.closed.Load()
}

func (d *DB) NewBatch() *Batch {
	return newBatch(d)
}

func (d *DB) Apply(batch *Batch, opts *WriteOptions) error {
	if d.IsClosed() || d.opts.DisablePlainDB {
		return ErrClosed
	}

	if atomic.LoadUint32(&batch.applied) != 0 {
		return errors.New("bitaloslog: batch already applied")
	}

	if batch.db != nil && batch.db != d {
		return errors.Errorf("bitaloslog: batch db mismatch: %p != %p", batch.db, d)
	}

	if batch.db == nil {
		batch.refreshMemTableSize()
	}

	var err error
	isSync := opts.GetSync()
	if isSync && d.plaindb.disableWAL {
		return errors.New("bitaloslog: PlainDB WAL disabled")
	}
	if batch.memTableSize >= d.plaindb.largeBatchThreshold {
		batch.flushable = newFlushableBatch(batch, d.opts.Comparer)
	}
	err = d.plaindb.commit.Commit(batch, isSync)
	if err != nil {
		return errors.Errorf("bitaloslog: apply commit fail err:%v", err)
	}

	if batch.flushable != nil {
		batch.data = nil
	}
	return nil
}

func (d *DB) Get(key []byte, dbType int) ([]byte, func(), error) {
	if dbType == DBTypeSequentialDB {
		return d.seqdb.Get(key)
	}

	if !d.opts.DisablePlainDB {
		return d.plaindb.Get(key)
	}

	return nil, nil, ErrNotFound
}

func (d *DB) NewIter(o *IterOptions) *Iterator {
	if o == nil {
		o = &IterOptions{}
	}

	if o.DbType == DBTypePlainDB && !d.opts.DisablePlainDB {
		return d.plaindb.NewIter(o)
	}

	return d.seqdb.NewIter(o)
}

func (d *DB) SetPlainDB(key, value []byte, o *WriteOptions) error {
	if o == nil {
		o = PlainNoSync
	}
	b := newBatch(d)
	_ = b.Set(key, value, o)
	if err := d.Apply(b, o); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) DeletePlainDB(key []byte, o *WriteOptions) error {
	if o == nil {
		o = PlainNoSync
	}
	b := newBatch(d)
	_ = b.Delete(key, o)
	if err := d.Apply(b, o); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) SetSeqDB(key, value []byte) error {
	return d.seqdb.Set(key, value, InternalKeyKindSet)
}

func (d *DB) DeleteSeqDB(key []byte) error {
	return d.seqdb.Set(key, nil, InternalKeyKindDelete)
}

func (d *DB) Flush() error {
	flushed, err := d.AsyncFlush()
	if err != nil {
		return err
	}
	if flushed != nil {
		<-flushed
	}
	return nil
}

func (d *DB) AsyncFlush() (<-chan struct{}, error) {
	if d.IsClosed() {
		return nil, ErrClosed
	}

	flushed := make(chan struct{})
	flusheds := make([]<-chan struct{}, 0, 2)

	if !d.opts.DisablePlainDB {
		ch, _ := d.plaindb.manualAsyncFlush()
		if ch != nil {
			flusheds = append(flusheds, ch)
		}
	}

	ch, _ := d.seqdb.manualAsyncFlush()
	if ch != nil {
		flusheds = append(flusheds, ch)
	}

	if len(flusheds) == 0 {
		return nil, nil
	}

	go func() {
		for _, c := range flusheds {
			<-c
		}
		close(flushed)
	}()

	return flushed, nil
}

func (d *DB) CompactSequentialDB(key []byte) {
	d.seqdb.Compact(key)
}

func (d *DB) deleteObsoleteFile(filename string) {
	if utils.IsNotExist(filename) {
		return
	}

	d.optspool.BaseOptions.DeleteFilePacer.AddFile(filename)
}
