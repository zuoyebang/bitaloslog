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

package bitree

import (
	"bytes"
	"path/filepath"
	"sync"

	"github.com/zuoyebang/bitaloslog/bitree/bdb"
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/options"
	"github.com/zuoyebang/bitaloslog/internal/utils"
)

type Bitree struct {
	dirname     string
	opts        *options.BitreeOptions
	meta        *metadata
	closed      bool
	bdb         *bdb.DB
	txPool      *TxPool
	bdbPath     string
	maxStSize   uint64
	stMap       sync.Map
	stMutable   *superTable
	stKeyLength int
}

func OpenBitree(path string, opts *options.BitreeOptions) (*Bitree, error) {
	var err error
	bitreePath := filepath.Join(path, "bitree")
	if err = opts.FS.MkdirAll(bitreePath, 0755); err != nil {
		return nil, err
	}

	t := &Bitree{
		dirname:     bitreePath,
		opts:        opts,
		maxStSize:   uint64(opts.MaxStSize),
		stMap:       sync.Map{},
		stKeyLength: opts.SuperTableKeyLength,
	}

	defer func() {
		if err != nil {
			if t.txPool != nil {
				_ = t.txPool.Close()
			}
			if t.bdb != nil {
				_ = t.bdb.Close()
			}
		}
	}()

	metaOpts := &metaOptions{
		fs:     t.opts.FS,
		logger: t.opts.Logger,
		path:   t.dirname,
	}
	t.meta, err = openMetadata(metaOpts)
	if err != nil {
		return nil, err
	}

	opts.BdbOpts.DoFreed = t.doStFreed
	opts.BdbOpts.CheckFreed = t.checkStFreed
	t.bdbPath = base.MakeFilepath(opts.FS, t.dirname, base.FileTypeBdb, 0)
	if err = t.openBDB(t.bdbPath, opts.BdbOpts); err != nil {
		return nil, err
	}
	if err = t.openTxPool(); err != nil {
		return nil, err
	}

	if err = t.openSuperTables(); err != nil {
		return nil, err
	}

	t.opts.Logger.Infof("open bitree success")
	return t, nil
}

func (t *Bitree) getStMap(fn FileNum) *superTable {
	p, ok := t.stMap.Load(fn)
	if !ok {
		return nil
	}

	return p.(*superTable)
}

func (t *Bitree) setStMap(st *superTable) {
	t.stMap.Store(st.fn, st)
}

func (t *Bitree) delStMap(fn FileNum) {
	t.stMap.Delete(fn)
}

func (t *Bitree) checkStFreed(fn base.FileNum) bool {
	if FileNum(fn) <= t.meta.getMinUnCompactFileNum() {
		return true
	}
	return false
}

func (t *Bitree) Get(key []byte) ([]byte, bool, func()) {
	_, fn, rtxCloser := t.findKeyFileNum(key)
	defer rtxCloser()
	if fn == FileNum(0) {
		return nil, false, nil
	}

	st := t.getStMap(fn)
	if st == nil {
		return nil, false, nil
	}

	value, closer := st.get(key)
	if value == nil {
		return nil, false, nil
	}
	return value, true, closer

	//v, err := t.opts.Compressor.Decode(nil, value)
	//if err != nil {
	//	closer()
	//	return nil, false, nil
	//}
	//
	//return v, true, closer
}

func (t *Bitree) Set(key, value []byte) error {
	return t.stMutable.set(key, value)
}

func (t *Bitree) doStFreed(fns []base.FileNum) {
	for _, fn := range fns {
		t.freeSuperTable(FileNum(fn))
		t.opts.Logger.Infof("bitree free superTable(%d) finish", fn)
	}
}

func (t *Bitree) NewIter(o *IterOptions) *BitreeIterator {
	iter := &BitreeIterator{
		btree:   t,
		cmp:     t.opts.Cmp,
		opts:    o,
		bdbIter: t.newBdbIter(),
		stIters: make(map[FileNum]*superTableIterator, 4),
	}
	iter.SetBounds(o.GetLowerBound(), o.GetUpperBound())
	return iter
}

func (t *Bitree) Close() (err error) {
	if t.closed {
		return nil
	}

	t.closed = true

	if t.txPool != nil {
		err = utils.FirstError(err, t.txPool.Close())
		t.txPool = nil
	}

	if t.bdb != nil {
		err = utils.FirstError(err, t.bdb.Close())
		t.bdb = nil
	}

	err = utils.FirstError(err, t.meta.close())
	t.opts.Logger.Infof("bitree close finish")
	return err
}

func (t *Bitree) newSuperTable() (*superTable, error) {
	newFn := t.meta.getNextFileNum()
	opts := &stOptions{
		fs:        t.opts.FS,
		path:      t.dirname,
		fn:        newFn,
		logger:    t.opts.Logger,
		keyLength: t.stKeyLength,
	}
	st, err := newSuperTable(opts)
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (t *Bitree) freeSuperTable(fn FileNum) {
	st := t.getStMap(fn)
	if st == nil {
		return
	}

	t.delStMap(fn)

	_ = st.close()
	t.deleteObsoleteFile(st.keyTbl.path)
	t.deleteObsoleteFile(st.valueTbl.path)
	st = nil
}

func (t *Bitree) makeRoomForWrite(maxKey []byte) error {
	st, err := t.newSuperTable()
	if err != nil {
		return err
	}

	oldFn := t.stMutable.fn
	newFn := st.fn
	if err = t.switchBdbKey(maxKey, oldFn, newFn); err != nil {
		t.opts.Logger.Errorf("bitree makeRoomForWrite bdb switch key fail err:%s", err)
		return err
	}

	t.setStMap(st)
	t.stMutable = st

	t.opts.Logger.Infof("bitree flush switch superTable %d to %d success maxKey:%d",
		oldFn, newFn, t.opts.DecodeSequentialKey(maxKey))
	return nil
}

func (t *Bitree) openSuperTables() error {
	files, err := t.opts.FS.List(t.dirname)
	if err != nil {
		return err
	}

	type stInfo struct {
		path      string
		keyFile   string
		valueFile string
	}

	deleteFn := make(map[FileNum]bool)
	stMaps := make(map[FileNum]*stInfo, 16)
	minUnCompactFn := t.meta.getMinUnCompactFileNum()
	for i := range files {
		ft, bfn, _ := base.ParseFilename(t.opts.FS, files[i])
		if ft != base.FileTypeSuperTable && ft != base.FileTypeSuperTableIndex {
			continue
		}

		fn := FileNum(bfn)
		if fn <= minUnCompactFn {
			deleteFn[fn] = true
			t.deleteObsoleteFile(t.opts.FS.PathJoin(t.dirname, files[i]))
			continue
		}

		if _, exist := stMaps[fn]; !exist {
			stMaps[fn] = &stInfo{}
		}

		if ft == base.FileTypeSuperTable {
			stMaps[fn].valueFile = files[i]
		} else {
			stMaps[fn].keyFile = files[i]
		}
	}

	var deleteKeys [][]byte
	if err = func() error {
		bdbIter := t.newBdbIter()
		defer bdbIter.Close()

		for ik, iv := bdbIter.First(); ik != nil; ik, iv = bdbIter.Next() {
			fn := FileNum(base.DecodeFileNum(iv))
			if _, exist := deleteFn[fn]; exist {
				deleteKeys = append(deleteKeys, utils.CloneBytes(ik.UserKey))
				continue
			}
			if _, exist := stMaps[fn]; !exist {
				deleteKeys = append(deleteKeys, utils.CloneBytes(ik.UserKey))
				t.opts.Logger.Errorf("panic bitree open bdb superTable(%d) not exist", fn)
				continue
			}

			opts := &stOptions{
				fs:        t.opts.FS,
				path:      t.dirname,
				fn:        fn,
				logger:    t.opts.Logger,
				readOnly:  !t.isBdbMaxKey(ik.UserKey),
				keyLength: t.stKeyLength,
			}
			st, e := openSuperTable(opts)
			if e != nil {
				return e
			}

			t.setStMap(st)
			if !opts.readOnly {
				t.stMutable = st
			}
		}
		return nil
	}(); err != nil {
		return err
	}

	if len(deleteKeys) > 0 {
		if err = t.deleteBdbKeys(deleteKeys); err != nil {
			return err
		}
	}

	if t.stMutable != nil {
		return nil
	}

	var st *superTable
	st, err = t.newSuperTable()
	if err != nil {
		return err
	}
	t.setStMap(st)
	if err = t.setBdbMaxKey(st.fn); err != nil {
		t.freeSuperTable(st.fn)
		return err
	}
	t.stMutable = st
	t.opts.Logger.Infof("bitree set maxKey superTable(%d) success", st.fn)
	return nil
}

func (t *Bitree) deleteObsoleteFile(filename string) {
	if utils.IsNotExist(filename) {
		return
	}

	t.opts.DeleteFilePacer.AddFile(filename)
}

func (t *Bitree) CompactTo(key []byte) {
	t.opts.Logger.Infof("bitree compact start key:%d", t.opts.DecodeSequentialKey(key))

	var deleteKeys [][]byte
	var compactToFn FileNum

	bdbIter := t.newBdbIter()
	sk, _ := bdbIter.SeekGE(key)
	if sk != nil {
		upperKey := utils.CloneBytes(sk.UserKey)
		for ik, iv := bdbIter.First(); ik != nil; ik, iv = bdbIter.Next() {
			if bytes.Compare(ik.UserKey, upperKey) >= 0 {
				break
			}
			deleteKeys = append(deleteKeys, utils.CloneBytes(ik.UserKey))
			fn := FileNum(base.DecodeFileNum(iv))
			compactToFn = max(compactToFn, fn)

			t.opts.Logger.Infof("bitree compact need delete key:%d fn:%d", t.opts.DecodeSequentialKey(ik.UserKey), fn)
		}
	}
	bdbIter.Close()

	if len(deleteKeys) == 0 {
		t.opts.Logger.Infof("bitree compact end no deleteKeys")
		return
	}

	if len(deleteKeys) > 0 {
		t.meta.setMinUnCompactFileNum(compactToFn)
		if err := t.deleteBdbKeys(deleteKeys); err != nil {
			t.opts.Logger.Errorf("bitree compact deleteBdbKeys fail err:%v deleteKeys:%+v", err, deleteKeys)
			return
		}
	}

	t.opts.Logger.Infof("bitree compact end compactToFn:%d", compactToFn)
}
