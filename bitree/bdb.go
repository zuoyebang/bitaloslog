// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
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

package bitree

import (
	"bytes"
	"sync"

	"github.com/zuoyebang/bitaloslog/bitree/bdb"
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/consts"
	"github.com/zuoyebang/bitaloslog/internal/options"
)

func (t *Bitree) openBDB(path string, opts *options.BdbOptions) error {
	db, err := bdb.Open(path, opts)
	if err != nil {
		return err
	}

	var tx *bdb.Tx
	tx, err = db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	t.bdb = db
	bucket := tx.Bucket(consts.BdbBucketName)
	if bucket != nil {
		_ = tx.Rollback()
		return nil
	}

	bucket, err = tx.CreateBucket(consts.BdbBucketName)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (t *Bitree) bdbSet(key, value []byte) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bucket := tx.Bucket(consts.BdbBucketName)
		if bucket == nil {
			return bdb.ErrBucketNotFound
		}
		return bucket.Put(key, value)
	})
	return err
}

func (t *Bitree) bdbDelete(key []byte) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bucket := tx.Bucket(consts.BdbBucketName)
		if bucket == nil {
			return bdb.ErrBucketNotFound
		}
		return bucket.Delete(key)
	})
	return err
}

func (t *Bitree) bdbUpdate() bool {
	_ = t.bdb.Update(func(tx *bdb.Tx) error { return nil })
	return t.txPool.Update()
}

func (t *Bitree) newBdbIter() *bdb.BdbIterator {
	rtx := t.txPool.Load()
	return t.bdb.NewIter(rtx)
}

func (t *Bitree) findKeyFileNum(key []byte) ([]byte, FileNum, func()) {
	rtx := t.txPool.Load()
	rtxCloser := func() {
		rtx.Unref(true)
	}

	skey, fileNum := t.findBdbKey(key, rtx.Bucket().Cursor())
	return skey, fileNum, rtxCloser
}

func (t *Bitree) findBdbKey(key []byte, cursor *bdb.Cursor) ([]byte, FileNum) {
	sk, v := cursor.Seek(key)
	if v == nil {
		return nil, FileNum(0)
	}
	return sk, FileNum(base.DecodeFileNum(v))
}

func (t *Bitree) switchBdbKey(oldKey []byte, oldFn, newFn FileNum) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bkt := tx.Bucket(consts.BdbBucketName)
		if bkt == nil {
			return bdb.ErrBucketNotFound
		}

		if e := bkt.Put(oldKey, base.EncodeFileNum(base.FileNum(oldFn))); e != nil {
			return e
		}

		if e := bkt.Put(consts.BdbMaxKey, base.EncodeFileNum(base.FileNum(newFn))); e != nil {
			return e
		}

		return nil
	})
	if err != nil {
		return err
	}
	t.bdbUpdate()
	return nil
}

func (t *Bitree) isBdbMaxKey(key []byte) bool {
	return bytes.Equal(key, consts.BdbMaxKey)
}

func (t *Bitree) setBdbMaxKey(fn FileNum) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bucket := tx.Bucket(consts.BdbBucketName)
		if bucket == nil {
			return bdb.ErrBucketNotFound
		}
		return bucket.Put(consts.BdbMaxKey, base.EncodeFileNum(base.FileNum(fn)))
	})
	if err != nil {
		return err
	}
	t.bdbUpdate()
	return nil
}

func (t *Bitree) deleteBdbKeys(keys [][]byte) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bucket := tx.Bucket(consts.BdbBucketName)
		if bucket == nil {
			return bdb.ErrBucketNotFound
		}
		for _, key := range keys {
			if e := bucket.Delete(key); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	t.bdbUpdate()
	return nil
}

type TxPool struct {
	lock sync.RWMutex
	rTx  *bdb.ReadTx
	bdb  *bdb.DB
}

func (t *Bitree) openTxPool() error {
	if t.bdb == nil {
		return base.ErrBdbNotExist
	}

	tx, err := t.bdb.Begin(false)
	if err != nil {
		return err
	}

	bkt := tx.Bucket(consts.BdbBucketName)
	if bkt == nil {
		return base.ErrBucketNotExist
	}

	rt := &bdb.ReadTx{}
	rt.Init(tx, bkt, t.bdb)
	t.txPool = &TxPool{
		rTx: rt,
		bdb: t.bdb,
	}

	return nil
}

func (tp *TxPool) Load() *bdb.ReadTx {
	tp.lock.RLock()
	rTx := tp.rTx
	rTx.Ref()
	tp.lock.RUnlock()
	return rTx
}

func (tp *TxPool) Update() bool {
	tx, err := tp.bdb.Begin(false)
	if err != nil {
		return false
	}

	bkt := tx.Bucket(consts.BdbBucketName)
	if bkt == nil {
		return false
	}

	rt := &bdb.ReadTx{}
	rt.Init(tx, bkt, tp.bdb)

	tp.lock.Lock()
	prev := tp.rTx
	tp.rTx = rt
	tp.lock.Unlock()

	if prev != nil {
		prev.Unref(true)
	}

	return true
}

func (tp *TxPool) Close() error {
	tp.lock.RLock()
	rTx := tp.rTx
	tp.lock.RUnlock()
	return rTx.Unref(false)
}
