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
	"github.com/zuoyebang/bitaloslog/bitree/bdb"
	"github.com/zuoyebang/bitaloslog/internal/base"
)

type BitreeIterator struct {
	btree     *Bitree
	opts      *IterOptions
	cmp       base.Compare
	compact   bool
	err       error
	iterKey   *InternalKey
	iterValue []byte
	ikey      *InternalKey
	value     []byte
	putPools  []func()
	lower     []byte
	upper     []byte
	bdbIter   *bdb.BdbIterator
	stIter    *superTableIterator
	stIters   map[FileNum]*superTableIterator
}

func (i *BitreeIterator) getKV() (*InternalKey, []byte) {
	if i.iterKey == nil {
		return nil, nil
	}

	//v, err := i.btree.opts.Compressor.Decode(nil, i.iterValue)
	//if err != nil {
	//	return nil, nil
	//}
	//i.iterValue = v
	return i.iterKey, i.iterValue
}

func (i *BitreeIterator) setStMapIter(v []byte) bool {
	fn := FileNum(base.DecodeFileNum(v))
	stIter, ok := i.stIters[fn]
	if !ok {
		st := i.btree.getStMap(fn)
		if st == nil {
			return false
		}
		stIter = st.newIter(i.opts)
		i.stIters[fn] = stIter
	}

	i.stIter = stIter
	return true
}

func (i *BitreeIterator) findBdbFirst() bool {
	bdbKey, bdbValue := i.bdbIter.First()
	if bdbKey == nil {
		return false
	}

	return i.setStMapIter(bdbValue)
}

func (i *BitreeIterator) findBdbLast() bool {
	bdbKey, bdbValue := i.bdbIter.Last()
	if bdbKey == nil {
		return false
	}

	return i.setStMapIter(bdbValue)
}

func (i *BitreeIterator) findBdbNext() bool {
	bdbKey, bdbValue := i.bdbIter.Next()
	if bdbKey == nil {
		return false
	}

	return i.setStMapIter(bdbValue)
}

func (i *BitreeIterator) findBdbPrev() bool {
	bdbKey, bdbValue := i.bdbIter.Prev()
	if bdbKey == nil {
		return false
	}

	return i.setStMapIter(bdbValue)
}

func (i *BitreeIterator) findBdbSeekGE(key []byte) bool {
	bdbKey, bdbValue := i.bdbIter.SeekGE(key)
	if bdbKey == nil {
		return false
	}

	return i.setStMapIter(bdbValue)
}

func (i *BitreeIterator) First() (*InternalKey, []byte) {
	if !i.findBdbFirst() {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.stIter.First()
	for i.iterKey == nil {
		if !i.findBdbNext() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.stIter.First()
	}

	if i.upper != nil && i.cmp(i.upper, i.iterKey.UserKey) <= 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Last() (*InternalKey, []byte) {
	if !i.findBdbLast() {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.stIter.Last()
	for i.iterKey == nil {
		if !i.findBdbPrev() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.stIter.Last()
	}

	if i.lower != nil && i.cmp(i.lower, i.iterKey.UserKey) > 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Next() (*InternalKey, []byte) {
	if i.iterKey == nil {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.stIter.Next()
	for i.iterKey == nil {
		if !i.findBdbNext() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.stIter.First()
	}

	if i.upper != nil && i.cmp(i.upper, i.iterKey.UserKey) <= 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Prev() (*InternalKey, []byte) {
	if i.iterKey == nil {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.stIter.Prev()
	for i.iterKey == nil {
		if !i.findBdbPrev() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.stIter.Last()
	}

	if i.lower != nil && i.cmp(i.lower, i.iterKey.UserKey) > 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	if !i.findBdbSeekGE(key) {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.stIter.SeekGE(key)
	for i.iterKey == nil {
		if !i.findBdbNext() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.stIter.SeekGE(key)
	}

	if i.upper != nil && i.cmp(i.upper, i.iterKey.UserKey) <= 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	if !i.findBdbSeekGE(key) {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.stIter.SeekLT(key)
	for i.iterKey == nil {
		if !i.findBdbPrev() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.stIter.SeekLT(key)
	}

	if i.lower != nil && i.cmp(i.lower, i.iterKey.UserKey) > 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	return i.SeekGE(key)
}

func (i *BitreeIterator) Close() error {
	if len(i.putPools) > 0 {
		for _, f := range i.putPools {
			f()
		}
	}

	for _, pageIter := range i.stIters {
		if err := pageIter.Close(); err != nil && i.err == nil {
			i.err = err
		}
	}

	if err := i.bdbIter.Close(); err != nil && i.err == nil {
		i.err = err
	}

	return i.err
}

func (i *BitreeIterator) Error() error {
	return nil
}

func (i *BitreeIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *BitreeIterator) SetCompact() {
	i.compact = true
}

func (i *BitreeIterator) String() string {
	return "BitreeIterator"
}
