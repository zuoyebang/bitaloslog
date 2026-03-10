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

	"github.com/zuoyebang/bitaloslog/internal/base"
)

var _ base.InternalIterator = (*arrayTableIterator)(nil)

func (a *arrayTable) newIter(o *IterOptions) internalIterator {
	if o == nil {
		o = &IterOptions{}
	}
	iter := &arrayTableIterator{
		at:    a,
		lower: o.LowerBound,
		upper: o.UpperBound,
	}
	return iter
}

func (a *arrayTable) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	return &arrayTableFlushIterator{
		arrayTableIterator: arrayTableIterator{at: a},
		bytesIterated:      bytesFlushed,
	}
}

type arrayTableIterator struct {
	at          *arrayTable
	intIndexPos int
	iterKey     InternalKey
	iterValue   []byte
	lower       []byte
	upper       []byte
}

func (i *arrayTableIterator) findItem() (*InternalKey, []byte) {
	key, value := i.at.getKV(i.intIndexPos)
	if key == nil {
		return nil, nil
	}

	i.iterKey = base.MakeInternalSetKey(key)
	i.iterValue = value
	return &i.iterKey, i.iterValue
}

func (i *arrayTableIterator) First() (*InternalKey, []byte) {
	i.intIndexPos = 0
	k, v := i.findItem()
	if i.upper != nil && bytes.Compare(i.upper, k.UserKey) <= 0 {
		return nil, nil
	}
	return k, v
}

func (i *arrayTableIterator) Next() (*InternalKey, []byte) {
	i.intIndexPos++
	k, v := i.findItem()
	if i.upper != nil && bytes.Compare(i.upper, k.UserKey) <= 0 {
		return nil, nil
	}
	return k, v
}

func (i *arrayTableIterator) Prev() (*InternalKey, []byte) {
	i.intIndexPos--
	k, v := i.findItem()
	if i.lower != nil && bytes.Compare(i.lower, k.UserKey) > 0 {
		return nil, nil
	}
	return k, v
}

func (i *arrayTableIterator) Last() (*InternalKey, []byte) {
	i.intIndexPos = i.at.num - 1
	k, v := i.findItem()
	if i.lower != nil && bytes.Compare(i.lower, k.UserKey) > 0 {
		return nil, nil
	}
	return k, v
}

func (i *arrayTableIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	i.intIndexPos = i.at.findKeyIndexPos(key)
	k, v := i.findItem()
	if i.upper != nil && bytes.Compare(i.upper, k.UserKey) <= 0 {
		return nil, nil
	}
	return k, v
}

func (i *arrayTableIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	i.intIndexPos = i.at.findKeyIndexPos(key)
	poskey, _ := i.findItem()
	if poskey != nil {
		return i.Prev()
	}

	lastKey, lastValue := i.Last()
	if lastKey != nil && bytes.Compare(lastKey.UserKey, key) < 0 {
		if i.lower != nil && bytes.Compare(i.lower, lastKey.UserKey) > 0 {
			return nil, nil
		}
		return lastKey, lastValue
	}

	return nil, nil
}

func (i *arrayTableIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *InternalKey, value []byte) {
	return i.SeekGE(key)
}

func (i *arrayTableIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *arrayTableIterator) Error() error {
	return nil
}

func (i *arrayTableIterator) Close() error {
	i.at = nil
	i.intIndexPos = 0
	i.iterValue = nil
	i.lower = nil
	i.upper = nil
	return nil
}

func (i *arrayTableIterator) String() string {
	return "arrayTableIterator"
}

type arrayTableFlushIterator struct {
	arrayTableIterator
	bytesIterated *uint64
}

func (i *arrayTableFlushIterator) findItem() (*InternalKey, []byte) {
	key, value := i.at.getKV(i.intIndexPos)
	if key == nil {
		return nil, nil
	}

	i.iterKey = base.MakeInternalSetKey(key)
	i.iterValue = value
	return &i.iterKey, i.iterValue
}

func (i *arrayTableFlushIterator) First() (*InternalKey, []byte) {
	i.intIndexPos = 0
	k, v := i.findItem()
	if k == nil || (i.upper != nil && bytes.Compare(i.upper, k.UserKey) <= 0) {
		return nil, nil
	}
	*i.bytesIterated += uint64(k.Size() + len(v))
	return k, v
}

func (i *arrayTableFlushIterator) Next() (*InternalKey, []byte) {
	i.intIndexPos++
	k, v := i.findItem()
	if k == nil || (i.upper != nil && bytes.Compare(i.upper, k.UserKey) <= 0) {
		return nil, nil
	}
	*i.bytesIterated += uint64(k.Size() + len(v))
	return k, v
}

func (i *arrayTableFlushIterator) String() string {
	return "arrayTableFlushIterator"
}
