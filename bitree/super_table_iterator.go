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
	"sort"

	"github.com/zuoyebang/bitaloslog/internal/base"
)

type superTableIterator struct {
	st        *superTable
	keyNum    int
	keyPos    int
	iterKey   InternalKey
	iterValue []byte
	putPools  []func()
	lower     []byte
	upper     []byte
}

func (s *superTable) newIter(o *IterOptions) *superTableIterator {
	if o == nil {
		o = &IterOptions{}
	}
	it := &superTableIterator{
		st:     s,
		lower:  o.LowerBound,
		upper:  o.UpperBound,
		keyNum: int(s.keyReadable.Load()),
	}

	return it
}

func (i *superTableIterator) findKeyIndexPos(key []byte) int {
	if i.keyNum == 0 {
		return -1
	}

	return sort.Search(i.keyNum, func(p int) bool {
		return bytes.Compare(i.st.readKeyByPos(p), key) >= 0
	})
}

func (i *superTableIterator) findItem() (*InternalKey, []byte) {
	if i.keyPos < 0 || i.keyPos >= i.keyNum {
		return nil, nil
	}

	key, voff := i.st.readKVByPos(i.keyPos)
	if key == nil {
		return nil, nil
	}
	value, valueCloser := i.st.getValue(voff)
	if valueCloser != nil {
		i.putPools = append(i.putPools, valueCloser)
	}

	i.iterKey = base.MakeInternalSetKey(key)
	i.iterValue = value
	return &i.iterKey, i.iterValue
}

func (i *superTableIterator) First() (*InternalKey, []byte) {
	i.keyPos = 0
	return i.findItem()
}

func (i *superTableIterator) Next() (*InternalKey, []byte) {
	i.keyPos++
	return i.findItem()
}

func (i *superTableIterator) Prev() (*InternalKey, []byte) {
	i.keyPos--
	return i.findItem()
}

func (i *superTableIterator) Last() (*InternalKey, []byte) {
	i.keyPos = i.keyNum - 1
	return i.findItem()
}

func (i *superTableIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	i.keyPos = i.findKeyIndexPos(key)
	return i.findItem()
}

func (i *superTableIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	i.keyPos = i.findKeyIndexPos(key)
	poskey, _ := i.findItem()
	if poskey != nil {
		return i.Prev()
	}

	return i.Last()
}

func (i *superTableIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *InternalKey, value []byte) {
	return i.SeekGE(key)
}

func (i *superTableIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *superTableIterator) Error() error {
	return nil
}

func (i *superTableIterator) Close() error {
	for _, f := range i.putPools {
		f()
	}
	return nil
}

func (i *superTableIterator) String() string {
	return "superTableIterator"
}
