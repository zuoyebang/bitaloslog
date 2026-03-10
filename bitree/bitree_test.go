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
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitaloslog/internal/options"
	"github.com/zuoyebang/bitaloslog/internal/utils"
)

func testNewBitree() (bt *Bitree) {
	_, err := os.Stat(testDirname)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(testDirname, 0775)
		if err != nil {
			panic(err)
		}
	}

	optsPool := options.InitTestOptionsPool()
	bitreeOpts := optsPool.CloneBitreeOptions()
	bitreeOpts.MaxStSize = 1 << 20
	bt, err = OpenBitree(testDirname, bitreeOpts)
	if err != nil {
		panic(err)
	}
	return bt
}

func makeTestKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key[:], uint64(i))
	return key
}

func makeTestValue(i int, v []byte) []byte {
	return []byte(fmt.Sprintf("%s_%d", v, i))
}

func TestBdb(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	bt := testNewBitree()

	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			continue
		}
		key := makeTestKey(i)
		val := makeTestValue(i, key)
		require.NoError(t, bt.bdbSet(key, val))
	}

	bt.bdbUpdate()

	bdbIter := bt.newBdbIter()
	for i := 0; i < 10; i++ {
		key := makeTestKey(i)
		ik, iv := bdbIter.SeekGE(key)
		fmt.Println(ik.String(), string(iv))
	}
	require.NoError(t, bdbIter.Close())

	require.NoError(t, bt.Close())
}

func TestBitreeWrite(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	bt := testNewBitree()

	start, end := 0, 0
	step := 1000
	prefixValue := utils.FuncRandBytes(1024)
	readData := func() {
		for i := 0; i < end; i++ {
			key := makeTestKey(i)
			expVal := makeTestValue(i, prefixValue)
			value, exist, closer := bt.Get(key)
			require.Equal(t, true, exist)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		flusher := bt.NewFlusher()
		for j := start; j < end; j++ {
			key := makeTestKey(j)
			value := makeTestValue(j, prefixValue)
			require.NoError(t, flusher.Set(key, value))
		}
		require.NoError(t, flusher.Finish())
		start += step
	}

	readData()

	require.NoError(t, bt.Close())

	bt = testNewBitree()
	readData()

	for i := 0; i < 5; i++ {
		end = start + step
		flusher := bt.NewFlusher()
		for j := start; j < end; j++ {
			key := makeTestKey(j)
			value := makeTestValue(j, prefixValue)
			require.NoError(t, flusher.Set(key, value))
		}
		require.NoError(t, flusher.Finish())
		start += step
	}
	readData()
	require.NoError(t, bt.Close())

	bt = testNewBitree()
	readData()
	require.NoError(t, bt.Close())
}

func TestBitreeIterator(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	bt := testNewBitree()

	start, end := 10, 0
	step := 1000
	prefixValue := utils.FuncRandBytes(1024)
	readData := func() {
		for i := start; i < end; i++ {
			key := makeTestKey(i)
			expVal := makeTestValue(i, prefixValue)
			value, exist, closer := bt.Get(key)
			require.Equal(t, true, exist)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		flusher := bt.NewFlusher()
		for j := start; j < end; j++ {
			key := makeTestKey(j)
			value := makeTestValue(j, prefixValue)
			require.NoError(t, flusher.Set(key, value))
		}
		require.NoError(t, flusher.Finish())
		start += step
	}

	start = 10
	readData()

	seekGE := func(it *BitreeIterator, i int) {
		if i >= end {
			ik, iv := it.SeekGE(makeTestKey(i))
			require.Equal(t, (*InternalKey)(nil), ik)
			require.Equal(t, []byte(nil), iv)
		} else {
			j := i
			if i < start {
				j = start
			}
			expKey := makeTestKey(j)
			expVal := makeTestValue(j, prefixValue)
			ik, iv := it.SeekGE(makeTestKey(i))
			require.Equal(t, expKey, ik.UserKey)
			require.Equal(t, expVal, iv)
		}
	}

	seekLT := func(it *BitreeIterator, i int) {
		j := i - 1
		if j < start {
			ik, iv := it.SeekLT(makeTestKey(i))
			require.Equal(t, (*InternalKey)(nil), ik)
			require.Equal(t, []byte(nil), iv)
		} else {
			if j >= end {
				j = end - 1
			} else {
				j = i - 1
			}
			expKey := makeTestKey(j)
			expVal := makeTestValue(j, prefixValue)
			ik, iv := it.SeekLT(makeTestKey(i))
			require.Equal(t, expKey, ik.UserKey)
			require.Equal(t, expVal, iv)
		}
	}

	iterCheck := func() {
		iter := bt.NewIter(nil)
		i := start
		for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
			key := makeTestKey(i)
			expVal := makeTestValue(i, prefixValue)
			require.Equal(t, key, ik.UserKey)
			require.Equal(t, expVal, iv)
			i++
		}
		i = end - 1
		for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
			key := makeTestKey(i)
			expVal := makeTestValue(i, prefixValue)
			require.Equal(t, key, ik.UserKey)
			require.Equal(t, expVal, iv)
			i--
		}

		for i = start - 2; i < end+2; i++ {
			seekGE(iter, i)
			seekLT(iter, i)
		}

		require.NoError(t, iter.Close())
	}

	iterCheck()
	require.NoError(t, bt.Close())

	bt = testNewBitree()
	iterCheck()
	require.NoError(t, bt.Close())
}

func TestBitreeCompact(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	bt := testNewBitree()

	start, end := 0, 0
	step := 1000
	prefixValue := utils.FuncRandBytes(1024)
	readData := func() {
		for i := 0; i < end; i++ {
			key := makeTestKey(i)
			expVal := makeTestValue(i, prefixValue)
			value, exist, closer := bt.Get(key)
			require.Equal(t, true, exist)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		flusher := bt.NewFlusher()
		for j := start; j < end; j++ {
			key := makeTestKey(j)
			value := makeTestValue(j, prefixValue)
			require.NoError(t, flusher.Set(key, value))
		}
		require.NoError(t, flusher.Finish())
		start += step
	}

	readData()
	require.NoError(t, bt.Close())

	bt = testNewBitree()
	readData()

	for i := 0; i < 5; i++ {
		end = start + step
		flusher := bt.NewFlusher()
		for j := start; j < end; j++ {
			key := makeTestKey(j)
			value := makeTestValue(j, prefixValue)
			require.NoError(t, flusher.Set(key, value))
		}
		require.NoError(t, flusher.Finish())
		start += step
	}

	bt.CompactTo(makeTestKey(5000))
	time.Sleep(2 * time.Second)
	require.NoError(t, bt.Close())

	bt = testNewBitree()
	time.Sleep(2 * time.Second)
	require.NoError(t, bt.Close())
}
