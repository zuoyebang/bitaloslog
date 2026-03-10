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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/consts"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

const testDirname = "./test-data"

func testNewSuperTable() (st *superTable) {
	opts := &stOptions{
		fs:        vfs.Default,
		path:      testDirname,
		fn:        FileNum(1),
		logger:    base.DefaultLogger,
		keyLength: consts.SuperTableKeyLength,
	}
	_, err := os.Stat(testDirname)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(testDirname, 0775)
		if err != nil {
			panic(err)
		}
	}

	st, err = newSuperTable(opts)
	if err != nil {
		panic(err)
	}
	return st
}

func testOpenSuperTable() *superTable {
	opts := &stOptions{
		fs:        vfs.Default,
		path:      testDirname,
		fn:        FileNum(1),
		logger:    base.DefaultLogger,
		keyLength: consts.SuperTableKeyLength,
	}
	st, err := openSuperTable(opts)
	if err != nil {
		panic(err)
	}
	return st
}

func TestSuperTableWrite(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	st := testNewSuperTable()

	start, end := 0, 0
	step := 1000

	readData := func() {
		for i := 0; i < end; i++ {
			key := makeTestKey(i)
			expVal := makeTestValue(i, key)
			value, closer := st.get(key)
			if !bytes.Equal(expVal, value) {
				t.Fatalf("%d key:%s expecting value %v, got %v", i, string(key), expVal, value)
			}
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		for j := start; j < end; j++ {
			key := makeTestKey(j)
			value := makeTestValue(j, key)
			require.NoError(t, st.set(key, value))
		}

		require.NoError(t, st.flushFinish())
		require.Equal(t, st.keyReadable.Load(), st.readKeyNum())

		readData()
		start += step
	}

	require.NoError(t, st.close())

	st = testOpenSuperTable()
	readData()
	require.NoError(t, st.close())
}

func TestSuperTableIterator(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	st := testNewSuperTable()
	keyNum := 1000
	start := 10

	for i := start; i < keyNum; i++ {
		key := makeTestKey(i)
		value := makeTestValue(i, key)
		require.NoError(t, st.set(key, value))
	}

	require.NoError(t, st.flushFinish())

	for i := start; i < keyNum; i++ {
		key := makeTestKey(i)
		expVal := makeTestValue(i, key)
		value, closer := st.get(key)
		require.Equal(t, expVal, value)
		closer()
	}

	seekGE := func(it *superTableIterator, i int) {
		if i >= keyNum {
			ik, iv := it.SeekGE(makeTestKey(i))
			require.Equal(t, (*InternalKey)(nil), ik)
			require.Equal(t, []byte(nil), iv)
		} else {
			j := i
			if i < start {
				j = start
			}
			expKey := makeTestKey(j)
			expVal := makeTestValue(j, expKey)
			ik, iv := it.SeekGE(makeTestKey(i))
			require.Equal(t, expKey, ik.UserKey)
			require.Equal(t, expVal, iv)
		}
	}

	seekLT := func(it *superTableIterator, i int) {
		j := i - 1
		if j < start {
			ik, iv := it.SeekLT(makeTestKey(i))
			require.Equal(t, (*InternalKey)(nil), ik)
			require.Equal(t, []byte(nil), iv)
		} else {
			if j >= keyNum {
				j = keyNum - 1
			} else {
				j = i - 1
			}
			expKey := makeTestKey(j)
			expVal := makeTestValue(j, expKey)
			ik, iv := it.SeekLT(makeTestKey(i))
			require.Equal(t, expKey, ik.UserKey)
			require.Equal(t, expVal, iv)
		}
	}

	iter := st.newIter(nil)
	i := start
	for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
		key := makeTestKey(i)
		expVal := makeTestValue(i, key)
		require.Equal(t, key, ik.UserKey)
		require.Equal(t, expVal, iv)
		i++
	}
	i = keyNum - 1
	for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
		key := makeTestKey(i)
		expVal := makeTestValue(i, key)
		require.Equal(t, key, ik.UserKey)
		require.Equal(t, expVal, iv)
		i--
	}

	for i = start - 2; i < keyNum+2; i++ {
		seekGE(iter, i)
		seekLT(iter, i)
	}

	require.NoError(t, iter.Close())

	require.NoError(t, st.close())
}

func TestSuperTableRebuild(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	st := testNewSuperTable()

	start, end := 0, 0
	step := 10

	readData := func() {
		for i := 0; i < end; i++ {
			key := makeTestKey(i)
			expVal := makeTestValue(i, key)
			value, closer := st.get(key)
			if !bytes.Equal(expVal, value) {
				t.Fatalf("%d key:%s expecting value %v, got %v", i, string(key), expVal, value)
			}
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		for j := start; j < end; j++ {
			key := makeTestKey(j)
			value := makeTestValue(j, key)
			require.NoError(t, st.set(key, value))
		}
		require.NoError(t, st.flushFinish())
		require.Equal(t, st.keyReadable.Load(), st.readKeyNum())
		start += step
	}
	readData()
	keyNum := st.readKeyNum()
	require.NoError(t, st.writeKeyNum(keyNum-1))
	require.NoError(t, st.close())

	st = testOpenSuperTable()
	readData()
	require.NoError(t, st.close())
}
