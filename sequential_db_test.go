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
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitaloslog/internal/utils"
)

func makeTestSeqDBKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key[:], uint64(i))
	return key
}

func TestSequentialDBWrite(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	start, end := 0, 0
	step := 100

	readData := func() {
		for i := 0; i < end; i++ {
			key := makeTestSeqDBKey(i)
			expVal := makeTestIntValue(i, key)
			value, closer, err := db.Get(key, DBTypeSequentialDB)
			require.NoError(t, err)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		for j := start; j < end; j++ {
			key := makeTestSeqDBKey(j)
			value := makeTestIntValue(j, key)
			require.NoError(t, db.SetSeqDB(key, value))
		}

		readData()
		require.NoError(t, db.seqdb.manualFlush())
		readData()

		start += step
	}

	readData()

	require.NoError(t, db.Close())

	db = openTestDB(dir)
	readData()
	require.NoError(t, db.Close())
}

func TestSequentialDBIterator(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)

	o := &IterOptions{
		DbType: DBTypeSequentialDB,
	}
	iter := db.NewIter(o)
	require.Equal(t, false, iter.First())
	require.Equal(t, false, iter.Last())
	require.NoError(t, iter.Close())

	start, end := 10, 0
	step := 1000
	prefixValue := utils.FuncRandBytes(1024)
	readData := func() {
		for i := start; i < end; i++ {
			key := makeTestSeqDBKey(i)
			expVal := makeTestIntValue(i, prefixValue)
			value, closer, err := db.Get(key, DBTypeSequentialDB)
			require.NoError(t, err)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		for j := start; j < end; j++ {
			key := makeTestSeqDBKey(j)
			value := makeTestIntValue(j, prefixValue)
			require.NoError(t, db.SetSeqDB(key, value))
		}
		start += step
	}

	start = 10
	readData()

	seekGE := func(it *Iterator, i int) {
		if i >= end {
			require.Equal(t, false, it.SeekGE(makeTestSeqDBKey(i)))
		} else {
			j := i
			if i < start {
				j = start
			}
			expKey := makeTestSeqDBKey(j)
			expVal := makeTestIntValue(j, prefixValue)
			require.Equal(t, true, it.SeekGE(makeTestSeqDBKey(i)))
			require.Equal(t, expKey, it.Key())
			require.Equal(t, expVal, it.Value())
		}
	}

	seekLT := func(it *Iterator, i int) {
		j := i - 1

		if j < start {
			require.Equal(t, false, it.SeekLT(makeTestSeqDBKey(i)))
		} else {
			if j >= end {
				j = end - 1
			} else {
				j = i - 1
			}
			expKey := makeTestSeqDBKey(j)
			expVal := makeTestIntValue(j, prefixValue)
			require.Equal(t, true, it.SeekLT(makeTestSeqDBKey(i)))
			require.Equal(t, expKey, it.Key())
			require.Equal(t, expVal, it.Value())
		}
	}

	iterCheck := func() {
		o = &IterOptions{
			DbType: DBTypeSequentialDB,
		}
		iter = db.NewIter(o)
		i := start
		for iter.First(); iter.Valid(); iter.Next() {
			key := makeTestSeqDBKey(i)
			expVal := makeTestIntValue(i, prefixValue)
			require.Equal(t, key, iter.Key())
			require.Equal(t, expVal, iter.Value())
			i++
		}
		i = end - 1
		for iter.Last(); iter.Valid(); iter.Prev() {
			key := makeTestSeqDBKey(i)
			expVal := makeTestIntValue(i, prefixValue)
			require.Equal(t, key, iter.Key())
			require.Equal(t, expVal, iter.Value())
			i--
		}

		for i = start - 2; i < end+2; i++ {
			seekGE(iter, i)
			seekLT(iter, i)
		}

		require.NoError(t, iter.Close())
	}

	iterCheck()
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	iterCheck()
	o = &IterOptions{
		DbType:     DBTypeSequentialDB,
		LowerBound: makeTestSeqDBKey(start),
		UpperBound: makeTestSeqDBKey(end),
	}
	iter = db.NewIter(o)
	require.Equal(t, true, iter.First())
	fi := db.optspool.BaseOptions.DecodeSequentialKey(iter.Key())
	require.Equal(t, uint64(start), fi)
	require.Equal(t, true, iter.Last())
	li := db.optspool.BaseOptions.DecodeSequentialKey(iter.Key())
	require.Equal(t, uint64(end-1), li)
	require.NoError(t, iter.Close())
	require.NoError(t, db.Close())
}
