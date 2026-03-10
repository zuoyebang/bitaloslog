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
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
	"github.com/stretchr/testify/require"
)

const (
	testDirname = "./test-data"
)

func makeTestIntKey(i int) []byte {
	return []byte(fmt.Sprintf("testkey_%d", i))
}

func makeTestPlainDBKey(i int) []byte {
	key := make([]byte, 12)
	key[0] = 0x1
	key[1] = 0x1
	key[2] = 0
	key[3] = 0
	binary.BigEndian.PutUint64(key[:], uint64(i))
	return key
}

func makeTestIntValue(i int, v []byte) []byte {
	return []byte(fmt.Sprintf("%s_%d", v, i))
}

func openTestDB(dir string) *DB {
	opts := &Options{
		Logger:                 DefaultLogger,
		PlainDisableWAL:        false,
		SequentialKeyLength:    8,
		SequentialMemTableSize: 1 << 20,
		SuperTableMaxSize:      1 << 20,
		PlainMemTableSize:      1 << 20,
	}
	return openTestDBByOpts(dir, opts)
}

func openTestDBByOpts(dir string, opts *Options) *DB {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0775); err != nil {
			panic(err)
		}
	}

	db, err := Open(dir, opts)
	if err != nil {
		panic(err)
	}
	return db
}

func TestPlainDBOpenClose(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	fs := vfs.Default
	opts := &Options{
		FS: fs,
	}

	for _, startFromEmpty := range []bool{false, true} {
		for _, length := range []int{-1, 0, 1, 128, 256} {
			dirname := "/sharedDatabase"
			if startFromEmpty {
				dirname = "/startFromEmpty" + strconv.Itoa(length)
			}
			dirname = fs.PathJoin(dir, dirname)
			got, xxx := []byte(nil), ""
			if length >= 0 {
				xxx = strings.Repeat("x", length)
			}

			d0, err := Open(dirname, opts)
			if err != nil {
				t.Fatalf("sfe=%t, length=%d: Open #0: %v", startFromEmpty, length, err)
			}
			if length >= 0 {
				tmpk := []byte("key")
				err = d0.SetPlainDB(tmpk, []byte(xxx), PlainNoSync)
				if err != nil {
					t.Fatalf("sfe=%t, length=%d: Set: %v", startFromEmpty, length, err)
				}
			}
			err = d0.Close()
			if err != nil {
				t.Fatalf("sfe=%t, length=%d: Close #0: %v", startFromEmpty, length, err)
			}

			d1, err1 := Open(dirname, opts)
			if err1 != nil {
				t.Errorf("sfe=%t, length=%d: Open #1: %v", startFromEmpty, length, err1)
				continue
			}
			if length >= 0 {
				var closer func()
				tmpk := []byte("key")
				got, closer, err = d1.Get(tmpk, DBTypePlainDB)
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Get: %v", startFromEmpty, length, err)
					continue
				}
				got = append([]byte(nil), got...)
				closer()
			}
			err = d1.Close()
			if err != nil {
				t.Errorf("sfe=%t, length=%d: Close #1: %v", startFromEmpty, length, err)
				continue
			}

			if length >= 0 && string(got) != xxx {
				t.Errorf("sfe=%t, length=%d: got value differs from set value", startFromEmpty, length)
				continue
			}
		}
	}
}

func TestPlainDBWrite(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	start, end := 0, 0
	step := 100

	readData := func() {
		for i := 0; i < end; i++ {
			key := makeTestIntKey(i)
			expVal := makeTestIntValue(i, key)
			value, closer, err := db.Get(key, DBTypePlainDB)
			require.NoError(t, err)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		for j := start; j < end; j++ {
			key := makeTestIntKey(j)
			value := makeTestIntValue(j, key)
			require.NoError(t, db.SetPlainDB(key, value, PlainNoSync))
		}

		readData()
		require.NoError(t, db.plaindb.manualFlush())
		readData()

		start += step
	}

	readData()

	require.NoError(t, db.Close())

	db = openTestDB(dir)
	readData()
	require.NoError(t, db.Close())
}

func TestPlainDBWriteRead(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	keyNum := 10
	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, key)
		require.NoError(t, db.SetPlainDB(key, value, PlainNoSync))
	}

	readData := func() {
		for i := 0; i < keyNum; i++ {
			key := makeTestIntKey(i)
			expVal := makeTestIntValue(i, key)
			value, closer, err := db.Get(key, DBTypePlainDB)
			require.NoError(t, err)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	readData()
	require.NoError(t, db.plaindb.manualFlush())
	readData()

	require.NoError(t, db.Close())

	db = openTestDB(dir)
	readData()
	require.NoError(t, db.Close())
}

func TestPlainDBIterator(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)

	start, end := 10, 0
	step := 1000
	prefixValue := utils.FuncRandBytes(1024)
	readData := func() {
		for i := start; i < end; i++ {
			key := makeTestPlainDBKey(i)
			expVal := makeTestIntValue(i, prefixValue)
			value, closer, err := db.Get(key, DBTypePlainDB)
			require.NoError(t, err)
			require.Equal(t, expVal, value)
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		end = start + step
		for j := start; j < end; j++ {
			key := makeTestPlainDBKey(j)
			value := makeTestIntValue(j, prefixValue)
			require.NoError(t, db.SetPlainDB(key, value, PlainNoSync))
		}
		start += step
	}

	start = 10
	readData()

	seekGE := func(it *Iterator, i int) {
		if i >= end {
			require.Equal(t, false, it.SeekGE(makeTestPlainDBKey(i)))
		} else {
			j := i
			if i < start {
				j = start
			}
			expKey := makeTestPlainDBKey(j)
			expVal := makeTestIntValue(j, prefixValue)
			require.Equal(t, true, it.SeekGE(makeTestPlainDBKey(i)))
			require.Equal(t, expKey, it.Key())
			require.Equal(t, expVal, it.Value())
		}
	}

	seekLT := func(it *Iterator, i int) {
		j := i - 1

		if j < start {
			require.Equal(t, false, it.SeekLT(makeTestPlainDBKey(i)))
		} else {
			if j >= end {
				j = end - 1
			} else {
				j = i - 1
			}
			expKey := makeTestPlainDBKey(j)
			expVal := makeTestIntValue(j, prefixValue)
			require.Equal(t, true, it.SeekLT(makeTestPlainDBKey(i)))
			require.Equal(t, expKey, it.Key())
			require.Equal(t, expVal, it.Value())
		}
	}

	iterCheck := func() {
		o := &IterOptions{
			DbType: DBTypePlainDB,
		}
		iter := db.NewIter(o)
		i := start
		for iter.First(); iter.Valid(); iter.Next() {
			key := makeTestPlainDBKey(i)
			expVal := makeTestIntValue(i, prefixValue)
			if !bytes.Equal(iter.Key(), key) {
				t.Fatalf("got i:%d key=%s, want key=%s", i, string(iter.Key()), string(key))
			}
			require.Equal(t, key, iter.Key())
			require.Equal(t, expVal, iter.Value())
			i++
		}
		i = end - 1
		for iter.Last(); iter.Valid(); iter.Prev() {
			key := makeTestPlainDBKey(i)
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
}
