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
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestOpenPlainDBWALReplay(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	largeValue := []byte(strings.Repeat("a", 100<<10))
	hugeValue := []byte(strings.Repeat("b", 1<<20))
	keys := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		keys[i] = makeTestIntKey(i)
	}

	checkIter := func(iter *Iterator) {
		t.Helper()

		i := 0
		for valid := iter.First(); valid; valid = iter.Next() {
			require.Equal(t, keys[i], iter.Key())
			i++
		}
		require.NoError(t, iter.Close())
	}

	d := openTestDB(dir)
	require.NoError(t, d.SetPlainDB(keys[0], largeValue, nil))
	require.NoError(t, d.SetPlainDB(keys[1], largeValue, nil))
	require.NoError(t, d.SetPlainDB(keys[2], largeValue, nil))
	require.NoError(t, d.SetPlainDB(keys[3], hugeValue, nil))
	require.NoError(t, d.SetPlainDB(keys[4], largeValue, nil))
	checkIter(d.NewIter(nil))
	require.NoError(t, d.Close())

	files, err := d.opts.FS.List(d.plaindb.walDirname)
	require.NoError(t, err)
	sort.Strings(files)
	logCount := 0
	for _, fname := range files {
		if strings.HasSuffix(fname, ".log") {
			logCount++
		}
	}

	require.Equal(t, 2, logCount)

	d = openTestDB(dir)
	checkIter(d.NewIter(nil))
	require.NoError(t, d.Close())
}

func TestOpenPlainDBWALReplay2(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	for _, reason := range []string{"forced", "size"} {
		t.Run(reason, func(t *testing.T) {
			d := openTestDB(dir)
			switch reason {
			case "forced":
				require.NoError(t, d.SetPlainDB([]byte("1"), nil, nil))
				require.NoError(t, d.Flush())
				require.NoError(t, d.SetPlainDB([]byte("2"), nil, nil))
			case "size":
				largeValue := []byte(strings.Repeat("a", 100<<10))
				require.NoError(t, d.SetPlainDB([]byte("1"), largeValue, nil))
				require.NoError(t, d.SetPlainDB([]byte("2"), largeValue, nil))
				require.NoError(t, d.SetPlainDB([]byte("3"), largeValue, nil))
			}
			require.NoError(t, d.Close())
			d = openTestDB(dir)
			require.NoError(t, d.Close())
		})
	}
}

func TestOpenPlainDBWALReplay3(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	d := openTestDB(dir)
	d.plaindb.mu.compact.flushing = true
	num := 2048
	val := utils.FuncRandBytes(1024)
	for i := 0; i < num; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, d.SetPlainDB(key, val, nil))
	}
	d.plaindb.mu.compact.flushing = false
	memNum := len(d.plaindb.mu.mem.queue)
	require.Equal(t, 3, memNum)
	require.NoError(t, d.Close())

	var expFns, actFns []FileNum
	for i := 1; i <= memNum; i++ {
		expFns = append(expFns, FileNum(i))
	}
	ls, _ := d.opts.FS.List(d.plaindb.walDirname)
	sort.Strings(ls)
	for _, filename := range ls {
		if ft, fn, ok := base.ParseFilename(d.opts.FS, filename); ok && ft == base.FileTypeLog {
			actFns = append(actFns, fn)
		}
	}
	require.Equal(t, expFns, actFns)

	d = openTestDB(dir)
	require.Equal(t, FileNum(7), d.plaindb.mu.meta.minUnflushedLogNum)
	d.optspool.BaseOptions.DeleteFilePacer.Flush()

	for _, fn := range actFns {
		walFile := d.plaindb.makeWalFilename(fn)
		if utils.IsExist(walFile) {
			t.Fatalf("obsolete wal exist file:%s", walFile)
		}
	}

	require.NoError(t, d.Close())

	d = openTestDB(dir)
	for i := 0; i < num*2; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, d.SetPlainDB(key, val, nil))
	}
	require.NoError(t, d.Close())
}
