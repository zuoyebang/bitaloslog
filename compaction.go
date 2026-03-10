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
	"errors"
	"runtime/pprof"
	"sort"
)

var errFlushInvariant = errors.New("bitaloslog: flush next log number is unset")
var flushLabels = pprof.Labels("bitalosdb", "flush")

type fileInfo struct {
	fileNum  FileNum
	fileSize uint64
}

type compaction struct {
	cmp           Compare
	logger        Logger
	flushing      flushableList
	bytesIterated uint64
	bytesWritten  int64
	keyWritten    int64
	keyFail       int64
}

func newFlush(opts *Options, flushing flushableList) *compaction {
	return &compaction{
		cmp:      opts.Comparer.Compare,
		logger:   opts.Logger,
		flushing: flushing,
	}
}

func (c *compaction) newInputIter() internalIterator {
	if len(c.flushing) == 1 {
		f := c.flushing[0]
		iter := f.newFlushIter(nil, &c.bytesIterated)
		return iter
	}
	iters := make([]internalIterator, 0, len(c.flushing))
	for i := range c.flushing {
		f := c.flushing[i]
		iters = append(iters, f.newFlushIter(nil, &c.bytesIterated))
	}
	return newMergingIter(c.logger, c.cmp, iters...)
}

func (c *compaction) String() string {
	return "memtable flush\n"
}

func merge(a, b []fileInfo) []fileInfo {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	sort.Slice(a, func(i, j int) bool {
		return a[i].fileNum < a[j].fileNum
	})

	n := 0
	for i := 0; i < len(a); i++ {
		if n == 0 || a[i].fileNum != a[n-1].fileNum {
			a[n] = a[i]
			n++
		}
	}
	return a[:n]
}
