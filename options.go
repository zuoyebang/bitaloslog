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
	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/compress"
	"github.com/zuoyebang/bitaloslog/internal/consts"
	"github.com/zuoyebang/bitaloslog/internal/options"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

type IterOptions = options.IterOptions

type WriteOptions struct {
	Sync bool
}

var (
	PlainSync = &WriteOptions{
		Sync: true,
	}
	PlainNoSync = &WriteOptions{
		Sync: false,
	}
)

func (o *WriteOptions) GetSync() bool {
	return o == nil || o.Sync
}

type Options struct {
	BytesPerSync                          int
	WALBytesPerSync                       int
	PlainDisableWAL                       bool
	Comparer                              *Comparer
	FS                                    vfs.FS
	Logger                                Logger
	CompressionType                       int
	Verbose                               bool
	LogTag                                string
	DeleteFileInternal                    int
	DisablePlainDB                        bool
	PlainMemTableSize                     int
	PlainMemTableStopWritesThreshold      int
	SequentialMemTableSize                int
	SequentialMemTableStopWritesThreshold int
	SequentialKeyLength                   int
	SuperTableMaxSize                     int
	GetNowTimestamp                       func() uint64
	DecodeSequentialKey                   func([]byte) uint64

	private struct {
		logInit  bool
		optspool *options.OptionsPool
	}
}

func (o *Options) ensureOptionsPool(optspool *options.OptionsPool) *options.OptionsPool {
	if optspool == nil {
		optspool = options.InitOptionsPool()
	}

	optspool.BaseOptions.FS = o.FS
	optspool.BaseOptions.Cmp = o.Comparer.Compare
	optspool.BaseOptions.Logger = o.Logger
	optspool.BaseOptions.Compressor = compress.SetCompressor(o.CompressionType)
	optspool.BaseOptions.BytesPerSync = o.BytesPerSync
	if o.GetNowTimestamp != nil {
		optspool.BaseOptions.GetNowTimestamp = o.GetNowTimestamp
	}
	if o.DecodeSequentialKey != nil {
		optspool.BaseOptions.DecodeSequentialKey = o.DecodeSequentialKey
	}

	dflOpts := &base.DFLOption{
		Logger:         o.Logger,
		DeleteInterval: o.DeleteFileInternal,
	}
	optspool.BaseOptions.DeleteFilePacer.Run(dflOpts)

	return optspool
}

func (o *Options) EnsureDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.BytesPerSync <= 0 {
		o.BytesPerSync = consts.BytesPerSyncDefault
	}
	if o.Comparer == nil {
		o.Comparer = DefaultComparer
	}
	if o.DeleteFileInternal == 0 {
		o.DeleteFileInternal = consts.DeletionFileInterval
	}
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.PlainMemTableSize <= 0 {
		o.PlainMemTableSize = consts.PlainMemTableSize
	}
	if o.PlainMemTableStopWritesThreshold <= 0 {
		o.PlainMemTableStopWritesThreshold = consts.PlainMemTableStopWritesThreshold
	}
	if o.SequentialMemTableSize <= 0 {
		o.SequentialMemTableSize = consts.SequentialMemTableSize
	}
	if o.SequentialMemTableStopWritesThreshold <= 0 {
		o.SequentialMemTableStopWritesThreshold = consts.SequentialMemTableStopWritesThreshold
	}
	if o.SequentialKeyLength <= 0 {
		o.SequentialKeyLength = consts.SuperTableKeyLength
	}
	if o.SuperTableMaxSize <= 0 {
		o.SuperTableMaxSize = consts.SuperTableMaxSize
	}
	if !o.private.logInit {
		o.Logger = base.NewLogger(o.Logger, o.LogTag)
		o.private.logInit = true
	}

	return o
}

func (o *Options) Clone() *Options {
	n := &Options{}
	if o != nil {
		*n = *o
	}
	return n
}
