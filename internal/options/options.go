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

package options

import (
	"encoding/binary"
	"os"
	"time"

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/compress"
	"github.com/zuoyebang/bitaloslog/internal/consts"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

type IterOptions struct {
	LowerBound []byte
	UpperBound []byte
	Logger     base.Logger
	DbType     int
}

func (o *IterOptions) GetLowerBound() []byte {
	if o == nil {
		return nil
	}
	return o.LowerBound
}

func (o *IterOptions) GetUpperBound() []byte {
	if o == nil {
		return nil
	}
	return o.UpperBound
}

func (o *IterOptions) GetLogger() base.Logger {
	if o == nil || o.Logger == nil {
		return base.DefaultLogger
	}
	return o.Logger
}

type Options struct {
	Id                  int
	FS                  vfs.FS
	Cmp                 base.Compare
	Logger              base.Logger
	Compressor          compress.Compressor
	BytesPerSync        int
	DeleteFilePacer     *base.DeletionFileLimiter
	GetNowTimestamp     func() uint64
	DecodeSequentialKey func([]byte) uint64
	SuperTableKeyLength int
}

func (o *Options) Clone() *Options {
	n := &Options{}
	if o != nil {
		*n = *o
	}
	return n
}

type BdbOptions struct {
	*Options
	Index           int
	Timeout         time.Duration
	NoGrowSync      bool
	NoFreelistSync  bool
	FreelistType    string
	ReadOnly        bool
	MmapFlags       int
	InitialMmapSize int
	AllocSize       int
	PageSize        int
	NoSync          bool
	OpenFile        func(string, int, os.FileMode) (*os.File, error)
	Mlock           bool
	CheckFreed      func(base.FileNum) bool
	DoFreed         func([]base.FileNum)
}

type BitpageOptions struct {
	*Options
	Index    int
	SplitNum int
}

type BitreeOptions struct {
	*Options
	MaxStSize int
	BdbOpts   *BdbOptions
}

var DefaultBdbOptions = &BdbOptions{
	Options: &Options{
		Logger: base.DefaultLogger,
		Cmp:    base.DefaultComparer.Compare,
	},
	Timeout:      0,
	NoGrowSync:   false,
	FreelistType: consts.BdbFreelistArrayType,
}

var DefaultGetNowTimestamp = func() uint64 {
	return uint64(time.Now().UnixMilli())
}

var DefaultDecodeSequentialKey = func(k []byte) uint64 {
	if len(k) == 8 {
		return binary.BigEndian.Uint64(k)
	}
	return 0
}

func NewDefaultDeletionFileLimiter() *base.DeletionFileLimiter {
	dflOpts := &base.DFLOption{
		Logger:         base.DefaultLogger,
		DeleteInterval: consts.DeletionFileInterval,
	}
	return base.NewDeletionFileLimiter(dflOpts)
}

type OptionsPool struct {
	BaseOptions   *Options
	BitreeOptions *BitreeOptions
	BdbOptions    *BdbOptions
}

func InitOptionsPool() *OptionsPool {
	optspool := &OptionsPool{}
	optspool.BaseOptions = &Options{
		FS:                  vfs.Default,
		Cmp:                 base.DefaultComparer.Compare,
		Logger:              base.DefaultLogger,
		Compressor:          compress.SnappyCompressor,
		BytesPerSync:        consts.BytesPerSyncDefault,
		DeleteFilePacer:     NewDefaultDeletionFileLimiter(),
		GetNowTimestamp:     DefaultGetNowTimestamp,
		DecodeSequentialKey: DefaultDecodeSequentialKey,
		SuperTableKeyLength: consts.SuperTableKeyLength,
	}

	brOpts := &BitreeOptions{
		Options:   optspool.BaseOptions,
		MaxStSize: consts.SuperTableMaxSize,
	}

	bdbOpts := &BdbOptions{
		Options:         optspool.BaseOptions,
		Timeout:         time.Second,
		InitialMmapSize: consts.BdbInitialSize,
		AllocSize:       consts.BdbAllocSize,
		NoSync:          true,
		NoGrowSync:      true,
		Mlock:           false,
		FreelistType:    consts.BdbFreelistMapType,
		PageSize:        consts.BdbPageSize,
		CheckFreed:      func(num base.FileNum) bool { return false },
	}

	optspool.BdbOptions = bdbOpts
	optspool.BitreeOptions = brOpts

	return optspool
}

func (o *OptionsPool) CloneBitreeOptions() *BitreeOptions {
	bropts := &BitreeOptions{}
	*bropts = *(o.BitreeOptions)
	bdbopts := &BdbOptions{}
	*bdbopts = *(o.BdbOptions)

	bropts.BdbOpts = bdbopts
	return bropts
}

func (o *OptionsPool) Close() {
	o.BaseOptions.DeleteFilePacer.Close()
}

func InitTestOptionsPool() *OptionsPool {
	optsPool := InitOptionsPool()
	optsPool.BaseOptions.DeleteFilePacer.Run(nil)
	return optsPool
}
