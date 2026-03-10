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
)

const (
	fileTypeLog        = base.FileTypeLog
	fileTypeLock       = base.FileTypeLock
	fileTypeMeta       = base.FileTypeMeta
	FileTypeArrayTable = base.FileTypeArrayTable
)

const (
	InternalKeyKindDelete       = base.InternalKeyKindDelete
	InternalKeyKindSet          = base.InternalKeyKindSet
	InternalKeyKindLogData      = base.InternalKeyKindLogData
	InternalKeyKindPrefixDelete = base.InternalKeyKindPrefixDelete
	InternalKeyKindExpireAt     = base.InternalKeyKindExpireAt
	InternalKeyKindMax          = base.InternalKeyKindMax
	InternalKeySeqNumMax        = base.InternalKeySeqNumMax
	InternalKeyKindInvalid      = base.InternalKeyKindInvalid
)

type InternalKeyKind = base.InternalKeyKind

type InternalKey = base.InternalKey

type FileNum = base.FileNum

type Compare = base.Compare

type Equal = base.Equal

type Split = base.Split

type Comparer = base.Comparer

type Logger = base.Logger

type internalIterator = base.InternalIterator

var DefaultComparer = base.DefaultComparer

var DefaultLogger = base.DefaultLogger
