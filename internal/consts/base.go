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

package consts

const (
	FileMode                 = 0600
	MaxKeySize           int = 62 << 10
	DeletionFileInterval     = 100
	BytesPerSyncDefault  int = 1 << 20
	BufioWriterBufSize   int = 256 << 10
)

const (
	BdbPageSize          int = 4 << 10
	BdbInitialSize       int = 256 << 20
	BdbAllocSize         int = 64 << 10
	BdbFreelistArrayType     = "array"
	BdbFreelistMapType       = "hashmap"
)

var (
	BdbBucketName = []byte("brt")
	BdbMaxKey     = []byte{0xff, 0xff, 0xff, 0xff}
)

const (
	PlainMemTableSize                     = 16 << 20
	PlainMemTableStopWritesThreshold      = 4 // base unit is the size of each shard
	SequentialMemTableSize                = 64 << 20
	SequentialMemTableStopWritesThreshold = 8

	SuperTableMaxSize   = 64 << 20
	SuperTableKeyLength = 8
)
