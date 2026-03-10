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

package base

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

type FileNum uint64

func (fn FileNum) String() string { return fmt.Sprintf("%d", fn) }

func EncodeFileNum(sn FileNum) []byte {
	return utils.Uint32ToBytes(uint32(sn))
}

func DecodeFileNum(b []byte) FileNum {
	return FileNum(utils.BytesToUint32(b))
}

type FileType int

const (
	FileTypeLog FileType = iota
	FileTypeLock
	FileTypeMeta
	FileTypeBdb
	FileTypeArrayTable
	FileTypeSuperTable
	FileTypeSuperTableIndex
)

func MakeFilename(fileType FileType, fileNum FileNum) string {
	switch fileType {
	case FileTypeLog:
		return fmt.Sprintf("%s.log", fileNum)
	case FileTypeArrayTable:
		return fmt.Sprintf("%s.at", fileNum)
	case FileTypeSuperTable:
		return fmt.Sprintf("%s.xt", fileNum)
	case FileTypeSuperTableIndex:
		return fmt.Sprintf("%s.xti", fileNum)
	case FileTypeBdb:
		return "bitree.bdb"
	case FileTypeLock:
		return "LOCK"
	case FileTypeMeta:
		return "MANIFEST"
	}
	panic("unreachable")
}

func MakeFilepath(fs vfs.FS, dirname string, fileType FileType, fileNum FileNum) string {
	return fs.PathJoin(dirname, MakeFilename(fileType, fileNum))
}

func ParseFilename(fs vfs.FS, filename string) (fileType FileType, fileNum FileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "LOCK":
		return FileTypeLock, 0, true
	case filename == "MANIFEST":
		return FileTypeMeta, 0, true
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		fileNum, ok = parseFileNum(filename[:i])
		if !ok {
			break
		}
		switch filename[i+1:] {
		case "log":
			return FileTypeLog, fileNum, true
		case "at":
			return FileTypeArrayTable, fileNum, true
		case "xt":
			return FileTypeSuperTable, fileNum, true
		case "xti":
			return FileTypeSuperTableIndex, fileNum, true
		}
	}
	return 0, fileNum, false
}

func parseFileNum(s string) (fileNum FileNum, ok bool) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fileNum, false
	}
	return FileNum(u), true
}

func GetFilePathBase(path string) string {
	if path == "" {
		return "."
	}

	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[0 : len(path)-1]
	}

	pos := strings.LastIndex(path, "/")
	if pos >= 0 {
		pos = strings.LastIndex(path[:pos], "/")
		if pos >= 0 {
			path = path[pos+1:]
		}
	}

	if path == "" {
		return "/"
	}
	return path
}
