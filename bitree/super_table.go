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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/zuoyebang/bitaloslog/internal/base"
	"github.com/zuoyebang/bitaloslog/internal/bytepools"
	"github.com/zuoyebang/bitaloslog/internal/consts"
	"github.com/zuoyebang/bitaloslog/internal/utils"
	"github.com/zuoyebang/bitaloslog/internal/vfs"
)

const (
	keyFileVersion = 1

	keyFileHeaderSize   = 8
	keyFileHeaderKeyNum = 4

	valueOffsetSize = 4
)

type superTable struct {
	opts        *stOptions
	keyTbl      *stable
	valueTbl    *stable
	fn          FileNum
	version     uint32
	keyReadable atomic.Uint32
	keyFlushed  int
	keyLength   int
	keyStep     int
}

type stOptions struct {
	fs        vfs.FS
	path      string
	fn        FileNum
	logger    Logger
	readOnly  bool
	keyLength int
}

func newSuperTable(opts *stOptions) (st *superTable, err error) {
	st = &superTable{
		opts:      opts,
		fn:        opts.fn,
		version:   keyFileVersion,
		keyLength: opts.keyLength,
		keyStep:   opts.keyLength + valueOffsetSize,
	}

	keyPath := base.MakeFilepath(opts.fs, opts.path, base.FileTypeSuperTableIndex, base.FileNum(opts.fn))
	st.keyTbl, err = newStable(keyPath)
	if err != nil {
		return nil, err
	}
	if err = st.writeKeyFileHeader(); err != nil {
		return nil, err
	}

	valuePath := base.MakeFilepath(opts.fs, opts.path, base.FileTypeSuperTable, base.FileNum(opts.fn))
	st.valueTbl, err = newStable(valuePath)
	if err != nil {
		return nil, err
	}

	st.opts.logger.Infof("new superTable(%d) success", st.fn)
	return st, nil
}

func openSuperTable(opts *stOptions) (st *superTable, err error) {
	st = &superTable{
		opts:      opts,
		fn:        opts.fn,
		keyLength: opts.keyLength,
		keyStep:   opts.keyLength + 4,
	}

	keyPath := base.MakeFilepath(opts.fs, opts.path, base.FileTypeSuperTableIndex, base.FileNum(opts.fn))
	st.keyTbl, err = openStable(keyPath, opts.readOnly)
	if err != nil {
		return nil, err
	}

	valuePath := base.MakeFilepath(opts.fs, opts.path, base.FileTypeSuperTable, base.FileNum(opts.fn))
	st.valueTbl, err = openStable(valuePath, opts.readOnly)
	if err != nil {
		return nil, err
	}

	if err = st.readKeyFileHeader(); err != nil {
		return nil, err
	}

	st.opts.logger.Infof("open superTable(%d) success readOnly:%v", st.fn, opts.readOnly)
	return st, nil
}

func (s *superTable) writeKeyFileHeader() error {
	var header [keyFileHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], s.version)
	binary.BigEndian.PutUint32(header[4:8], 0)
	n, err := s.keyTbl.write(header[:])
	if err != nil {
		return err
	} else if n != 8 {
		return io.ErrShortWrite
	}
	s.keyReadable.Store(0)
	if err = s.keyTbl.fdatasync(); err != nil {
		return err
	}

	s.keyTbl.offset.Add(keyFileHeaderSize)
	return nil
}

func (s *superTable) readKeyFileHeader() error {
	var header [keyFileHeaderSize]byte
	n, err := s.keyTbl.file.ReadAt(header[:], 0)
	if err != nil {
		return err
	} else if n != keyFileHeaderSize {
		return io.ErrShortWrite
	}

	s.version = binary.BigEndian.Uint32(header[0:4])
	expKeyNum := binary.BigEndian.Uint32(header[4:8])
	expKeyFilesz := keyFileHeaderSize + int(expKeyNum)*s.keyStep
	keyFilesz := int(s.keyTbl.filesz)
	valueFilesz := int(s.valueTbl.filesz)
	if expKeyFilesz == keyFilesz {
		s.keyReadable.Store(expKeyNum)
		return nil
	}

	var keyPos, voff int
	var actKeyNum uint32
	var szBuf [4]byte
	var valBuf []byte

	logTag := fmt.Sprintf("open superTable(%d) rebuild", s.fn)
	s.opts.logger.Infof("%s start filesz not eq expKeyNum:%d expFilesz:%d actFilesz:%d",
		logTag, expKeyNum, expKeyFilesz, keyFilesz)
	keyPos = keyFileHeaderSize
	for keyPos+s.keyStep <= keyFilesz {
		koff := keyPos + s.keyLength
		n, err = s.keyTbl.file.ReadAt(szBuf[:], int64(koff))
		if err != nil {
			s.opts.logger.Infof("%s read keyFile fail keyOff:%d err:%v", logTag, koff, err)
			break
		} else if n != 4 {
			s.opts.logger.Infof("%s read keyFile short keyOff:%d n:%d", logTag, koff, n)
			break
		}
		voff = int(binary.BigEndian.Uint32(szBuf[:]))
		if voff+4 >= valueFilesz {
			s.opts.logger.Infof("%s value offset invalid keyPos:%d voff:%d valueFilesz:%d", logTag, keyPos, voff, valueFilesz)
			break
		}

		n, err = s.valueTbl.file.ReadAt(szBuf[:], int64(voff))
		if err != nil {
			s.opts.logger.Infof("%s read value size fail keyPos:%d voff:%d err:%v", logTag, keyPos, voff, err)
			break
		} else if n != 4 {
			s.opts.logger.Infof("%s read value size short keyPos:%d voff:%d n:%d", logTag, keyPos, voff, n)
			break
		}

		valueSize := binary.BigEndian.Uint32(szBuf[:])
		if valueSize == 0 {
			continue
		}

		if cap(valBuf) < int(valueSize) {
			valBuf = nil
			valBuf = make([]byte, valueSize)
		}
		n, err = s.valueTbl.file.ReadAt(valBuf[0:valueSize], int64(voff+4))
		if err != nil {
			s.opts.logger.Infof("%s read value fail keyPos:%d voff:%d vsize:%d err:%v", logTag, keyPos, voff+4, valueSize, err)
			break
		} else if n != int(valueSize) {
			s.opts.logger.Infof("%s read value short keyPos:%d voff:%d vsize:%d n:%d", logTag, keyPos, voff+4, valueSize, n)
			break
		}

		actKeyNum++
		keyPos += s.keyStep
	}

	s.keyReadable.Store(actKeyNum)
	if actKeyNum != expKeyNum {
		if err = s.writeKeyNum(actKeyNum); err != nil {
			s.opts.logger.Errorf("%s writeKeyNum fail err:%v", logTag, err)
		}
	}

	s.opts.logger.Infof("%s finish actKeyNum:%d", logTag, actKeyNum)
	return nil
}

func (s *superTable) writeKeyNum(num uint32) error {
	offset := s.keyTbl.offset.Load()

	var buf [4]byte
	binary.BigEndian.PutUint32(buf[0:4], num)
	n, err := s.keyTbl.file.WriteAt(buf[:], keyFileHeaderKeyNum)
	if err != nil {
		return err
	} else if n != 4 {
		return io.ErrShortWrite
	}

	if _, err = s.keyTbl.file.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	if err = s.keyTbl.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *superTable) readKeyNum() uint32 {
	var buf [4]byte
	s.keyTbl.file.ReadAt(buf[:], keyFileHeaderKeyNum)
	keyNum := binary.BigEndian.Uint32(buf[0:4])
	return keyNum
}

func (s *superTable) getKeyPosOffset(pos int) int {
	return keyFileHeaderSize + pos*s.keyStep
}

func (s *superTable) readKeyByPos(pos int) []byte {
	p := s.getKeyPosOffset(pos)
	key := make([]byte, s.keyLength)
	n, err := s.keyTbl.file.ReadAt(key, int64(p))
	if err != nil || n != s.keyLength {
		return nil
	}
	return key
}

func (s *superTable) readValueOffsetByPos(pos int) int {
	p := s.getKeyPosOffset(pos) + s.keyLength
	var buf [valueOffsetSize]byte
	n, err := s.keyTbl.file.ReadAt(buf[:], int64(p))
	if err != nil || n != valueOffsetSize {
		return -1
	}
	return int(binary.BigEndian.Uint32(buf[:]))
}

func (s *superTable) readKVByPos(pos int) ([]byte, uint32) {
	p := s.getKeyPosOffset(pos)
	buf := make([]byte, s.keyStep)
	n, err := s.keyTbl.file.ReadAt(buf, int64(p))
	if err != nil || n != s.keyStep {
		return nil, 0
	}
	key := buf[:s.keyLength]
	voff := binary.BigEndian.Uint32(key[s.keyLength:s.keyStep])
	return key, voff
}

func (s *superTable) get(key []byte) ([]byte, func()) {
	pos, found := sort.Find(int(s.keyReadable.Load()), func(i int) int {
		cmp := bytes.Compare(s.readKeyByPos(i), key)
		return -cmp
	})
	if !found {
		return nil, nil
	}
	valueOffset := s.readValueOffsetByPos(pos)
	if valueOffset < 0 {
		return nil, nil
	}
	return s.getValue(uint32(valueOffset))
}

func (s *superTable) getMaxKey() []byte {
	buf := make([]byte, s.keyLength)
	pos := int(s.keyReadable.Load() - 1)
	off := s.getKeyPosOffset(pos)
	n, err := s.keyTbl.file.ReadAt(buf, int64(off))
	if err != nil || n != s.keyLength {
		return nil
	}

	return buf
}

func (s *superTable) getValue(offset uint32) ([]byte, func()) {
	var szBuf [4]byte
	n, err := s.valueTbl.file.ReadAt(szBuf[:], int64(offset))
	if err != nil || n != 4 {
		return nil, nil
	}

	valueSize := binary.BigEndian.Uint32(szBuf[:])
	if valueSize == 0 {
		return nil, nil
	}

	buf, closer := bytepools.ReaderBytePools.GetBytePool(int(valueSize))
	buf = buf[:valueSize]
	n, err = s.valueTbl.file.ReadAt(buf, int64(offset+4))
	if err != nil || n != int(valueSize) {
		closer()
		return nil, nil
	}

	return buf, closer
}

func (s *superTable) set(key, value []byte) error {
	valueSize := uint32(len(value))
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[0:4], valueSize)
	n, err := s.valueTbl.writer.Write(buf[0:4])
	if err != nil {
		return err
	} else if n != 4 {
		return io.ErrShortWrite
	}
	n, err = s.valueTbl.writer.Write(value)
	if err != nil {
		return err
	} else if n != len(value) {
		return io.ErrShortWrite
	}
	valueOffset := s.valueTbl.offset.Load()
	s.valueTbl.offset.Add(valueSize + 4)

	n, err = s.keyTbl.writer.Write(key)
	if err != nil {
		return err
	} else if n != len(key) {
		return io.ErrShortWrite
	}
	binary.BigEndian.PutUint32(buf[0:4], valueOffset)
	n, err = s.keyTbl.writer.Write(buf[0:4])
	if err != nil {
		return err
	} else if n != 4 {
		return io.ErrShortWrite
	}
	s.keyTbl.offset.Add(uint32(len(key) + 4))

	s.keyFlushed++
	return nil
}

func (s *superTable) inuseBytes() uint64 {
	return uint64(s.keyTbl.inuseBytes()) + uint64(s.valueTbl.inuseBytes())
}

func (s *superTable) flushFinish() error {
	if err := s.keyTbl.fdatasync(); err != nil {
		return err
	}
	if err := s.valueTbl.fdatasync(); err != nil {
		return err
	}
	keyNum := s.keyReadable.Load() + uint32(s.keyFlushed)
	if err := s.writeKeyNum(keyNum); err != nil {
		return err
	}

	s.keyReadable.Store(keyNum)
	s.keyFlushed = 0
	return nil
}

func (s *superTable) close() (err error) {
	err = utils.FirstError(err, s.keyTbl.close())
	err = utils.FirstError(err, s.valueTbl.close())
	return err
}

type stable struct {
	file      *os.File
	path      string
	filesz    int64
	writer    io.Writer
	wbuf      []byte
	bufWriter *bufio.Writer
	offset    atomic.Uint32
	readOnly  bool
}

func newStable(path string) (*stable, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, consts.FileMode)
	if err != nil {
		return nil, err
	}

	t := &stable{
		file: file,
		path: path,
	}

	t.bufWriter = bufio.NewWriterSize(t.file, consts.BufioWriterBufSize)
	t.writer = t.bufWriter
	return t, nil
}

func openStable(path string, readOnly bool) (*stable, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, consts.FileMode)
	if err != nil {
		return nil, err
	}

	t := &stable{
		file:     file,
		path:     path,
		readOnly: readOnly,
	}

	fileStat, err := t.file.Stat()
	if err != nil {
		return nil, err
	}

	t.filesz = fileStat.Size()
	t.offset.Store(uint32(t.filesz))

	if readOnly {
		return t, nil
	}

	if _, err = t.file.Seek(t.filesz, io.SeekStart); err != nil {
		return nil, err
	}

	t.bufWriter = bufio.NewWriterSize(t.file, consts.BufioWriterBufSize)
	t.writer = t.bufWriter

	return t, nil
}

func (t *stable) write(p []byte) (int, error) {
	return t.writer.Write(p)
}

func (t *stable) inuseBytes() uint32 {
	return t.offset.Load()
}

func (t *stable) fdatasync() error {
	if t.readOnly {
		return nil
	}

	if err := t.bufWriter.Flush(); err != nil {
		return err
	}

	return t.file.Sync()
}

func (t *stable) close() error {
	if err := t.fdatasync(); err != nil {
		return err
	}
	return t.file.Close()
}
