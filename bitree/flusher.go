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

type Flusher struct {
	b           *Bitree
	st          *superTable
	lastKey     []byte
	compressBuf []byte
}

func (t *Bitree) NewFlusher() *Flusher {
	w := &Flusher{
		b:  t,
		st: t.stMutable,
	}
	return w
}

func (f *Flusher) Set(key, value []byte) error {
	//compressed := f.b.opts.Compressor.Encode(f.compressBuf, value)
	//if cap(compressed) > cap(f.compressBuf) {
	//	f.compressBuf = compressed[:cap(compressed)]
	//}

	if err := f.st.set(key, value); err != nil {
		return err
	}

	f.lastKey = key
	return nil
}

func (f *Flusher) Finish() error {
	if err := f.st.flushFinish(); err != nil {
		return err
	}

	if f.st.inuseBytes() < f.b.maxStSize {
		return nil
	}

	if err := f.b.makeRoomForWrite(f.lastKey); err != nil {
		return err
	}

	f.compressBuf = nil
	f.lastKey = nil
	f.st = nil
	return nil
}
