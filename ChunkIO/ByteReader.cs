// Copyright 2019 Roman Perepelitsa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  sealed class ByteReader : IDisposable {
    readonly FileStream _file;

    public ByteReader(string fname) {
      _file = new FileStream(
          fname,
          FileMode.Open,
          FileAccess.Read,
          FileShare.ReadWrite | FileShare.Delete,
          bufferSize: 512,
          useAsync: true);
    }

    public string Name => _file.Name;
    public long Length => _file.Length;
    public void Seek(long position) {
      if (_file.Seek(position, SeekOrigin.Begin) != position) {
        throw new IOException($"Cannot seek to {position}");
      }
    }
    public Task<int> ReadAsync(byte[] array, int offset, int count) => _file.ReadAsync(array, offset, count);
    public void Dispose() => _file.Dispose();
  }
}
