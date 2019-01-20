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
  sealed class ByteWriter : IDisposable {
    readonly FileStream _file;

    public ByteWriter(string fname) {
      _file = new FileStream(
          fname,
          FileMode.Append,
          FileAccess.Write,
          FileShare.Read | FileShare.Delete,
          bufferSize: 4 << 10,
          useAsync: true);
    }

    public string Name => _file.Name;

    public long Position => _file.Position;
    public Task WriteAsync(byte[] array, int offset, int count) => _file.WriteAsync(array, offset, count);
    public Task FlushAsync(bool flushToDisk) {
      // The flush API in FileStream is fucked up:
      //
      //   * FileStream.Flush(flushToDisk) synchronously flushes to OS and then optionally synchronously
      //     flushes to disk.
      //   * FileStream.FlushAsync() synchronously flushes to OS and then asynchronously flushes to disk.
      //
      // To add insult to injury, when called without arguments, FileStream.Flush() doesn't flush to disk
      // while FileStream.FlushAsync() does!
      //
      // Based on this API we implement ByteWriter.FlushAsync(flushToDisk) that synchronously flushes to OS
      // and then optionally asynchronously flushes to disk. Not perfect but the best we can do.
      if (flushToDisk) {
        return _file.FlushAsync();
      } else {
        _file.Flush(flushToDisk: false);
        return Task.CompletedTask;
      }
    }

    public void Dispose() => _file.Dispose();
  }
}
