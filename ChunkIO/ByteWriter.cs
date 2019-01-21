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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  class InjectedWriteException : Exception {
    public InjectedWriteException(string op) : base($"Artificially injected write error: {op}") { }
  }

  sealed class WriteErrorInjector {
    readonly ConditionalWeakTable<FileStream, Calls> _calls = new ConditionalWeakTable<FileStream, Calls>();

    public void Position(FileStream file) {
      if (Fail(_calls.GetOrCreateValue(file).Position++)) throw new InjectedWriteException("Position");
    }

    public async Task WriteAsync(FileStream file, byte[] array, int offset, int count) {
      if (Fail(_calls.GetOrCreateValue(file).Write++)) {
        await file.WriteAsync(array, offset, count / 2);
        throw new InjectedWriteException("Write");
      }
    }

    public Task FlushAsync(FileStream file, bool flushToDisk) {
      if (flushToDisk) {
        if (Fail(_calls.GetOrCreateValue(file).FlushToDisk++)) throw new InjectedWriteException("FlushToDisk");
      } else {
        if (Fail(_calls.GetOrCreateValue(file).FlushToOS++)) throw new InjectedWriteException("FlushToOS");
      }
      return Task.CompletedTask;
    }

    // Fail on calls 0, 1, 2, 4, 8, etc.
    bool Fail(long call) => (call & (call - 1)) == 0;

    class Calls {
      public long Position { get; set; }
      public long Write { get; set; }
      public long FlushToOS { get; set; }
      public long FlushToDisk { get; set; }
    }
  }

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

    public long Position {
      get {
        ErrorInjector?.Position(_file);
        return _file.Position;
      }
    }

    public async Task WriteAsync(byte[] array, int offset, int count) {
      if (ErrorInjector != null) await ErrorInjector?.WriteAsync(_file, array, offset, count);
      await _file.WriteAsync(array, offset, count);
    }

    public async Task FlushAsync(bool flushToDisk) {
      if (ErrorInjector != null) await ErrorInjector.FlushAsync(_file, flushToDisk);
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
        await _file.FlushAsync();
      } else {
        _file.Flush(flushToDisk: false);
      }
    }

    public void Dispose() => _file.Dispose();

    public static WriteErrorInjector ErrorInjector { get; set; }
  }
}
