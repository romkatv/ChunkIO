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
  class InjectedReadException : Exception {
    public InjectedReadException(string op) : base($"Artificially injected read error: {op}") { }
  }

  sealed class ReadErrorInjector {
    readonly ConditionalWeakTable<FileStream, Calls> _calls = new ConditionalWeakTable<FileStream, Calls>();

    public void Length(FileStream file) {
      if (Fail(_calls.GetOrCreateValue(file).Length++)) throw new InjectedReadException("Length");
    }

    public void Seek(FileStream file, long position) {
      if (Fail(_calls.GetOrCreateValue(file).Seek++)) {
        file.Seek(file.Position + (position - file.Position) / 2, SeekOrigin.Begin);
        throw new InjectedReadException("Seek");
      }
    }

    public async Task ReadAsync(FileStream file, byte[] array, int offset, int count) {
      if (Fail(_calls.GetOrCreateValue(file).Read++)) {
        await file.ReadAsync(array, offset, count / 2);
        throw new InjectedReadException("Read");
      }
    }

    // Fail on calls 0, 1, 2, 4, 8, etc.
    bool Fail(long call) => (call & (call - 1)) == 0;

    class Calls {
      public long Length { get; set; }
      public long Seek { get; set; }
      public long Read { get; set; }
    }
  }

  sealed class ByteReader : IDisposable {
    readonly FileStream _file;

    public ByteReader(string fname) {
      // We need a file handle from _file to get the unique file ID. If we simply query _file.SafeFileHandle,
      // _file will remember this and will perform extra checks in most methods, which will significantly slow down
      // everything ByteReader will do. Hence we create another FileStream from the same file name and get
      // SafeFileHandle from *that* instance. However, if we aren't careful, the file might be deleted and
      // recreated in between our two FileStream constructor calls. If this happens, we'll acquire ID of a
      // different file (even though that file has the same name, it's a different file nonetheless). To avoid
      // this issue, we create the first FileStream without FileShare.Delete to disallow concurrent file deletions.
      // After creating the second FileStream, we close the first and once again allow concurrent deletions.
      using (var f = new FileStream(fname, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read | FileShare.Write)) {
        Id = FileId.Get(f.SafeFileHandle);
        _file = new FileStream(
          fname,
          FileMode.Open,
          FileAccess.Read,
          FileShare.ReadWrite | FileShare.Delete,
          bufferSize: 512,
          useAsync: true);
      }
    }

    // Returns unique file ID. Two file handles have the same ID if they are attached to the same kernel object.
    // That is, writes through one handle can be seen through the other.
    public IReadOnlyCollection<byte> Id { get; }

    public string Name => _file.Name;

    public long Length {
      get {
        ErrorInjector?.Length(_file);
        return _file.Length;
      }
    }

    public void Seek(long position) {
      ErrorInjector?.Seek(_file, position);
      if (position == _file.Position) return;
      if (_file.Seek(position, SeekOrigin.Begin) != position) {
        throw new IOException($"Cannot seek to {position}");
      }
    }

    public async Task<int> ReadAsync(byte[] array, int offset, int count) {
      if (ErrorInjector != null) await ErrorInjector.ReadAsync(_file, array, offset, count);
      return await _file.ReadAsync(array, offset, count);
    }

    public void Dispose() => _file.Dispose();

    public static ReadErrorInjector ErrorInjector { get; set; }
  }
}
