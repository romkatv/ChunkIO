using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  class ByteWriter : IDisposable {
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
    public Task FlushAsync(bool flushToDisk) => _file.FlushAsync();

    public void Dispose() => _file.Dispose();
  }
}
