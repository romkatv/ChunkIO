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
    public void Seek(long position) => _file.Seek(position, SeekOrigin.Begin);
    public Task<int> ReadAsync(byte[] array, int offset, int count) => _file.ReadAsync(array, offset, count);
    public void Dispose() => _file.Dispose();
  }
}
