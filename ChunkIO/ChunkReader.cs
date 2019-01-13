using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  interface IChunk {
    ulong BeginPosition { get; }
    ulong EndPosition { get; }
    ulong ContentLength { get; }
    UserData UserData { get; }

    Task<bool> ReadContent(byte[] array, int offset);
  }

  class ChunkReader : IDisposable {
    public ChunkReader(string fname) {
      throw new NotImplementedException();
    }

    public string Name => throw new NotImplementedException();
    public long Length => throw new NotImplementedException();

    public Task<IChunk> ReadBefore(long position) {
      throw new NotImplementedException();
    }

    public Task<IChunk> ReadAfter(long position) {
      throw new NotImplementedException();
    }

    public void Dispose() {
      throw new NotImplementedException();
    }
  }
}
