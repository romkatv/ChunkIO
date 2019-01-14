using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  interface IChunk {
    ulong BeginPosition { get; }
    ulong EndPosition { get; }
    ulong ContentLength { get; }
    UserData UserData { get; }

    Task<bool> ReadContentAsync(byte[] array, int offset);
  }

  class ChunkReader : IDisposable {
    readonly ByteReader _reader;

    public ChunkReader(string fname) {
      _reader = new ByteReader(fname);
    }

    public string Name => _reader.Name;
    public long Length => _reader.Length;

    public Task<IChunk> ReadBeforeAsync(long position) {
      throw new NotImplementedException();
    }

    public Task<IChunk> ReadAfterAsync(long position) {
      throw new NotImplementedException();
    }

    public void Dispose() => _reader.Dispose();

    class Chunk : IChunk {
      readonly ByteReader _reader;
      readonly ChunkHeader _header;

      public Chunk(ByteReader reader, ulong pos, ChunkHeader header) {
        _reader = reader;
        _header = header;
        BeginPosition = pos;
      }

      public ulong BeginPosition { get; }
      public ulong EndPosition => ChunkIO.Chunk.MeteredPosition(BeginPosition, ChunkHeader.Size + ContentLength).Value;
      public ulong ContentLength => _header.ContentLength;
      public UserData UserData => _header.UserData;

      public async Task<bool> ReadContentAsync(byte[] array, int offset) {
        if (array == null) throw new ArgumentNullException(nameof(array));
        if (offset < 0) throw new ArgumentException($"Negative offset: {offset}");
        if ((ulong)offset + ContentLength >= (ulong)array.Length) {
          throw new Exception($"Output array too short: {array.Length}");
        }
        if (EndPosition > (ulong)_reader.Length) return false;
        ulong read = 0;
        while (read != ContentLength) {
          ulong p = ChunkIO.Chunk.MeteredPosition(BeginPosition, BeginPosition + ChunkHeader.Size + read).Value;
          _reader.Seek((long)p);
          ulong n = Math.Min(ContentLength - read, ChunkIO.Chunk.MeterInterval - p % ChunkIO.Chunk.MeterInterval);
          if (await _reader.ReadAsync(array, offset + (int)read, (int)n) != (int)n) return false;
          read += n;
        }
        return SipHash.ComputeHash(array, offset, (int)read) == _header.ContentHash;
      }
    }
  }
}
