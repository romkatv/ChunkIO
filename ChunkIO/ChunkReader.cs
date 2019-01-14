using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using static ChunkIO.Chunk;

namespace ChunkIO {
  interface IChunk {
    long BeginPosition { get; }
    long EndPosition { get; }
    int ContentLength { get; }
    UserData UserData { get; }

    Task<bool> ReadContentAsync(byte[] array, int offset);
  }

  class ChunkReader : IDisposable {
    readonly ByteReader _reader;
    readonly byte[] _meter = new byte[Meter.Size];
    readonly byte[] _header = new byte[ChunkHeader.Size];

    public ChunkReader(string fname) {
      _reader = new ByteReader(fname);
    }

    public string Name => _reader.Name;
    public long Length => _reader.Length;

    public async Task<IChunk> ReadBeforeAsync(long position) {
      if (position < 0) return null;
      long fsize = _reader.Length;
      if (fsize < Meter.Size) return null;
      long m = (fsize - Meter.Size) / MeterInterval * MeterInterval;
      if (m - position >= MeterInterval) m = (position + MeterInterval - 1) % MeterInterval;
      while (true) {
        Meter? meter = await ReadMeter(m);
        if (!meter.HasValue) {
          if (m == 0) return null;
          m -= MeterInterval;
          continue;
        }
        ChunkHeader? header = await ReadChunkHeader(meter.Value.ChunkBeginPosition);
        // TODO: Implement me.
      }
    }

    public Task<IChunk> ReadAfterAsync(long position) {
      throw new NotImplementedException();
    }

    public void Dispose() => _reader.Dispose();

    async Task<Meter?> ReadMeter(long pos) {
      Debug.Assert(pos % MeterInterval == 0);
      _reader.Seek(pos);
      if (await _reader.ReadAsync(_meter, 0, _meter.Length) != _meter.Length) return null;
      var res = new Meter();
      if (!res.ReadFrom(_meter)) return null;
      return res;
    }

    async Task<ChunkHeader?> ReadChunkHeader(long pos) {
      if (!await ReadMetered(pos, _header, 0, _header.Length)) return null;
      var res = new ChunkHeader();
      if (!res.ReadFrom(_header)) return null;
      return res;
    }

    async Task<bool> ReadMetered(long pos, byte[] array, int offset, int count) {
      Debug.Assert(MeteredPosition(pos, count).HasValue);
      if (MeteredPosition(pos, count) > _reader.Length) return false;
      pos = MeteredPosition(pos, 0).Value;
      while (count > 0) {
        _reader.Seek(pos);
        int n = Math.Min(count, (int)(MeterInterval - pos % MeterInterval));
        if (await _reader.ReadAsync(array, offset, n) != n) return false;
        offset += n;
        count -= n;
        pos = MeteredPosition(pos, n).Value;
      }
      return true;
    }

    class Chunk : IChunk {
      readonly ChunkReader _reader;
      readonly ChunkHeader _header;

      public Chunk(ChunkReader reader, long pos, ChunkHeader header) {
        _reader = reader;
        _header = header;
        BeginPosition = pos;
      }

      public long BeginPosition { get; }
      public long EndPosition => MeteredPosition(BeginPosition, ChunkHeader.Size + ContentLength).Value;
      public int ContentLength => _header.ContentLength;
      public UserData UserData => _header.UserData;

      public async Task<bool> ReadContentAsync(byte[] array, int offset) {
        if (array == null) throw new ArgumentNullException(nameof(array));
        if (offset < 0) throw new ArgumentException($"Negative offset: {offset}");
        if (array.Length - ContentLength <= offset)  throw new ArgumentException($"Array too short: {array.Length}");
        long pos = MeteredPosition(BeginPosition, ChunkHeader.Size).Value;
        return await _reader.ReadMetered(pos, array, offset, ContentLength) &&
               SipHash.ComputeHash(array, offset, ContentLength) == _header.ContentHash;
      }
    }
  }
}
